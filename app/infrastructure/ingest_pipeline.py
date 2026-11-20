"""
Esteira de ingestao: recebe do MQTT, avisa o painel e grava no banco, cada
etapa na sua thread.

    paho (MQTT) --fila de chegada--> preparar --+--> painel (na hora)
                                                 \\
                                                  +--fila de gravacao--> gravar no banco

Por que existe (incidente de 12/11/2026): o callback do paho gravava no banco
dentro da propria thread do MQTT. Quando o RDS ficou lento (consultas pesadas do
painel num db.t4g.micro), cada gravacao levava ate ~40 s entre timeouts e
retentativas. Enquanto isso:
  - nenhuma maquina era atualizada, e com o limite de 4 s o painel mostrava
    TODAS offline ao mesmo tempo;
  - o paho nao respondia ao broker, que derrubou o Listener por "Keep alive
    timeout".

Agora a thread do paho so enfileira (nunca espera o banco), o painel recebe a
leitura assim que a maquina e identificada, e o banco grava no ritmo dele. Banco
lento atrasa a GRAVACAO, nao o painel nem a conexao com o broker.

Garantias:
  - Ordem preservada: uma thread por etapa, filas FIFO.
  - A hora da leitura (received_at) e a da chegada, carimbada no submit.
  - Filas limitadas: se a gravacao ficar para tras alem do limite, a leitura
    mais antiga vai para o arquivo .jsonl de reserva (nada some em silencio).
"""

import queue
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable

from app.application.ingest_telemetry import IngestStatus, IngestTelemetry, PersistOutcome
from app.application.ports import TelemetryFallbackWriter
from app.domain.entities import TelemetryEvent

# Ritmo real (12/11/2026): ~1,7 mensagem/s somando as maquinas. 20 mil leituras
# na fila de gravacao seguram ~3 h de banco fora do ar com folga de memoria.
MAX_CHEGADA = 5_000
MAX_GRAVACAO = 20_000
# Avisa no log quando a gravacao acumula, no maximo uma vez a cada intervalo.
AVISO_FILA = 200
AVISO_INTERVALO_S = 30.0

_FIM = object()  # sinal de encerramento que atravessa as duas filas
# De quanto em quanto a thread de gravacao, mesmo sem leitura nova, atualiza o
# "mantido ate" dos estados parados (maquina que parou de mandar no meio de um).
MANUTENCAO_S = 1.0


@dataclass(frozen=True)
class MensagemMqtt:
    topic: str
    payload: bytes
    qos: int
    retain: bool
    received_at: datetime


class IngestPipeline:
    def __init__(
        self,
        ingest: IngestTelemetry,
        notifier: Any | None = None,
        overflow: TelemetryFallbackWriter | None = None,
        max_chegada: int = MAX_CHEGADA,
        max_gravacao: int = MAX_GRAVACAO,
        log: Callable[[str], None] = print,
        clock: Callable[[], float] = time.monotonic,
    ):
        self._ingest = ingest
        self._notifier = notifier
        self._overflow = overflow
        self._log = log
        self._clock = clock
        self._chegada: "queue.Queue[Any]" = queue.Queue(maxsize=max_chegada)
        self._gravacao: "queue.Queue[Any]" = queue.Queue(maxsize=max_gravacao)
        self._threads = [
            threading.Thread(target=self._preparar, name="ingest-preparar", daemon=True),
            threading.Thread(target=self._gravar, name="ingest-gravar", daemon=True),
        ]
        self._lock = threading.Lock()
        self._descartadas = 0
        self._ultimo_aviso = float("-inf")
        self._atrasada = False

    # --- entrada (thread do paho) ---------------------------------------------

    def start(self) -> None:
        for t in self._threads:
            t.start()

    def submit(
        self,
        topic: str,
        payload: bytes,
        qos: int,
        retain: bool,
        received_at: datetime | None = None,
    ) -> None:
        """Enfileira a mensagem e volta na hora. Chamado pelo callback do paho."""
        msg = MensagemMqtt(
            topic=topic,
            payload=payload,
            qos=qos,
            retain=retain,
            received_at=received_at or datetime.now(timezone.utc),
        )
        self._enfileirar(self._chegada, msg)

    @property
    def pendentes(self) -> int:
        """Leituras recebidas e ainda nao gravadas (as duas filas)."""
        return self._chegada.qsize() + self._gravacao.qsize()

    @property
    def descartadas(self) -> int:
        return self._descartadas

    def stop(self, timeout: float = 20.0) -> bool:
        """Para de aceitar e grava o que ja esta na fila, ate `timeout` segundos.
        Devolve True se tudo foi gravado a tempo."""
        prazo = self._clock() + timeout
        try:
            self._chegada.put(_FIM, timeout=max(0.0, prazo - self._clock()))
        except queue.Full:
            return False
        for t in self._threads:
            if t.is_alive():
                t.join(max(0.0, prazo - self._clock()))
        return not any(t.is_alive() for t in self._threads)

    # --- etapas -----------------------------------------------------------------

    def _preparar(self) -> None:
        while True:
            msg = self._chegada.get()
            if msg is _FIM:
                self._gravacao.put(_FIM)
                return
            try:
                result = self._ingest.prepare(
                    msg.topic, msg.payload, msg.qos, msg.retain, msg.received_at
                )
            except Exception as exc:  # nao pode matar a thread: as demais leituras seguem
                self._log(f"[ERRO] Falha ao preparar {msg.topic}: {exc}")
                continue

            if result.status is IngestStatus.INVALID_TOPIC:
                self._log(f"[SKIP] Topico sem nivel de tipo de dado: {msg.topic}")
                continue
            if result.status is not IngestStatus.PREPARED or result.event is None:
                self._log(
                    f"[ERRO] Nao foi possivel resolver/registrar a maquina "
                    f"'{result.machine_code}' (repositorio indisponivel?) — topico {msg.topic}"
                )
                continue

            # Painel primeiro: a maquina aparece viva mesmo com o banco lento.
            if self._notifier is not None:
                try:
                    self._notifier.notify(result.event)
                except Exception as exc:
                    self._log(f"[PUSH] Falha ao avisar o painel (ignorado): {exc}")
            self._enfileirar(self._gravacao, (result.machine_code, result.event))

    def _gravar(self) -> None:
        proxima_manutencao = self._clock() + MANUTENCAO_S
        while True:
            try:
                item = self._gravacao.get(timeout=MANUTENCAO_S)
            except queue.Empty:
                item = None
            if item is _FIM:
                # o estado aberto de cada topico vai inteiro para o banco
                self._manter(final=True)
                return
            if item is not None:
                code, event = item
                try:
                    status = self._persistir(event)
                except Exception as exc:
                    self._log(f"[REST] Falha inesperada ao gravar {code}/{event.category}: {exc}")
                    status = PersistOutcome.FAILED.value
                dono = event.tenant_id or "SEM-CLIENTE"
                self._log(f"[MSG] {code}/{event.category} (tenant={dono}) {status}")
                self._avisar_atraso()
            if self._clock() >= proxima_manutencao:
                proxima_manutencao = self._clock() + MANUTENCAO_S
                self._manter(final=False)

    def _persistir(self, event: TelemetryEvent) -> str:
        """gravada / mantida / FALHOU. Ingestao sem estado mantido (ou de teste)
        so tem `persist`."""
        outcome = getattr(self._ingest, "persist_outcome", None)
        if outcome is not None:
            return outcome(event).value
        return "gravada" if self._ingest.persist(event) else PersistOutcome.FAILED.value

    def _manter(self, final: bool) -> None:
        flush = getattr(self._ingest, "flush_held", None)
        if flush is None:
            return
        try:
            flush(final=final)
        except Exception as exc:
            self._log(f"[ESTADO] Falha ao atualizar os estados mantidos (ignorado): {exc}")

    # --- apoio -------------------------------------------------------------------

    def _enfileirar(self, fila: "queue.Queue[Any]", item: Any) -> None:
        """Poe na fila sem bloquear. Cheia: tira a mais antiga para abrir espaco."""
        while True:
            try:
                fila.put_nowait(item)
                return
            except queue.Full:
                try:
                    antiga = fila.get_nowait()
                except queue.Empty:
                    continue
                if antiga is _FIM:  # encerramento tem prioridade: devolve e desiste
                    fila.put_nowait(antiga)
                    self._descartar(item)
                    return
                self._descartar(antiga)

    def _descartar(self, item: Any) -> None:
        with self._lock:
            self._descartadas += 1
            total = self._descartadas
        event = item[1] if isinstance(item, tuple) else None
        if isinstance(event, TelemetryEvent) and self._overflow is not None:
            try:
                self._overflow.write(event)
                destino = "salva no .jsonl de reserva"
            except Exception as exc:
                destino = f"PERDIDA (reserva falhou: {exc})"
        else:
            destino = "PERDIDA (fila de chegada cheia)"
        if total == 1 or total % 100 == 0:
            self._log(f"[FILA] Fila cheia: leitura {destino}. Total descartado: {total}")

    def _avisar_atraso(self) -> None:
        pendentes = self._gravacao.qsize()
        agora = self._clock()
        if pendentes >= AVISO_FILA:
            self._atrasada = True
            if agora - self._ultimo_aviso >= AVISO_INTERVALO_S:
                self._ultimo_aviso = agora
                self._log(f"[FILA] {pendentes} leituras aguardando gravacao (banco lento?)")
        elif self._atrasada and pendentes == 0:
            self._atrasada = False
            self._log("[FILA] Gravacao em dia")
