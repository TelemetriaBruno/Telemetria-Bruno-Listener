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

Log: nenhuma linha por mensagem (era o que enchia o disco em 24/11/2026). A
esteira conta o que passou e escreve um resumo por maquina a cada minuto, avisa
quando uma maquina fica em silencio e limita a repeticao dos erros (ver
ingest_resumo.py e logfmt.py).
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
from app.domain.topic import parse_topic
from app.infrastructure import logfmt
from app.infrastructure.ingest_resumo import JANELA_S, SILENCIO_S, ResumoIngestao

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
        log: Callable[..., None] | None = None,
        clock: Callable[[], float] = time.monotonic,
        janela_s: float = JANELA_S,
        silencio_s: float = SILENCIO_S,
    ):
        self._ingest = ingest
        self._notifier = notifier
        self._overflow = overflow
        # log(nivel, evt, **campos), ver logfmt.py. Os erros que podem se repetir
        # a cada mensagem passam pelo limitado (uma linha por minuto por tipo).
        self._log = log or logfmt.log
        self._log_limitado = logfmt.LogLimitado(self._log, clock=clock)
        self._clock = clock
        self._resumo = ResumoIngestao(self._log, clock, janela_s=janela_s, silencio_s=silencio_s)
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
        # A deteccao de silencio olha a CHEGADA do broker, aqui: mais adiante a
        # thread de preparar pode ficar presa no cadastro (banco fora), e a
        # maquina que esta publicando pareceria calada.
        parsed = parse_topic(topic)
        if parsed is not None:
            self._resumo.chegou(parsed.machine_code, msg.received_at)
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
                self._resumo.falha_preparar(msg.topic, exc)
                continue

            if result.status is IngestStatus.INVALID_TOPIC:
                # topico sem o nivel do tipo de dado (ou sem o da maquina)
                self._resumo.topico_invalido(msg.topic)
                continue
            if result.status is not IngestStatus.PREPARED or result.event is None:
                # cadastro de maquinas indisponivel: a leitura se perde
                self._resumo.maquina_nao_resolvida(result.machine_code, msg.topic)
                continue

            # Painel primeiro: a maquina aparece viva mesmo com o banco lento.
            if self._notifier is not None:
                try:
                    self._notifier.notify(result.event)
                except Exception as exc:
                    self._log_limitado("WARN", "painel.aviso_falhou", maquina=result.machine_code, erro=exc)
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
                self._resumo.fechar()
                return
            if item is not None:
                code, event = item
                try:
                    status = self._persistir(event)
                except Exception as exc:
                    # qualquer parte da gravacao: banco, copia .jsonl (disco cheio), estado
                    self._log_limitado(
                        "ERRO", "gravacao.falha_inesperada", maquina=code, categoria=event.category, erro=exc
                    )
                    status = PersistOutcome.FAILED.value
                # sem linha por mensagem: entra no resumo da janela
                self._resumo.gravou(code, event.category, status)
                self._avisar_atraso()
            if self._clock() >= proxima_manutencao:
                proxima_manutencao = self._clock() + MANUTENCAO_S
                self._manter(final=False)
                self._resumo.tick()

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
            # roda a cada segundo: limitado, senao uma falha fixa seria 60 linhas/min
            self._log_limitado("WARN", "estado.falha_manutencao", final=final, erro=exc)

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
        fila = "gravacao" if isinstance(item, tuple) else "chegada"
        erro = None
        if isinstance(event, TelemetryEvent) and self._overflow is not None:
            try:
                self._overflow.write(event)
                destino = "reserva_jsonl"
            except Exception as exc:
                destino, erro = "perdida", exc
        else:
            destino = "perdida"
        if total == 1 or total % 100 == 0:
            # salva na reserva = degradado (WARN); perdida = ERRO
            nivel = "WARN" if destino == "reserva_jsonl" else "ERRO"
            self._log(nivel, "fila.cheia", fila=fila, destino=destino, total_descartadas=total, erro=erro)

    def _avisar_atraso(self) -> None:
        pendentes = self._gravacao.qsize()
        agora = self._clock()
        if pendentes >= AVISO_FILA:
            self._atrasada = True
            if agora - self._ultimo_aviso >= AVISO_INTERVALO_S:
                self._ultimo_aviso = agora
                # leituras aguardando gravacao: banco lento?
                self._log("WARN", "fila.atrasada", pendentes=pendentes)
        elif self._atrasada and pendentes == 0:
            self._atrasada = False
            self._log("INFO", "fila.em_dia")
