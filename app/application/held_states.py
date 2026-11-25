"""
Os estados abertos de cada topico de cada maquina (ver domain/hold.py).

A leitura igual a anterior e ABSORVIDA pelo estado aberto: nao vira linha nova,
so estende "mantido ate" (held_until) e soma uma repeticao. A linha no banco e
atualizada com isso de tempos em tempos (`flush_after`), e uma ultima vez quando
o valor muda, a hora vira ou o Listener para. Enquanto a linha nao foi atualizada
o banco so fica atrasado alguns segundos sobre ate quando o valor durou; o valor
em si ja esta la desde a primeira leitura.

Roda na thread de gravacao da esteira (uma so), mas tem trava propria porque o
encerramento e o `execute` direto tambem o chamam.
"""

import threading
from datetime import datetime, timedelta, timezone
from typing import Callable, Protocol

from app.application.ports import EventLog, sem_log
from app.domain.entities import TelemetryEvent
from app.domain.hold import HeldState, holds_state, hour_of, state_signature


class HoldRepository(Protocol):
    def extend_hold(self, row_id: int, held_until: datetime, repeats: int) -> bool:
        """Grava na linha ate quando o valor durou e quantas leituras iguais
        vieram depois da primeira. True se gravou."""
        ...


# Pior caso de atraso do "mantido ate" no banco. Abaixo dos 15 s em que o painel
# le um topico do motor como parado ao reabrir a tela, e dos 60 s em que o
# consumo medio da frota conta o silencio como motor desligado.
FLUSH_AFTER = timedelta(seconds=10)


class HeldStates:
    def __init__(
        self,
        repo: HoldRepository,
        flush_after: timedelta = FLUSH_AFTER,
        log: EventLog = sem_log,
        clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    ):
        self._repo = repo
        self._flush_after = flush_after
        # log(nivel, evt, **campos); o main.py injeta o de logfmt (limitado: a
        # falha se repete a cada atualizacao de cada estado aberto)
        self._log = log
        self._clock = clock
        self._abertos: dict[tuple[str, str], HeldState] = {}
        self._lock = threading.Lock()

    def absorb(self, event: TelemetryEvent) -> bool:
        """True = a leitura e igual a anterior do topico e foi absorvida pelo
        estado aberto (nao grava linha). False = valor novo: quem chama grava a
        linha e depois chama `opened`."""
        if not holds_state(event.category):
            return False
        chave = (event.machine_id, event.category)
        assinatura = state_signature(event.payload)
        with self._lock:
            st = self._abertos.get(chave)
            if st is not None and st.absorbs(assinatura, event.received_at):
                st.repeats += 1
                st.held_until = event.received_at
                if st.written_at is None or event.received_at - st.written_at >= self._flush_after:
                    self._write(st)
                return True
            # Mudou (valor, hora cheia ou leitura fora de ordem): o estado anterior
            # termina aqui, com o que ainda nao estava no banco.
            if st is not None:
                if st.dirty:
                    self._write(st)
                del self._abertos[chave]
            return False

    def opened(self, event: TelemetryEvent, row_id: int | None) -> None:
        """A leitura virou a linha `row_id`: ela abre o estado do topico."""
        if row_id is None or row_id <= 0 or not holds_state(event.category):
            return
        assinatura = state_signature(event.payload)
        if assinatura is None:
            return
        with self._lock:
            self._abertos[(event.machine_id, event.category)] = HeldState(
                row_id=row_id,
                signature=assinatura,
                hour=hour_of(event.received_at),
                started_at=event.received_at,
                written_at=event.received_at,
            )

    def flush_due(self) -> int:
        """Atualiza no banco os estados com repeticoes paradas ha mais de
        `flush_after` (a maquina parou de mandar no meio do estado). Devolve
        quantos atualizou."""
        agora = self._clock()
        feitos = 0
        with self._lock:
            for st in list(self._abertos.values()):
                if st.dirty and st.written_at is not None and agora - st.written_at >= self._flush_after:
                    feitos += self._write(st)
        return feitos

    def flush_all(self) -> int:
        """Grava tudo o que falta (encerramento)."""
        feitos = 0
        with self._lock:
            for st in list(self._abertos.values()):
                if st.dirty:
                    feitos += self._write(st)
        return feitos

    @property
    def open_count(self) -> int:
        return len(self._abertos)

    def _write(self, st: HeldState) -> bool:
        if st.held_until is None:
            return False
        try:
            ok = self._repo.extend_hold(st.row_id, st.held_until, st.repeats)
        except Exception as exc:  # rede, resposta invalida: tenta de novo depois
            self._log("WARN", "estado.falha_atualizar", linha=st.row_id, erro=exc)
            return False
        if ok:
            st.written_repeats = st.repeats
            st.written_at = st.held_until
        return ok
