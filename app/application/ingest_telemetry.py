"""
Caso de uso central do Listener: ingerir uma mensagem MQTT.

Fluxo:
  1. Faz parse do topico ({machine_code}/{data_type}).
  2. Resolve a maquina (via MachineRepository); auto-registra como pendente
     (sem cliente) se ainda nao existir. So falha se o repositorio cair.
  3. Monta o TelemetryEvent e persiste (via TelemetryRepository).

Nao conhece MQTT, HTTP nem banco: recebe dados crus e fala com portas.
"""

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Callable

from app.application.ports import (
    AlarmRepository,
    MachinePositionRepository,
    MachineRepository,
    TelemetryFallbackWriter,
    TelemetryRepository,
)
from app.domain.alarm import REPEAT_WINDOW, alarm_from_payload, is_alarm_category, is_all_clear, message_time
from app.domain.entities import TelemetryEvent
from app.domain.geo import position_from_payload
from app.domain.payload import decode_payload
from app.domain.topic import parse_topic


class IngestStatus(Enum):
    SAVED = "saved"
    INVALID_TOPIC = "invalid_topic"
    UNKNOWN_MACHINE = "unknown_machine"
    SAVE_FAILED = "save_failed"


@dataclass(frozen=True)
class IngestResult:
    status: IngestStatus
    event: TelemetryEvent | None = None
    machine_code: str | None = None


class IngestTelemetry:
    def __init__(
        self,
        machines: MachineRepository,
        telemetry: TelemetryRepository,
        fallback: TelemetryFallbackWriter | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
        alarms: AlarmRepository | None = None,
        positions: MachinePositionRepository | None = None,
    ):
        self._machines = machines
        self._telemetry = telemetry
        self._fallback = fallback
        self._clock = clock
        self._alarms = alarms
        self._positions = positions

    def execute(self, topic: str, payload: bytes, qos: int, retain: bool) -> IngestResult:
        parsed = parse_topic(topic)
        if parsed is None:
            return IngestResult(IngestStatus.INVALID_TOPIC)

        # Auto-registra a maquina se ela ainda nao existir (status pendente,
        # sem cliente). None aqui = falha de rede/config, nao "desconhecida".
        machine = self._machines.ensure_by_code(parsed.machine_code)
        if machine is None:
            return IngestResult(
                IngestStatus.UNKNOWN_MACHINE, machine_code=parsed.machine_code
            )

        event = TelemetryEvent(
            tenant_id=machine.tenant_id,
            machine_id=machine.id,
            category=parsed.data_type,
            topic=topic,
            qos=qos,
            retain=retain,
            received_at=self._clock(),
            payload=decode_payload(payload, parsed.data_type),
        )

        saved = self._telemetry.save(event)
        if self._fallback is not None:
            self._fallback.write(event)

        # Alarme: se esta linha e uma ATIVACAO, grava tambem na tabela dedicada
        # `alarms`; se e o "Maquina sem alarmes", fecha os alarmes abertos do
        # topico. Best-effort e isolado — falha aqui nao afeta a telemetria.
        #
        # O try e o que torna "best-effort" verdadeiro: os repositorios devolvem
        # False no caminho previsto, mas uma excecao inesperada (rede caindo no
        # meio, JSON invalido na resposta) subiria por aqui e mataria a mensagem
        # JA gravada, e a telemetria seria perdida por causa do registro auxiliar.
        if self._alarms is not None and is_alarm_category(event.category):
            try:
                if is_all_clear(event.payload):
                    self._alarms.clear(
                        machine_id=event.machine_id,
                        category=event.category,
                        cleared_at=message_time(event.payload, event.received_at),
                    )
                else:
                    alarm = alarm_from_payload(event.payload, event.received_at)
                    # o mesmo alarme de novo dentro da janela e a maquina
                    # repetindo a mensagem, nao um acionamento (REPEAT_WINDOW)
                    if alarm is not None and not self._alarms.fired_since(
                        machine_id=event.machine_id,
                        category=event.category,
                        code=alarm.code,
                        description=alarm.description,
                        since=alarm.activated_at - REPEAT_WINDOW,
                    ):
                        self._alarms.save(
                            tenant_id=event.tenant_id,
                            machine_id=event.machine_id,
                            code=alarm.code,
                            description=alarm.description,
                            activated_at=alarm.activated_at,
                            category=event.category,
                        )
            except Exception as exc:
                print(f"[ALARME] Falha ao registrar ativacao (ignorado): {exc}")

        # Localizacao: alem da linha na telemetry, atualiza a ULTIMA posicao
        # conhecida no cadastro da maquina. E o que mantem a maquina no mapa
        # depois de desligada, quando ela para de publicar coordenada.
        # Best-effort e isolado, como o alarme.
        #
        # Vale para QUALQUER category com latitude e longitude, nao so para
        # "Localizacao": ha firmware que manda a posicao junto com os
        # horimetros. position_from_payload exige o par completo e valido, entao
        # as outras categorias dao None e nao chegam ao banco.
        if self._positions is not None:
            try:
                pos = position_from_payload(event.payload, event.received_at)
                if pos is not None:
                    self._positions.update_position(
                        machine_id=event.machine_id,
                        lat=pos.lat,
                        lon=pos.lon,
                        measured_at=pos.measured_at,
                    )
            except Exception as exc:
                print(f"[POSICAO] Falha ao gravar posicao (ignorado): {exc}")

        status = IngestStatus.SAVED if saved else IngestStatus.SAVE_FAILED
        return IngestResult(status, event=event, machine_code=parsed.machine_code)
