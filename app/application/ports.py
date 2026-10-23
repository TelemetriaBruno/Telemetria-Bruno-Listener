"""
Portas (interfaces) que a camada de aplicacao exige do mundo externo.
A infraestrutura implementa; os casos de uso so conhecem estes contratos.
"""

from typing import Protocol

from app.domain.entities import Machine, TelemetryEvent


class MachineRepository(Protocol):
    def find_by_code(self, machine_code: str) -> Machine | None:
        """Retorna a maquina pre-cadastrada, ou None se nao existir."""
        ...

    def ensure_by_code(self, machine_code: str) -> Machine | None:
        """Retorna a maquina; se nao existir, auto-registra como 'pendente'
        (status unassigned, tenant_id None) e retorna a criada. None so em
        caso de falha de rede/config."""
        ...


class TelemetryRepository(Protocol):
    def save(self, event: TelemetryEvent) -> bool:
        """Persiste o evento. Retorna True se gravou."""
        ...


class TelemetryFallbackWriter(Protocol):
    def write(self, event: TelemetryEvent) -> None:
        """Registro auxiliar do evento (ex: arquivo local para auditoria)."""
        ...


class MachinePositionRepository(Protocol):
    def update_position(
        self,
        machine_id: str,
        lat: float,
        lon: float,
        measured_at,
    ) -> bool:
        """Grava a ULTIMA posicao conhecida no cadastro da maquina. Retorna True
        se gravou. Best-effort: falha aqui NAO deve derrubar a ingestao normal."""
        ...


class AlarmRepository(Protocol):
    def save(
        self,
        tenant_id: str | None,
        machine_id: str,
        code: str | None,
        description: str | None,
        activated_at,
        source_id: int | None = None,
        category: str | None = None,
    ) -> bool:
        """Persiste uma ativacao de alarme na tabela dedicada. Retorna True se
        gravou, ou se o mesmo alarme ja estava aberto (nao e um disparo novo).
        Best-effort: falha aqui NAO deve derrubar a ingestao normal."""
        ...

    def clear(self, machine_id: str, category: str, cleared_at) -> bool:
        """Fecha os alarmes ABERTOS da maquina naquele topico (e os sem topico,
        de antes da coluna existir) que dispararam ate `cleared_at`: e o
        "Maquina sem alarmes". Retorna True se a gravacao foi aceita."""
        ...
