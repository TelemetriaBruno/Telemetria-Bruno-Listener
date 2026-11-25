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

    # Para o estado mantido (domain/hold.py). So o repositorio do banco tem; sem
    # eles a ingestao grava toda leitura.
    def insert(self, event: TelemetryEvent) -> int | None:
        """Persiste o evento e devolve o id da linha (0 = gravou sem saber o id;
        None = nao gravou)."""
        ...

    def extend_hold(self, row_id: int, held_until, repeats: int) -> bool:
        """Grava na linha ate quando o valor durou e quantas leituras iguais
        vieram depois da primeira."""
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
        gravou, ou se esta mesma mensagem ja estava gravada (reentrega).
        Best-effort: falha aqui NAO deve derrubar a ingestao normal."""
        ...

    def fired_since(
        self,
        machine_id: str,
        category: str,
        code: str | None,
        description: str | None,
        since,
    ) -> bool:
        """True se o mesmo alarme (pelo codigo; sem codigo, pela descricao) ja
        tem disparo gravado nesta maquina e topico a partir de `since`. Na
        duvida (falha de rede) responde False: gravar uma repeticao e melhor
        que perder um alarme."""
        ...

    def clear(self, machine_id: str, category: str, cleared_at) -> bool:
        """Fecha os alarmes ABERTOS da maquina naquele topico (e os sem topico,
        de antes da coluna existir) que dispararam ate `cleared_at`: e o
        "Maquina sem alarmes". Retorna True se a gravacao foi aceita."""
        ...


class EventLog(Protocol):
    def __call__(self, nivel: str, evt: str, **campos: object) -> None:
        """Um evento de log: nivel INFO/WARN/ERRO, `evt` no formato area.acao e
        campos chave=valor (a implementacao e infrastructure/logfmt.py, injetada
        pelo main.py: a aplicacao nao importa a infraestrutura)."""
        ...


def sem_log(nivel: str, evt: str, **campos: object) -> None:
    """Log que descarta: padrao de quem nao recebeu um log injetado (testes)."""
