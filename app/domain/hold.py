"""
Estado mantido: a leitura igual a anterior nao vira linha nova no banco.

Pedido do usuario (20/11/2026): quando o valor que chega num topico e igual ao
anterior, o painel o mostra ao vivo, mas o banco nao grava de novo. Ele registra
que o valor FICOU naquele estado ate mudar. Exemplo dele: as 14h a bateria
marcava 24 V e as 15h caiu para 12 V; o historico tem de dizer que ela ficou uma
hora em 24 V e depois caiu, e nao inventar uma descida aos poucos nem um buraco.

Por isso cada linha da telemetria passa a ser um ESTADO:
  - received_at: quando o valor chegou pela primeira vez (como sempre foi);
  - held_until:  a chegada da ultima leitura igual (NULL = leitura unica);
  - repeats:     quantas leituras iguais vieram depois da primeira.

Quem le o banco (grafico, resumo por hora, exportacao, consumo da frota, trajeto,
ultima leitura do painel) usa esses dois campos para recompor o periodo inteiro.

Duas regras que mantem o historico exato:
  - o estado nao atravessa a virada da hora (UTC; o fuso -03:00 tem horas
    cheias iguais): o resumo por hora (migration 0023) agrega cada linha UMA vez,
    depois que a hora fecha, entao uma linha nunca pode crescer para a hora
    seguinte. Na virada, o mesmo valor abre uma linha nova;
  - alarme e evento, nao estado: cada mensagem de alarme continua sendo gravada
    (o disparo repetido ja tem regra propria, ver alarm.REPEAT_WINDOW).

"Igual" ignora so o que muda sozinho a cada mensagem: a hora do aparelho
(DataHora/HoraData, e o _ts derivado dela) e o texto bruto (_raw, que a contem).
"""

import json
from dataclasses import dataclass
from datetime import datetime, timezone

from app.domain.alarm import is_alarm_category

# chaves que mudam a cada mensagem sem que o VALOR mude
_VOLATEIS = frozenset({"_ts", "_raw", "DataHora", "HoraData"})


def state_signature(payload: dict | None) -> str | None:
    """O valor da leitura, para comparar com a anterior. None = nao comparavel
    (a leitura e gravada sempre, como antes)."""
    if not isinstance(payload, dict):
        return None
    corpo = {k: v for k, v in payload.items() if k not in _VOLATEIS}
    if not corpo:
        return None
    return json.dumps(corpo, sort_keys=True, ensure_ascii=False, default=str)


def holds_state(category: str | None) -> bool:
    """O topico guarda estado (pode ser mantido)? Alarme e evento: nao."""
    return bool(category) and not is_alarm_category(category)


def hour_of(instant: datetime) -> datetime:
    """A hora cheia (UTC) da leitura: o estado nao passa dela."""
    if instant.tzinfo is None:
        instant = instant.replace(tzinfo=timezone.utc)
    return instant.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)


@dataclass
class HeldState:
    """O estado aberto de um topico de uma maquina: a linha que o representa no
    banco e o quanto dele ainda nao foi escrito la."""

    row_id: int
    signature: str
    hour: datetime
    started_at: datetime
    held_until: datetime | None = None
    repeats: int = 0
    # o que o banco ja sabe: as repeticoes ate a ultima atualizacao da linha
    written_repeats: int = 0
    written_at: datetime | None = None

    @property
    def dirty(self) -> bool:
        return self.repeats != self.written_repeats

    def absorbs(self, signature: str | None, received_at: datetime) -> bool:
        """A leitura e a continuacao deste estado (mesmo valor, mesma hora, sem
        voltar no tempo)?"""
        return (
            signature is not None
            and signature == self.signature
            and hour_of(received_at) == self.hour
            and received_at >= (self.held_until or self.started_at)
        )
