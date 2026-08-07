"""
Regra de dominio: uma linha 'Alarmes Ativos' representa uma ATIVACAO de alarme?

Cada mensagem dessa category carrega, quando ha alarme, um codigo e/ou uma
descricao; payload so com a hora (sem esses campos) significa "sem alarme".
Aqui extraimos a ativacao (ou None) de um payload ja decodificado.

Este e o UNICO ponto que decide o que e ativacao: o que sai daqui vai para a
tabela `alarms`, e o backend le essa tabela em vez de reinterpretar o payload.
"""

from dataclasses import dataclass
from datetime import datetime

# category cujas linhas sao (ou nao) ativacoes de alarme
ALARM_CATEGORY = "Alarmes Ativos"
_CODE_FIELDS = ("CodigoAlarme", "Codigo", "codigo")
_DESC_FIELDS = ("Descricao", "Descrição", "descricao")


@dataclass(frozen=True)
class Alarm:
    code: str | None
    description: str | None
    activated_at: datetime


def _first_present(payload: dict, keys) -> str | None:
    for k in keys:
        v = payload.get(k)
        if v is None:
            continue
        s = str(v).strip()
        if s != "":
            return s
    return None


def alarm_from_payload(payload: dict, received_at: datetime) -> Alarm | None:
    """Ativacao de alarme do payload, ou None se a linha nao tiver alarme.
    O instante prefere o _ts (hora da maquina); cai para received_at."""
    code = _first_present(payload, _CODE_FIELDS)
    desc = _first_present(payload, _DESC_FIELDS)
    if code is None and desc is None:
        return None

    activated_at = received_at
    raw_ts = payload.get("_ts")
    if isinstance(raw_ts, str) and raw_ts.strip():
        try:
            activated_at = datetime.fromisoformat(raw_ts)
        except ValueError:
            pass
    return Alarm(code=code, description=desc, activated_at=activated_at)
