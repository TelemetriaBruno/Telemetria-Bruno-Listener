"""
Regra de dominio: como interpretar o payload bruto de uma mensagem TITAN.

Os dispositivos TITAN publicam em DOIS formatos (heterogeneo, em evolucao):

  1. Quase-JSON: chaves entre aspas mas VALORES sem aspas, ex:
       {"DataHora": 04/07/2026 - 14:34:46, "RpmMotor": 1857, "PressaoMotor": 4.5}
     Isso NAO e JSON valido (timestamp e strings sem aspas). Parseamos de
     forma tolerante extraindo pares "chave": valor.

  2. CSV posicional separado por ';', ex:
       04/07/2026 - 14:34;0;0;0;Producao Geral
     O 1o campo e sempre o timestamp (HoraData). Os demais sao nomeados pelo
     schema do data_type (ver app.domain.schemas); sem schema, viram col_1..N.

Em todos os casos o texto bruto e preservado em "_raw" para reprocessamento.
O timestamp da origem, quando reconhecido, e normalizado em "_ts" (ISO-8601).
"""

import json
import re
from datetime import datetime, timedelta, timezone

from app.domain.schemas import fields_for

# Fuso dos dispositivos TITAN/THOR: o relogio da maquina publica em horario
# local do Brasil (UTC-3). Gravamos o _ts com este offset para que ele seja
# AWARE — sem isso o timestamp fica naive e sua conversao para epoch dependeria
# do fuso do servidor que o consome.
_DEVICE_TZ = timezone(timedelta(hours=-3))

# Timestamp TITAN: "04/07/2026 - 14:34:46", "04/07/2026 - 14:09" (sem segundos),
# ou com hora SEM zero a esquerda "05/07/2026 - 1:43:52" / "08/07/2026 - 0:02:44".
# Por isso hora/min/seg aceitam 1 ou 2 digitos.
_TS_RE = re.compile(
    r"^\s*(\d{1,2})/(\d{1,2})/(\d{4})\s*-\s*(\d{1,2}):(\d{1,2})(?::(\d{1,2}))?\s*$"
)

# Par "chave": valor  do quase-JSON (valor vai ate a proxima virgula ou "}").
_KV_RE = re.compile(r'"([^"]+)"\s*:\s*([^,}]+)')

# Decimal pt-BR: digitos, UMA virgula, digitos (ex: "27,4", "120,0"). Sinal
# opcional. NAO casa "1,234,5" (milhar) nem "Producao, Geral" (texto).
_DECIMAL_COMMA_RE = re.compile(r"^[+-]?\d+,\d+$")


def _parse_ts(value: str) -> str | None:
    """Converte o timestamp TITAN em ISO-8601, ou None se nao casar."""
    m = _TS_RE.match(value)
    if not m:
        return None
    d, mo, y, hh, mm, ss = m.groups()
    ss = ss or "00"
    try:
        return datetime(
            int(y), int(mo), int(d), int(hh), int(mm), int(ss), tzinfo=_DEVICE_TZ
        ).isoformat()
    except ValueError:
        return None


def _coerce(value: str):
    """Converte um token de texto em int/float/bool quando possivel."""
    v = value.strip().strip('"')
    if v == "":
        return None
    low = v.lower()
    if low in ("true", "false"):
        return low == "true"
    # inteiro
    try:
        return int(v)
    except ValueError:
        pass
    # float (aceita 4.5)
    try:
        return float(v)
    except ValueError:
        pass
    # float com virgula decimal pt-BR (ex: "27,4", "12,5"). Os dispositivos
    # TITAN/THOR publicam decimais com virgula; sem isto o valor seria gravado
    # como texto e o painel nao plotaria. So trata UMA virgula entre digitos —
    # nao interpreta separador de milhar nem toca em texto com virgula.
    if _DECIMAL_COMMA_RE.match(v):
        try:
            return float(v.replace(",", "."))
        except ValueError:
            pass
    return v  # texto (ex: "Producao Geral", timestamp)


def _looks_like_pseudo_json(text: str) -> bool:
    t = text.lstrip()
    return t.startswith("{") and '":' in t


def _decode_pseudo_json(text: str) -> dict:
    """Extrai pares chave/valor do quase-JSON TITAN, tolerante a valores sem aspas."""
    out: dict = {}
    for key, raw_val in _KV_RE.findall(text):
        val = raw_val.strip().rstrip("}").strip()
        # O valor pode vir com ou sem aspas (Thor nova usa aspas). Testa o
        # timestamp sobre a versao sem aspas.
        unquoted = val.strip('"')
        ts = _parse_ts(unquoted)
        if ts is not None:
            out[key] = unquoted      # preserva o texto original do campo
            out["_ts"] = ts          # e a versao normalizada
        else:
            out[key] = _coerce(val)
    return out


def _postprocess_json_dict(data: dict) -> dict:
    """
    Pos-processa um dict vindo de JSON valido (Thor com aspas em tudo):
      - normaliza o campo de timestamp (DataHora/HoraData) em _ts;
      - coage strings numericas ("1862" -> 1862, "4.5" -> 4.5).
    Preserva o valor original do campo de timestamp como string.
    """
    out: dict = {}
    for key, value in data.items():
        if key in ("DataHora", "HoraData") and isinstance(value, str):
            out[key] = value
            ts = _parse_ts(value)
            if ts is not None:
                out["_ts"] = ts
        elif isinstance(value, str):
            out[key] = _coerce(value)
        else:
            out[key] = value
    return out


def _decode_csv(text: str, data_type: str | None) -> dict:
    """Parseia CSV ';' posicional; nomeia campos pelo schema do data_type."""
    parts = text.split(";")
    ts_iso = _parse_ts(parts[0]) if parts else None

    names = fields_for(data_type, len(parts)) if data_type else None
    out: dict = {}
    if names:
        for name, raw_val in zip(names, parts):
            out[name] = _coerce(raw_val)
    else:
        # Sem schema: HoraData + col_1..N (mantem tudo, nada se perde).
        out["HoraData"] = parts[0] if parts else None
        for i, raw_val in enumerate(parts[1:], start=1):
            out[f"col_{i}"] = _coerce(raw_val)

    if ts_iso is not None:
        out["_ts"] = ts_iso
    return out


def decode_payload(raw: bytes, data_type: str | None = None) -> dict:
    """
    Interpreta o payload TITAN. Sempre inclui "_raw" (texto original).
    Retorna binario em {"raw_hex": ...} se nao for texto decodificavel.
    """
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        return {"raw_hex": raw.hex(), "encoding": "binary"}

    text = text.strip()

    # JSON valido de verdade. Ocorre quando a THOR poe aspas em TODOS os valores
    # (o payload vira JSON legitimo). Precisa do mesmo pos-processamento do
    # pseudo-json: normalizar timestamp em _ts e coagir strings numericas.
    if text.startswith("{") or text.startswith("["):
        try:
            data = json.loads(text)
            if isinstance(data, dict):
                return {**_postprocess_json_dict(data), "_raw": text}
            return {"json": data, "_raw": text}
        except (json.JSONDecodeError, ValueError):
            pass

    # Quase-JSON TITAN (chaves com aspas, valores sem).
    if _looks_like_pseudo_json(text):
        parsed = _decode_pseudo_json(text)
        parsed["_raw"] = text
        return parsed

    # CSV posicional.
    if ";" in text:
        parsed = _decode_csv(text, data_type)
        parsed["_raw"] = text
        return parsed

    # Nao reconhecido: guarda como texto puro.
    return {"text": text, "_raw": text}
