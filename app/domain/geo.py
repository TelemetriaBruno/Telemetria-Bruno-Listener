"""
Regra de dominio: uma linha de telemetria carrega uma posicao utilizavel?

Qualquer category pode trazer a posicao: alem de 'Localizacao', ha firmware
que a manda junto com os horimetros. O que decide e o payload ter latitude e
longitude validas, nao o nome do topico.

Espelha frontend/src/domain/geo.ts, manter em sincronia.

Este e o UNICO ponto que decide o que e uma coordenada valida. O que sai daqui
vai para machines.metadata (ultima posicao conhecida), e o painel le de la em
vez de reinterpretar o payload.

Por que gravar em machines e nao so em telemetry: uma maquina DESLIGADA nao
publica coordenada, e e justamente quando o cliente mais quer saber onde ela
esta. A ultima posicao conhecida no cadastro sobrevive ao desligamento; a
consulta na telemetry, nao: ela cairia fora de qualquer janela.
"""

from dataclasses import dataclass
from datetime import datetime

# O contrato do fabricante nao fixa a grafia; os firmwares variam entre
# acentuado, abreviado e ingles.
_LAT_FIELDS = ("Latitude", "latitude", "Lat", "lat", "LAT")
_LON_FIELDS = ("Longitude", "longitude", "Lon", "lon", "Lng", "lng", "LON", "Long")

_MAX_LAT = 90.0
_MAX_LON = 180.0


@dataclass(frozen=True)
class Position:
    lat: float
    lon: float
    measured_at: datetime


def _parse_coord(raw, max_abs: float) -> float | None:
    """Valor cru -> grau decimal.

    Aceita numero, string decimal (virgula ou ponto, como o CSV do TITAN
    escreve) e o NMEA grau+minuto de alguns modulos GPS ("2333.0280,S").
    """
    if isinstance(raw, bool):
        return None
    if isinstance(raw, (int, float)):
        return float(raw)
    if not isinstance(raw, str):
        return None

    s = raw.strip()
    if s == "":
        return None

    # hemisferio: colado ("23.5S"), ou separado por virgula/espaco
    sign = 1.0
    if s[-1] in "NSEWnsew":
        if s[-1] in "SWsw":
            sign = -1.0
        s = s[:-1].strip().rstrip(",;").strip()

    try:
        n = float(s.replace(",", "."))
    except ValueError:
        return None

    # NMEA ddmm.mmmm: o inteiro passa do limite porque carrega os minutos
    # colados. So reinterpreta quando o decimal seria invalido, para nao
    # estragar um grau decimal legitimo.
    a = abs(n)
    if a > max_abs:
        deg = int(a // 100)
        minutes = a - deg * 100
        if minutes < 60:
            dec = deg + minutes / 60.0
            return sign * dec if dec <= max_abs else None
        return None
    return sign * n


def _first_present(payload: dict, keys) -> object | None:
    for k in keys:
        v = payload.get(k)
        if v is None:
            continue
        if str(v).strip() != "":
            return v
    return None


def position_from_payload(payload: dict, received_at: datetime) -> Position | None:
    """Posicao do payload, ou None se a linha nao tiver coordenada utilizavel.

    None e o caso NORMAL para todo data_type que nao e localizacao. O instante
    prefere o _ts (hora da maquina) e cai para received_at, como em alarm.py.
    """
    lat = _parse_coord(_first_present(payload, _LAT_FIELDS), _MAX_LAT)
    lon = _parse_coord(_first_present(payload, _LON_FIELDS), _MAX_LON)
    if lat is None or lon is None:
        return None
    if abs(lat) > _MAX_LAT or abs(lon) > _MAX_LON:
        return None
    # GPS sem fix reporta exatamente 0,0 (golfo da Guine). Nenhuma maquina da
    # operacao esta la: isso e ausencia de sinal, nao posicao.
    if lat == 0.0 and lon == 0.0:
        return None

    measured_at = received_at
    raw_ts = payload.get("_ts")
    if isinstance(raw_ts, str) and raw_ts.strip():
        try:
            measured_at = datetime.fromisoformat(raw_ts)
        except ValueError:
            pass

    return Position(lat=lat, lon=lon, measured_at=measured_at)
