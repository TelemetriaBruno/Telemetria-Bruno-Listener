"""
Regra de dominio: uma linha de alarme representa uma ATIVACAO de alarme?

Ha dois formatos no campo:

  - Simulador (Publisher), category "Alarmes Ativos": CodigoAlarme e Descricao
    separados; payload so com a hora significa "sem alarme".
  - Firmware real (TITAN BF0167, out/2026), categories "Alarmes" (CLP da
    maquina) e "Alarmes Motor Diesel" (central do motor): um EVENTO por
    mensagem, com codigo e descricao juntos no campo "A-":
        {"DataHora": "23/10/2026 - 7:58:45","A-": "9-Rede CAN IHM porta 0 em Falha"}
        {"DataHora": "22/10/2026 - 15:51:34","A-": "SPN158-Tensão Instavel no Sistema"}
    Quando nao ha mais alarme, o MESMO topico manda "Maquina sem alarmes" no
    lugar do alarme: e o fim dos alarmes daquele topico, nao um alarme novo.

Aqui extraimos a ativacao (ou None) de um payload ja decodificado, e dizemos
se ele e o aviso de "sem alarmes".

Este e o UNICO ponto que decide o que e ativacao: o que sai daqui vai para a
tabela `alarms`, e o backend le essa tabela em vez de reinterpretar o payload.
"""

import re
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timedelta

# categories cujas linhas sao (ou nao) ativacoes de alarme: "Alarmes",
# "Alarmes Ativos", "Alarmes Motor Diesel"... Por padrao de nome, como o painel
# agrupa os data_types, para um tipo novo do firmware nao passar despercebido.
_ALARM_CATEGORY_RE = re.compile(r"^alarmes?(\s|$)", re.IGNORECASE)
_CODE_FIELDS = ("CodigoAlarme", "Codigo", "codigo")
_DESC_FIELDS = ("Descricao", "Descrição", "descricao")
# campo do firmware real com "<codigo>-<descricao>"
_COMBINED_FIELD = "A-"
# codigo numerico do CLP ("9") ou SPN do motor ("SPN158"); a descricao pode ter
# hifens proprios ("Aviso no Motor Diesel - Verifique a Tela..."), por isso o
# corte e so no primeiro
_COMBINED_RE = re.compile(r"^([A-Za-z]*\d+)\s*-\s*(.*)$")
# O firmware costuma mandar o mesmo alarme duas vezes seguidas: 2 a 5 s de
# intervalo, as vezes ate ~30 s (medido na TITAN BF0167, 21-23/10/2026). Um
# acionamento de verdade (apertar a emergencia de novo) vem minutos depois.
# Dentro desta janela o mesmo alarme e REPETICAO e nao vira disparo novo.
REPEAT_WINDOW = timedelta(seconds=60)
# "Maquina sem alarmes", com ou sem acento e em qualquer caixa; o firmware
# escreve em cp1252, entao "Máquina" tambem pode chegar
_ALL_CLEAR_RE = re.compile(r"\bsem alarmes?\b")


def is_alarm_category(category: str) -> bool:
    return bool(_ALARM_CATEGORY_RE.match(category))


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


def _plain(text: str) -> str:
    """Minusculo, sem acento e com espacos simples, para comparar texto."""
    nfkd = unicodedata.normalize("NFKD", text)
    sem_acento = "".join(c for c in nfkd if not unicodedata.combining(c))
    return " ".join(sem_acento.lower().split())


def is_all_clear(payload: dict) -> bool:
    """True quando a mensagem e o aviso de "Maquina sem alarmes" do topico."""
    text = _first_present(payload, (_COMBINED_FIELD, *_DESC_FIELDS))
    return text is not None and bool(_ALL_CLEAR_RE.search(_plain(text)))


def _split_combined(value: str) -> tuple[str | None, str | None]:
    """'9-Rede CAN IHM porta 0 em Falha' -> ('9', 'Rede CAN IHM porta 0 em Falha').
    Sem codigo reconhecivel na frente, o texto inteiro vira a descricao."""
    m = _COMBINED_RE.match(value)
    if not m:
        return None, value
    return m.group(1), m.group(2).strip() or None


def alarm_from_payload(payload: dict, received_at: datetime) -> Alarm | None:
    """Ativacao de alarme do payload, ou None se a linha nao tiver alarme (ou
    for o "sem alarmes", que fecha alarmes em vez de abrir).
    O instante prefere o _ts (hora da maquina); cai para received_at."""
    if is_all_clear(payload):
        return None
    code = _first_present(payload, _CODE_FIELDS)
    desc = _first_present(payload, _DESC_FIELDS)
    if code is None and desc is None:
        combined = _first_present(payload, (_COMBINED_FIELD,))
        if combined is not None:
            code, desc = _split_combined(combined)
    if code is None and desc is None:
        return None
    return Alarm(code=code, description=desc, activated_at=message_time(payload, received_at))


def message_time(payload: dict, received_at: datetime) -> datetime:
    """Hora da mensagem: o _ts (hora da maquina) quando valido; senao a de
    chegada. E a hora do disparo e tambem a da resolucao."""
    raw_ts = payload.get("_ts")
    if isinstance(raw_ts, str) and raw_ts.strip():
        try:
            return datetime.fromisoformat(raw_ts)
        except ValueError:
            pass
    return received_at
