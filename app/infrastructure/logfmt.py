"""
Log do Listener em logfmt: uma linha por evento, em stdout, com flush.

    nivel=WARN evt=maquina.sem_dados maquina="TRHD BF6969" ultima_msg=2026-11-25T14:03:07Z silencio_s=121

Por que existe (24/11/2026): o Listener escrevia uma linha por mensagem MQTT
("[MSG] ... gravada"); so uma maquina manda ~300 mensagens/min e isso encheu o
disco do servidor (syslog ~150 MB/dia). Agora o volume e pequeno e constante:
resumos por janela, avisos de mudanca de estado e erros com limite de repeticao.

Padrao (o mesmo do backend):
  - Sem data/hora: o journald ja carimba cada linha.
  - Comeca com `nivel=INFO|WARN|ERRO evt=<area>.<acao>` e segue com `chave=valor`.
  - Chaves em minusculas, ASCII sem acento (`maquina`, `topico`, `tabela`...).
  - Valor vazio ou com espaco, `=` ou aspas vai entre aspas duplas, com `\\"` e
    `\\\\` escapados; quebra de linha (e outro caractere de controle) vira espaco.
  - `None` some da linha (campo opcional nao preenchido); bool vira true/false;
    datetime vira ISO em UTC (`2026-11-25T14:03:07Z`); excecao vira
    `TipoDaExcecao: mensagem`.
  - `erro=` e cortado em 300 caracteres, `resposta=` em 200, o resto em 300.
  - Niveis: INFO = ciclo de vida e resumos; WARN = degradado mas recuperavel
    (nova tentativa, maquina em silencio, mensagem descartada); ERRO = falha
    que perde dado ou precisa de gente.
  - NUNCA logar tokens, senhas, chaves, e-mails, payload nem querystring. O
    texto das excecoes passa por `limpar_segredos` (as do `requests` trazem a
    URL com a querystring da consulta).
"""

import re
import sys
import threading
import time
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable
from urllib.parse import urlsplit, urlunsplit

NIVEIS = ("INFO", "WARN", "ERRO")

# Limite de caracteres por campo (antes de escapar). `resposta` e o corpo de
# erro do PostgREST; `erro`, o texto da excecao.
LIMITE_PADRAO = 300
LIMITES = {"erro": 300, "resposta": 200}

_CHAVE_INVALIDA = re.compile(r"[^a-z0-9_]")
# Caracteres que quebrariam a linha no journald ou no terminal.
_CONTROLE = re.compile("\r\n|[\x00-\x1f\x7f\x85  ]")
# Querystring de URL (sempre tem `=`): ?machine_code=eq.X&select=id
_QUERYSTRING = re.compile(r"\?[^\s'\"()<>]*=[^\s'\"()<>]*")
# Usuario e senha embutidos na URL: https://usuario:senha@host
_USERINFO = re.compile(r"(\b[a-zA-Z][a-zA-Z0-9+.-]*://)[^/\s@]+@")


# --- formatador (puro) ------------------------------------------------------------


def formatar(nivel: str, evt: str, **campos: Any) -> str:
    """Monta a linha logfmt. Campos com valor None ficam de fora."""
    partes = [f"nivel={_valor('nivel', str(nivel).upper())}", f"evt={_valor('evt', evt)}"]
    for chave, valor in campos.items():
        if valor is None:
            continue
        chave = _CHAVE_INVALIDA.sub("_", str(chave).lower()) or "_"
        partes.append(f"{chave}={_valor(chave, valor)}")
    return " ".join(partes)


def _valor(chave: str, valor: Any) -> str:
    texto = _texto(valor)
    texto = _CONTROLE.sub(" ", texto)
    limite = LIMITES.get(chave, LIMITE_PADRAO)
    if len(texto) > limite:
        texto = texto[: limite - 3] + "..."
    if texto == "" or any(c.isspace() or c in '="' for c in texto):
        return '"' + texto.replace("\\", "\\\\").replace('"', '\\"') + '"'
    return texto


def _texto(valor: Any) -> str:
    if isinstance(valor, bool):
        return "true" if valor else "false"
    if isinstance(valor, int):
        return str(valor)
    if isinstance(valor, float):
        if valor != valor or valor in (float("inf"), float("-inf")):
            return str(valor)
        texto = repr(round(valor, 3))
        return texto[:-2] if texto.endswith(".0") else texto
    if isinstance(valor, datetime):
        return iso_utc(valor)
    if isinstance(valor, BaseException):
        return descrever_erro(valor)
    if isinstance(valor, Enum):
        return str(valor.value)
    return str(valor)


def iso_utc(quando: datetime) -> str:
    """2026-11-25T14:03:07Z. Sem fuso, assume que ja esta em UTC."""
    if quando.tzinfo is not None:
        quando = quando.astimezone(timezone.utc)
    return quando.strftime("%Y-%m-%dT%H:%M:%SZ")


def descrever_erro(exc: BaseException) -> str:
    """`TipoDaExcecao: mensagem`, sem querystring nem senha de URL."""
    mensagem = str(exc).strip()
    texto = f"{type(exc).__name__}: {mensagem}" if mensagem else type(exc).__name__
    return limpar_segredos(texto)


def limpar_segredos(texto: str) -> str:
    """Tira querystring e usuario:senha de URLs no meio do texto."""
    texto = _USERINFO.sub(r"\1", texto)
    return _QUERYSTRING.sub("?(omitida)", texto)


def url_segura(url: str | None) -> str:
    """So esquema, host, porta e caminho: sem usuario, senha, query nem fragmento."""
    if not url:
        return ""
    try:
        partes = urlsplit(url)
    except ValueError:
        return limpar_segredos(url)
    if not partes.scheme or not partes.hostname:
        return limpar_segredos(url)
    host = partes.hostname
    if ":" in host:  # IPv6
        host = f"[{host}]"
    if partes.port:
        host = f"{host}:{partes.port}"
    return urlunsplit((partes.scheme, host, partes.path, "", ""))


# --- saida ------------------------------------------------------------------------

# RLock: o tratador de sinal roda na thread principal e nao pode travar se ela
# estiver no meio de uma escrita.
_trava = threading.RLock()


def escrever(linha: str) -> None:
    """Uma linha no stdout, inteira (sem misturar threads) e com flush: sob o
    systemd o stdout nao e um terminal e o Python o bufferiza."""
    try:
        with _trava:
            saida = sys.stdout
            saida.write(linha + "\n")
            saida.flush()
    except Exception:  # stdout fechado/pipe quebrado: log nunca derruba quem chama
        pass


def log(nivel: str, evt: str, **campos: Any) -> None:
    """Escreve o evento. Ex.: log("WARN", "fila.atrasada", pendentes=250)."""
    try:
        linha = formatar(nivel, evt, **campos)
    except Exception:  # um __str__ quebrado num valor nao pode derrubar a thread
        linha = "nivel=ERRO evt=log.falha_formatar evento=" + _valor("evento", evt if isinstance(evt, str) else "?")
    escrever(linha)


Log = Callable[..., None]


class LogLimitado:
    """Envolve um log: a mesma ocorrencia passa no maximo uma vez por
    `intervalo_s`; as seguradas entram como `suprimidas=N` na proxima que passar.

    Para os erros que podem se repetir a cada mensagem (banco recusando todo
    insert com 401, posicao que nao grava...): sem isso, uma falha persistente
    escreveria centenas de linhas por minuto, que e o problema que este modulo
    resolve. "A mesma ocorrencia" = nivel + evt + os campos de CAMPOS_CHAVE + o
    tipo da excecao em `erro`.

    Uma rajada que termina antes do intervalo nao tem as suprimidas contadas em
    linha nenhuma (so aparecem se a falha voltar depois)."""

    CAMPOS_CHAVE = ("maquina", "tabela", "operacao", "status")

    def __init__(
        self,
        log: Log | None = None,
        intervalo_s: float = 60.0,
        clock: Callable[[], float] = time.monotonic,
        max_chaves: int = 500,
    ):
        self._log = log
        self._intervalo = intervalo_s
        self._clock = clock
        self._max = max_chaves
        # chave -> [instante da ultima linha escrita, suprimidas desde entao];
        # ordem de insercao = ordem da ultima escrita (a mais antiga na frente)
        self._vistas: dict[tuple, list] = {}
        self._trava = threading.Lock()

    def __call__(self, nivel: str, evt: str, **campos: Any) -> None:
        chave = self._chave(nivel, evt, campos)
        agora = self._clock()
        with self._trava:
            vista = self._vistas.get(chave)
            if vista is not None and agora - vista[0] < self._intervalo:
                vista[1] += 1
                return
            suprimidas = vista[1] if vista is not None else 0
            self._vistas.pop(chave, None)
            self._vistas[chave] = [agora, 0]
            self._podar()
        if suprimidas:
            campos["suprimidas"] = suprimidas
        (self._log or log)(nivel, evt, **campos)

    def _chave(self, nivel: str, evt: str, campos: dict) -> tuple:
        partes: list[Any] = [nivel, evt]
        for nome in self.CAMPOS_CHAVE:
            if campos.get(nome) is not None:
                partes.append((nome, str(campos[nome])))
        erro = campos.get("erro")
        if isinstance(erro, BaseException):
            partes.append(("erro", type(erro).__name__))
        elif erro is not None:
            partes.append(("erro", str(erro).split(":", 1)[0][:60]))
        return tuple(partes)

    def _podar(self) -> None:
        """Memoria limitada: passando do maximo, esquece as chaves escritas ha
        mais tempo (as da frente)."""
        while len(self._vistas) > self._max:
            del self._vistas[next(iter(self._vistas))]
