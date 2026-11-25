"""
O que a esteira de ingestao conta para o log, no lugar de uma linha por mensagem.

Ate 24/11/2026 cada mensagem MQTT virava uma linha ("[MSG] ... gravada"); uma
maquina so manda ~300 por minuto e isso encheu o disco do servidor. Agora:

  - Resumo por maquina a cada janela (60 s), so de quem teve atividade:
        nivel=INFO evt=mqtt.resumo maquina="TRHD BF6969" janela_s=60 msgs=300
            gravadas=20 mantidas=278 falhas=2 topicos=12
    (nivel=WARN quando houve falha.)
  - Descartes sem maquina numa linha so, quando houve algum:
        evt=mqtt.resumo_descartes topico_invalido=N maquina_nao_resolvida=M falha_preparar=K
    e a PRIMEIRA ocorrencia de cada topico invalido / maquina nao resolvida /
    tipo de falha ao preparar, por hora, numa linha propria.
  - Silencio: maquina que vinha mandando e passa de 120 s sem mensagem gera UM
    `maquina.sem_dados`; quando volta, `maquina.voltou`.

Volume: no maximo ~1 linha por maquina por minuto mais as mudancas de estado,
qualquer que seja o ritmo das mensagens.

Chamado por tres threads: a do paho (chegada, no submit), a de preparar
(descartes) e a de gravar (resultado da gravacao e o tique periodico), por isso
a trava. As linhas sao montadas sob a trava e escritas depois dela.
"""

import threading
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable

JANELA_S = 60.0
SILENCIO_S = 120.0
# "Primeira por hora" (topico invalido etc.): quantas chaves lembrar. Cheia de
# chaves recentes, as novas so entram na contagem do resumo: o log nao cresce
# com um aparelho publicando topicos aleatorios.
PRIMEIRA_POR_S = 3600.0
MAX_PRIMEIRAS = 200


@dataclass
class _Contagem:
    msgs: int = 0
    gravadas: int = 0
    mantidas: int = 0
    falhas: int = 0
    topicos: set = field(default_factory=set)


class _PrimeiraPor:
    """Responde se a chave e nova no periodo (a primeira em `periodo_s`)."""

    def __init__(self, periodo_s: float, maximo: int):
        self._periodo = periodo_s
        self._max = maximo
        self._vistas: dict[Any, float] = {}  # ordem = da mais antiga escrita

    def nova(self, chave: Any, agora: float) -> bool:
        while self._vistas:
            primeira = next(iter(self._vistas))
            if agora - self._vistas[primeira] < self._periodo:
                break
            del self._vistas[primeira]
        if chave in self._vistas:
            return False
        if len(self._vistas) >= self._max:
            return False
        self._vistas[chave] = agora
        return True


class ResumoIngestao:
    def __init__(
        self,
        log: Callable[..., None],
        clock: Callable[[], float],
        janela_s: float = JANELA_S,
        silencio_s: float = SILENCIO_S,
    ):
        self._log = log
        self._clock = clock
        self._janela_s = janela_s
        self._silencio_s = silencio_s
        self._trava = threading.Lock()
        self._inicio = clock()
        self._contagens: dict[str, _Contagem] = {}
        self._descartes = {"topico_invalido": 0, "maquina_nao_resolvida": 0, "falha_preparar": 0}
        self._primeiras = _PrimeiraPor(PRIMEIRA_POR_S, MAX_PRIMEIRAS)
        # maquina -> (instante no relogio da esteira, hora da mensagem)
        self._ultima: dict[str, tuple[float, datetime]] = {}
        self._em_silencio: set[str] = set()

    # --- chegada (thread do paho) e descartes (thread de preparar) ---------------

    def chegou(self, maquina: str, quando: datetime) -> None:
        """Mensagem da maquina chegou do broker (`quando` = hora da chegada, UTC)."""
        agora = self._clock()
        linhas = []
        with self._trava:
            anterior = self._ultima.get(maquina)
            self._ultima[maquina] = (agora, quando)
            if maquina in self._em_silencio:
                self._em_silencio.discard(maquina)
                parada = agora - anterior[0] if anterior else 0.0
                linhas.append(("INFO", "maquina.voltou", {"maquina": maquina, "parada_s": int(parada)}))
        self._escrever(linhas)

    def topico_invalido(self, topico: str) -> None:
        self._descarte(
            "topico_invalido", ("topico", topico),
            ("WARN", "mqtt.topico_invalido", {"topico": topico}),
        )

    def maquina_nao_resolvida(self, maquina: str | None, topico: str) -> None:
        # a leitura se perde (cadastro fora do ar): ERRO, uma vez por hora por maquina
        self._descarte(
            "maquina_nao_resolvida", ("maquina", maquina),
            ("ERRO", "mqtt.maquina_nao_resolvida", {"maquina": maquina, "topico": topico}),
        )

    def falha_preparar(self, topico: str, exc: BaseException) -> None:
        self._descarte(
            "falha_preparar", ("falha", type(exc).__name__),
            ("ERRO", "mqtt.falha_preparar", {"topico": topico, "erro": exc}),
        )

    def _descarte(self, motivo: str, chave: tuple, linha: tuple) -> None:
        agora = self._clock()
        with self._trava:
            self._descartes[motivo] += 1
            nova = self._primeiras.nova(chave, agora)
        if nova:
            self._escrever([linha])

    # --- thread de gravar -----------------------------------------------------------

    def gravou(self, maquina: str, categoria: str, resultado: str) -> None:
        """Resultado de `_persistir`: gravada / mantida / FALHOU."""
        with self._trava:
            c = self._contagens.get(maquina)
            if c is None:
                c = self._contagens[maquina] = _Contagem()
            c.msgs += 1
            if resultado == "gravada":
                c.gravadas += 1
            elif resultado == "mantida":
                c.mantidas += 1
            else:
                c.falhas += 1
            c.topicos.add(categoria)

    def tick(self) -> None:
        """Chamado de tempos em tempos (manutencao da esteira), chegue mensagem
        ou nao: fecha a janela vencida e procura maquinas em silencio."""
        agora = self._clock()
        linhas: list = []
        with self._trava:
            if agora - self._inicio >= self._janela_s:
                self._fechar_janela(agora, linhas)
            for maquina, (instante, quando) in self._ultima.items():
                silencio = agora - instante
                if maquina not in self._em_silencio and silencio > self._silencio_s:
                    self._em_silencio.add(maquina)
                    linhas.append(("WARN", "maquina.sem_dados", {
                        "maquina": maquina, "ultima_msg": quando, "silencio_s": int(silencio),
                    }))
        self._escrever(linhas)

    def fechar(self) -> None:
        """Encerramento: o que a janela em curso contou vai para o log."""
        linhas: list = []
        with self._trava:
            self._fechar_janela(self._clock(), linhas)
        self._escrever(linhas)

    # --- apoio ------------------------------------------------------------------------

    def _fechar_janela(self, agora: float, linhas: list) -> None:
        janela_s = int(round(agora - self._inicio))
        for maquina in sorted(self._contagens):
            c = self._contagens[maquina]
            if not c.msgs:
                continue
            linhas.append(("WARN" if c.falhas else "INFO", "mqtt.resumo", {
                "maquina": maquina, "janela_s": janela_s, "msgs": c.msgs,
                "gravadas": c.gravadas, "mantidas": c.mantidas, "falhas": c.falhas,
                "topicos": len(c.topicos),
            }))
        d = self._descartes
        if sum(d.values()):
            # perder leitura por falha (cadastro/bug) e ERRO; topico invalido e
            # so mensagem fora do padrao, descartada de proposito
            perdeu = d["maquina_nao_resolvida"] or d["falha_preparar"]
            linhas.append(("ERRO" if perdeu else "WARN", "mqtt.resumo_descartes", {
                "janela_s": janela_s, **d,
            }))
        self._contagens = {}
        self._descartes = dict.fromkeys(d, 0)
        self._inicio = agora

    def _escrever(self, linhas: list) -> None:
        for nivel, evt, campos in linhas:
            self._log(nivel, evt, **campos)
