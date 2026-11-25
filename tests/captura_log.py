"""Log injetavel para os testes: guarda cada evento e a linha logfmt que sairia."""

import threading

from app.infrastructure.logfmt import formatar


class Logs:
    def __init__(self):
        self._trava = threading.Lock()
        self.eventos: list[tuple[str, str, dict, str]] = []  # (nivel, evt, campos, linha)

    def __call__(self, nivel, evt, **campos):
        with self._trava:
            self.eventos.append((nivel, evt, dict(campos), formatar(nivel, evt, **campos)))

    @property
    def linhas(self) -> list[str]:
        return [e[3] for e in self.eventos]

    def de(self, evt) -> list[tuple[str, dict]]:
        """(nivel, campos) de cada linha do evento."""
        return [(n, c) for n, e, c, _ in self.eventos if e == evt]

    def linhas_de(self, evt) -> list[str]:
        return [l for _, e, _, l in self.eventos if e == evt]
