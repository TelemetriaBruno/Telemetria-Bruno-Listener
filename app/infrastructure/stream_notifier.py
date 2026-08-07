"""
Notificador de tempo real: empurra cada evento ingerido para o backend HTTP
(telemetria-backend), que repassa aos paineis conectados via WebSocket.

Por que existe: o subscriber grava no banco, mas o painel nao ve a gravacao na
hora. Este notificador da o "empurrao" imediato — o mesmo instante em que a
mensagem MQTT chega — sem que o painel precise ficar consultando o banco.

Garantias de projeto:
  - NUNCA bloqueia nem derruba o loop MQTT: o envio HTTP roda numa thread
    separada, alimentada por uma fila. Se o backend estiver fora do ar, o POST
    falha em silencio e a ingestao no banco segue normal (o painel reconcilia
    pelo snapshot HTTP).
  - Best-effort: se a fila encher (backend lento), descarta o mais antigo — o
    banco continua sendo a fonte da verdade.
"""

import queue
import threading
from datetime import datetime
from typing import Any

import requests

from app.domain.entities import TelemetryEvent


class StreamNotifier:
    def __init__(
        self,
        base_url: str,
        timeout: float = 2.0,
        max_queue: int = 1000,
        secret: str = "",
    ):
        self._url = base_url.rstrip("/") + "/api/internal/notify"
        self._timeout = timeout
        # Segredo compartilhado: o backend recusa o push sem ele (401). Vai em
        # todo POST; se estiver vazio o backend responde 401/503 e o painel
        # segue reconciliando pelo snapshot HTTP.
        self._headers = {"X-Stream-Secret": secret} if secret else {}
        self._queue: "queue.Queue[dict[str, Any]]" = queue.Queue(maxsize=max_queue)
        self._session = requests.Session()
        self._stop = threading.Event()
        self._worker = threading.Thread(target=self._run, name="stream-notifier", daemon=True)
        self._worker.start()

    def notify(self, event: TelemetryEvent) -> None:
        """Enfileira o evento para envio (nao bloqueia; descarta se cheia)."""
        received = event.received_at
        row = {
            "topic": event.topic,
            "payload": event.payload,
            "qos": event.qos,
            "retain": event.retain,
            "received_at": received.isoformat() if isinstance(received, datetime) else str(received),
            # tenant dono da maquina (None enquanto nao atribuida): o backend usa
            # este campo para so empurrar a linha aos paineis do cliente certo.
            "tenant_id": event.tenant_id,
        }
        try:
            self._queue.put_nowait(row)
        except queue.Full:
            # Backend nao acompanha: solta o mais antigo e enfileira o novo.
            try:
                self._queue.get_nowait()
                self._queue.put_nowait(row)
            except queue.Empty:
                pass

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                row = self._queue.get(timeout=0.5)
            except queue.Empty:
                continue
            try:
                self._session.post(
                    self._url, json=row, headers=self._headers, timeout=self._timeout
                )
            except requests.RequestException:
                # Backend fora do ar / lento: descarta este empurrao. O banco
                # ja tem o dado; o painel reconcilia pelo snapshot HTTP.
                pass

    def stop(self) -> None:
        self._stop.set()
