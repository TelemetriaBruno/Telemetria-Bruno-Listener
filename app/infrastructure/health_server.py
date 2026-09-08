"""
Servidor HTTP minimo, so para plataformas que exigem uma porta aberta.

O Listener e um consumidor MQTT e nao tem API. Alguns hosts (Render Web
Service, por exemplo) derrubam o processo se nada escutar na porta $PORT,
entao este adaptador abre uma porta que responde 200 em qualquer caminho.

Roda em thread daemon: o processo continua terminando quando o loop MQTT
termina, sem precisar encerrar o servidor.
"""

import threading
from http.server import BaseHTTPRequestHandler, HTTPServer


class _HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.end_headers()
        self.wfile.write(b"listener ok")

    def log_message(self, *args) -> None:
        """Silencia o log por requisicao: o ping do keep-alive poluiria a saida."""


def start_health_server(port: int) -> None:
    server = HTTPServer(("0.0.0.0", port), _HealthHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    print(f"[HTTP] Porta {port} aberta para o health check da plataforma.")
