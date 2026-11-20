"""
Configuracao da aplicacao, carregada do ambiente (.env).
Unico lugar que le os.getenv — o resto da aplicacao recebe Settings pronto.
"""

import os
from dataclasses import dataclass, field

from dotenv import load_dotenv


@dataclass(frozen=True)
class Settings:
    # MQTT (EMQX Serverless, TLS)
    mqtt_host: str
    mqtt_port: int
    mqtt_username: str | None
    mqtt_password: str | None
    mqtt_topics: list[str]
    mqtt_qos: int
    mqtt_client_id: str

    # Supabase (API REST / PostgREST)
    supabase_url: str
    supabase_key: str | None
    http_timeout: float

    # PostgREST próprio (EC2 na frente do RDS). Vazios = o do Supabase.
    postgrest_url: str
    postgrest_key: str | None

    # Cache de maquinas (segundos ate revalidar)
    machine_cache_ttl: float

    # Destino da telemetria: "supabase" (banco) ou "file" (so arquivos .jsonl)
    telemetry_sink: str
    # Salvar arquivos .jsonl de auditoria (sempre True quando sink=file)
    save_json_files: bool
    output_dir: str

    # Push em tempo real: URL do telemetria-backend que repassa aos paineis via
    # WebSocket. Vazio desliga o empurrao (painel volta a depender so do poll).
    stream_notify_url: str
    # Porta HTTP exigida por plataformas de hospedagem (Render define $PORT).
    # 0 desliga o servidor de saude, que e o caso do uso local.
    http_port: int

    # Segredo compartilhado com o backend, enviado no header X-Stream-Secret.
    # Precisa ser IGUAL ao STREAM_NOTIFY_SECRET de la, senao o backend recusa o
    # push com 401. Com backend em outra maquina, use https:// na URL acima: o
    # segredo autentica mas nao cifra, e sobre http ele vai em claro.
    stream_notify_secret: str
    # Estado mantido (domain/hold.py): leitura igual a anterior nao vira linha
    # nova. HOLD_REPEATED=false volta a gravar toda leitura.
    hold_repeated: bool = True
    hold_flush_s: float = 10.0

    @property
    def rest_url(self) -> str | None:
        if self.postgrest_url:
            return self.postgrest_url
        return f"{self.supabase_url}/rest/v1" if self.supabase_url else None

    @property
    def rest_key(self) -> str | None:
        return self.postgrest_key or self.supabase_key

    @staticmethod
    def from_env() -> "Settings":
        load_dotenv()
        return Settings(
            mqtt_host=os.getenv("MQTT_HOST", "t0ceb6f9.ala.us-east-1.emqxsl.com"),
            mqtt_port=int(os.getenv("MQTT_PORT", "8883")),
            mqtt_username=os.getenv("MQTT_USERNAME"),
            mqtt_password=os.getenv("MQTT_PASSWORD"),
            mqtt_topics=[
                t.strip()
                for t in os.getenv("MQTT_TOPICS", "tenants/#").split(",")
                if t.strip()
            ],
            mqtt_qos=int(os.getenv("MQTT_QOS", "1")),
            mqtt_client_id=os.getenv("MQTT_CLIENT_ID", "telemetria-listener"),
            supabase_url=os.getenv("SUPABASE_URL", "").rstrip("/"),
            supabase_key=os.getenv("SUPABASE_KEY"),
            http_timeout=float(os.getenv("HTTP_TIMEOUT", "10")),
            postgrest_url=os.getenv("POSTGREST_URL", "").strip().rstrip("/"),
            postgrest_key=os.getenv("POSTGREST_KEY") or None,
            machine_cache_ttl=float(os.getenv("MACHINE_CACHE_TTL", "300")),
            telemetry_sink=os.getenv("TELEMETRY_SINK", "supabase").strip().lower(),
            save_json_files=os.getenv("SAVE_JSON_FILES", "false").lower() == "true",
            output_dir=os.getenv("OUTPUT_DIR", "data"),
            stream_notify_url=os.getenv("STREAM_NOTIFY_URL", "http://127.0.0.1:8800").strip().rstrip("/"),
            http_port=int(os.getenv("PORT", "0")),
            stream_notify_secret=os.getenv("STREAM_NOTIFY_SECRET", "").strip(),
            hold_repeated=os.getenv("HOLD_REPEATED", "true").strip().lower() != "false",
            hold_flush_s=float(os.getenv("HOLD_FLUSH_S", "10")),
        )
