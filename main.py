"""
Raiz de composicao (composition root) do Listener.

Unico lugar que conhece TODAS as camadas: carrega a configuracao, instancia
os adaptadores de infraestrutura, injeta-os no caso de uso e sobe o consumer
MQTT. Nenhuma outra parte da aplicacao importa "para fora" da sua camada.

Uso:
    python main.py
"""

from app.application.ingest_telemetry import IngestTelemetry
from app.infrastructure.config import Settings
from app.infrastructure.health_server import start_health_server
from app.infrastructure.json_file_writer import JsonFileWriter
from app.infrastructure.machine_cache import CachedMachineRepository
from app.infrastructure.mqtt_subscriber import MqttSubscriber
from app.infrastructure.null_telemetry import NullTelemetryRepository
from app.infrastructure.stream_notifier import StreamNotifier
from app.infrastructure.supabase import (
    SupabaseAlarmRepository,
    SupabaseMachineRepository,
    SupabaseTelemetryRepository,
    build_http_session,
)


def main() -> None:
    settings = Settings.from_env()

    # Hosts como o Render matam o processo se nada escutar em $PORT. Sem a
    # variavel (uso local), nenhuma porta e aberta.
    if settings.http_port:
        start_health_server(settings.http_port)

    if not settings.mqtt_username or not settings.mqtt_password:
        print("[AVISO] MQTT_USERNAME/MQTT_PASSWORD nao definidos no .env.")
    if not settings.supabase_url or not settings.supabase_key:
        print("[AVISO] SUPABASE_URL/SUPABASE_KEY nao definidos no .env.")

    http = build_http_session(settings)

    machines = CachedMachineRepository(
        SupabaseMachineRepository(settings, http),
        ttl_seconds=settings.machine_cache_ttl,
    )

    file_mode = settings.telemetry_sink == "file"
    if file_mode:
        # Modo so-arquivo: telemetria vai para .jsonl locais; banco nao e tocado.
        telemetry = NullTelemetryRepository()
        fallback = JsonFileWriter(settings.output_dir)
        print(f"[MODO] Telemetria salva APENAS em arquivos .jsonl em '{settings.output_dir}/'")
    else:
        telemetry = SupabaseTelemetryRepository(settings, http)
        fallback = JsonFileWriter(settings.output_dir) if settings.save_json_files else None
        onoff = "com" if fallback else "sem"
        print(f"[MODO] Telemetria no Supabase ({onoff} copia local .jsonl)")

    # Alarmes vao para a tabela dedicada `alarms` (so no modo banco).
    alarms = None if file_mode else SupabaseAlarmRepository(settings, http)

    ingest = IngestTelemetry(
        machines=machines, telemetry=telemetry, fallback=fallback, alarms=alarms
    )

    notifier = None
    if settings.stream_notify_url:
        notifier = StreamNotifier(
            settings.stream_notify_url, secret=settings.stream_notify_secret
        )
        print(f"[PUSH] Empurrando telemetria ao painel via {settings.stream_notify_url}/api/internal/notify")
        if not settings.stream_notify_secret:
            print(
                "[PUSH] AVISO: STREAM_NOTIFY_SECRET vazio. O backend vai recusar o "
                "push (401/503) e o painel ficara so com o poll HTTP."
            )
        elif settings.stream_notify_url.startswith("http://") and not (
            "127.0.0.1" in settings.stream_notify_url
            or "localhost" in settings.stream_notify_url
        ):
            # o segredo autentica, nao cifra: sobre http remoto ele viaja em claro
            print(
                "[PUSH] AVISO: STREAM_NOTIFY_URL remoto sem TLS. O segredo vai em "
                "claro na rede; use https:// para o backend remoto."
            )

    MqttSubscriber(settings, ingest, notifier).run_forever()


if __name__ == "__main__":
    main()
