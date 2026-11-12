"""
Raiz de composicao (composition root) do Listener.

Unico lugar que conhece TODAS as camadas: carrega a configuracao, instancia
os adaptadores de infraestrutura, injeta-os no caso de uso e sobe o consumer
MQTT. Nenhuma outra parte da aplicacao importa "para fora" da sua camada.

Uso:
    python main.py
"""

import signal

from app.application.ingest_telemetry import IngestTelemetry
from app.infrastructure.config import Settings
from app.infrastructure.health_server import start_health_server
from app.infrastructure.ingest_pipeline import IngestPipeline
from app.infrastructure.json_file_writer import JsonFileWriter
from app.infrastructure.machine_cache import CachedMachineRepository
from app.infrastructure.mqtt_subscriber import MqttSubscriber
from app.infrastructure.null_telemetry import NullTelemetryRepository
from app.infrastructure.stream_notifier import StreamNotifier
from app.infrastructure.supabase import (
    SupabaseAlarmRepository,
    SupabaseMachinePositionRepository,
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
    if not settings.rest_url or not settings.rest_key:
        print("[AVISO] banco sem endereco: defina POSTGREST_URL/POSTGREST_KEY ou SUPABASE_URL/SUPABASE_KEY no .env.")

    http = build_http_session(settings)

    # Sessao HTTP propria: o cadastro e consultado pela thread que prepara as
    # leituras, e a gravacao usa a outra sessao em outra thread.
    machines = CachedMachineRepository(
        SupabaseMachineRepository(settings, build_http_session(settings)),
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
        # o destino real e o rest_url: POSTGREST_URL (RDS) quando definido, senao
        # o Supabase. So a URL vai para o log; a chave fica de fora.
        destino = (
            f"PostgREST proprio (RDS) em {settings.postgrest_url}"
            if settings.postgrest_url
            else f"Supabase em {settings.supabase_url or '(SUPABASE_URL vazio)'}"
        )
        print(f"[MODO] Telemetria no banco via {destino} ({onoff} copia local .jsonl)")

    # Alarmes vao para a tabela dedicada `alarms` (so no modo banco).
    alarms = None if file_mode else SupabaseAlarmRepository(settings, http)

    # Ultima posicao conhecida no cadastro da maquina (so no modo banco): e o
    # que mantem a maquina no mapa depois de desligada.
    positions = None if file_mode else SupabaseMachinePositionRepository(settings, http)

    ingest = IngestTelemetry(
        machines=machines,
        telemetry=telemetry,
        fallback=fallback,
        alarms=alarms,
        positions=positions,
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

    # Reserva para quando a fila de gravacao estourar (banco fora por horas).
    overflow = JsonFileWriter(settings.output_dir)
    pipeline = IngestPipeline(ingest, notifier=notifier, overflow=overflow)
    pipeline.start()
    subscriber = MqttSubscriber(settings, pipeline)

    # systemd para o servico com SIGTERM: desconecta do broker e grava o que
    # ainda esta na fila antes de sair, em vez de perder as leituras pendentes.
    signal.signal(signal.SIGTERM, lambda signum, frame: subscriber.stop())

    subscriber.run_forever()
    pendentes = pipeline.pendentes
    em_dia = pipeline.stop(timeout=20.0)
    print(
        f"[FIM] Listener encerrado. Fila ao parar: {pendentes} leitura(s); "
        + ("todas gravadas." if em_dia else "nem todas couberam no prazo.")
    )


if __name__ == "__main__":
    main()
