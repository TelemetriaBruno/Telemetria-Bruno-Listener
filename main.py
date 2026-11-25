"""
Raiz de composicao (composition root) do Listener.

Unico lugar que conhece TODAS as camadas: carrega a configuracao, instancia
os adaptadores de infraestrutura, injeta-os no caso de uso e sobe o consumer
MQTT. Nenhuma outra parte da aplicacao importa "para fora" da sua camada.

Uso:
    python main.py
"""

import signal
from datetime import timedelta

from app.application.held_states import HeldStates
from app.application.ingest_telemetry import IngestTelemetry
from app.infrastructure.config import Settings
from app.infrastructure.health_server import start_health_server
from app.infrastructure.ingest_pipeline import IngestPipeline
from app.infrastructure.json_file_writer import JsonFileWriter
from app.infrastructure.logfmt import LogLimitado, log, url_segura
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
    # Log (app/infrastructure/logfmt.py): uma linha logfmt por evento, sem linha
    # por mensagem. O limitado e para a aplicacao, cujas falhas (alarme, posicao,
    # estado mantido) podem se repetir a cada leitura.
    log_limitado = LogLimitado(log)

    # Hosts como o Render matam o processo se nada escutar em $PORT. Sem a
    # variavel (uso local), nenhuma porta e aberta.
    if settings.http_port:
        start_health_server(settings.http_port)

    file_mode = settings.telemetry_sink == "file"
    if not settings.mqtt_username or not settings.mqtt_password:
        log("WARN", "config.mqtt_sem_credenciais")  # MQTT_USERNAME/MQTT_PASSWORD vazios
    if not file_mode and (not settings.rest_url or not settings.rest_key):
        # defina POSTGREST_URL/POSTGREST_KEY ou SUPABASE_URL/SUPABASE_KEY no .env
        log("ERRO", "config.banco_sem_endereco")

    http = build_http_session(settings)

    # Sessao HTTP propria: o cadastro e consultado pela thread que prepara as
    # leituras, e a gravacao usa a outra sessao em outra thread.
    machines = CachedMachineRepository(
        SupabaseMachineRepository(settings, build_http_session(settings)),
        ttl_seconds=settings.machine_cache_ttl,
    )

    # Como o Listener subiu, numa linha so (listener.iniciado, logo antes de
    # conectar ao broker).
    inicio: dict = {}
    if file_mode:
        # Modo so-arquivo: telemetria vai para .jsonl locais; banco nao e tocado.
        telemetry = NullTelemetryRepository()
        fallback = JsonFileWriter(settings.output_dir)
        inicio.update(modo="arquivo", dir=settings.output_dir)
    else:
        telemetry = SupabaseTelemetryRepository(settings, http)
        fallback = JsonFileWriter(settings.output_dir) if settings.save_json_files else None
        # o destino real e o rest_url: POSTGREST_URL (RDS) quando definido, senao
        # o Supabase. So a URL (sem query nem credencial) vai para o log.
        inicio.update(
            modo="banco",
            destino="rds" if settings.postgrest_url else "supabase",
            banco_url=url_segura(settings.postgrest_url or settings.supabase_url),
            copia_jsonl=fallback is not None,
        )

    # Alarmes vao para a tabela dedicada `alarms` (so no modo banco).
    alarms = None if file_mode else SupabaseAlarmRepository(settings, http)

    # Ultima posicao conhecida no cadastro da maquina (so no modo banco): e o
    # que mantem a maquina no mapa depois de desligada.
    positions = None if file_mode else SupabaseMachinePositionRepository(settings, http)

    # Estado mantido: leitura igual a anterior do topico nao vira linha nova, so
    # estende "mantido ate" na linha do estado (ver app/domain/hold.py). So liga
    # se o banco ja tem as colunas (migration 0026); senao grava toda leitura.
    held = None
    if not file_mode and settings.hold_repeated:
        if telemetry.supports_hold():
            held = HeldStates(telemetry, flush_after=timedelta(seconds=settings.hold_flush_s), log=log_limitado)
        else:
            # o motivo exato veio na linha estado.* anterior
            log("WARN", "estado.desligado", motivo="banco_sem_0026")
    elif not file_mode:
        log("INFO", "estado.desligado", motivo="hold_repeated_false")
    if not file_mode:
        inicio["estado_mantido"] = held is not None
        if held is not None:
            inicio["estado_flush_s"] = settings.hold_flush_s

    ingest = IngestTelemetry(
        machines=machines,
        telemetry=telemetry,
        fallback=fallback,
        alarms=alarms,
        positions=positions,
        held=held,
        log=log_limitado,
    )

    notifier = None
    inicio["push"] = bool(settings.stream_notify_url)
    if settings.stream_notify_url:
        notifier = StreamNotifier(
            settings.stream_notify_url, secret=settings.stream_notify_secret
        )
        # empurra cada leitura ao painel em <push_url>/api/internal/notify
        inicio["push_url"] = url_segura(settings.stream_notify_url)
        if not settings.stream_notify_secret:
            # o backend vai recusar o push (401/503) e o painel fica so com o poll HTTP
            log("WARN", "push.sem_segredo")
        elif settings.stream_notify_url.startswith("http://") and not (
            "127.0.0.1" in settings.stream_notify_url
            or "localhost" in settings.stream_notify_url
        ):
            # o segredo autentica, nao cifra: sobre http remoto ele viaja em
            # claro; use https:// para o backend remoto
            log("WARN", "push.sem_tls", push_url=inicio["push_url"])

    # Reserva para quando a fila de gravacao estourar (banco fora por horas).
    overflow = JsonFileWriter(settings.output_dir)
    pipeline = IngestPipeline(ingest, notifier=notifier, overflow=overflow)
    pipeline.start()
    subscriber = MqttSubscriber(settings, pipeline)

    # systemd para o servico com SIGTERM: desconecta do broker e grava o que
    # ainda esta na fila antes de sair, em vez de perder as leituras pendentes.
    # Nada de log dentro do tratador de sinal: so marca o motivo.
    parada = {"motivo": None}

    def ao_sigterm(signum, frame):
        parada["motivo"] = "sigterm"
        subscriber.stop()

    signal.signal(signal.SIGTERM, ao_sigterm)

    log("INFO", "listener.iniciado", **inicio)
    subscriber.run_forever()
    pendentes = pipeline.pendentes
    em_dia = pipeline.stop(timeout=20.0)
    # fora do prazo, o que sobrou na fila nao foi gravado
    log(
        "INFO" if em_dia else "ERRO", "listener.encerrado",
        motivo=parada["motivo"], pendentes_ao_parar=pendentes, todas_gravadas=em_dia,
    )


if __name__ == "__main__":
    main()
