"""
Adaptador de entrada MQTT (paho): conecta no EMQX via TLS, assina os topicos
e entrega cada mensagem a esteira de ingestao (IngestPipeline).

E o unico lugar que conhece paho-mqtt. O callback so enfileira: gravar no banco
aqui dentro travava a thread do paho quando o banco ficava lento, e o broker
derrubava a conexao por keepalive (ver ingest_pipeline.py).
"""

import ssl

import paho.mqtt.client as mqtt

from app.infrastructure.config import Settings
from app.infrastructure.ingest_pipeline import IngestPipeline
from app.infrastructure.logfmt import log


def _motivo(reason_code) -> dict:
    """ReasonCode do paho em dois campos: o numero e o texto ("Not authorized")."""
    return {"reason_code": getattr(reason_code, "value", reason_code), "motivo": str(reason_code)}


class MqttSubscriber:
    def __init__(self, settings: Settings, pipeline: IngestPipeline):
        self._settings = settings
        self._pipeline = pipeline
        self._client = self._build_client()

    def _build_client(self) -> mqtt.Client:
        client = mqtt.Client(
            client_id=self._settings.mqtt_client_id,
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            protocol=mqtt.MQTTv5,
        )
        if self._settings.mqtt_username:
            client.username_pw_set(self._settings.mqtt_username, self._settings.mqtt_password)
        client.tls_set(cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_TLS_CLIENT)
        client.on_connect = self._on_connect
        client.on_disconnect = self._on_disconnect
        client.on_message = self._on_message
        client.reconnect_delay_set(min_delay=1, max_delay=30)
        return client

    # --- Callbacks MQTT -----------------------------------------------------

    def _on_connect(self, client, userdata, flags, reason_code, properties=None):
        if reason_code == 0:
            log("INFO", "mqtt.conectado", host=self._settings.mqtt_host, porta=self._settings.mqtt_port)
            for topic in self._settings.mqtt_topics:
                client.subscribe(topic, qos=self._settings.mqtt_qos)
                log("INFO", "mqtt.inscrito", topico=topic, qos=self._settings.mqtt_qos)
        else:
            # credencial recusada, broker cheio...: o paho tenta de novo sozinho
            log("ERRO", "mqtt.falha_conexao", host=self._settings.mqtt_host, **_motivo(reason_code))

    def _on_disconnect(self, client, userdata, flags, reason_code, properties=None):
        # 0 = desconexao pedida (encerramento); o resto e queda, e o paho reconecta
        nivel = "INFO" if reason_code == 0 else "WARN"
        log(nivel, "mqtt.desconectado", **_motivo(reason_code))

    def _on_message(self, client, userdata, msg):
        # So enfileira (com a hora da chegada) e volta: o paho precisa ficar
        # livre para responder ao broker e receber as proximas mensagens.
        self._pipeline.submit(msg.topic, msg.payload, msg.qos, bool(msg.retain))

    # --- Ciclo de vida --------------------------------------------------------

    def run_forever(self) -> None:
        """Bloqueia ate stop() (ou Ctrl+C) desconectar do broker."""
        s = self._settings
        log("INFO", "mqtt.conectando", host=s.mqtt_host, porta=s.mqtt_port)
        self._client.connect(s.mqtt_host, s.mqtt_port, keepalive=60)
        try:
            self._client.loop_forever()
        except KeyboardInterrupt:
            log("INFO", "listener.interrompido", motivo="ctrl_c")
            self._client.disconnect()

    def stop(self) -> None:
        self._client.disconnect()
