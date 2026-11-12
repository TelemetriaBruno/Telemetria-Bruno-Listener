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
            print(f"[OK] Conectado a {self._settings.mqtt_host}:{self._settings.mqtt_port}")
            for topic in self._settings.mqtt_topics:
                client.subscribe(topic, qos=self._settings.mqtt_qos)
                print(f"[SUB] Inscrito em '{topic}' (QoS {self._settings.mqtt_qos})")
        else:
            print(f"[ERRO] Falha na conexao MQTT. reason_code={reason_code}")

    def _on_disconnect(self, client, userdata, flags, reason_code, properties=None):
        print(f"[OFF] Desconectado do MQTT (reason_code={reason_code}). Reconectando...")

    def _on_message(self, client, userdata, msg):
        # So enfileira (com a hora da chegada) e volta: o paho precisa ficar
        # livre para responder ao broker e receber as proximas mensagens.
        self._pipeline.submit(msg.topic, msg.payload, msg.qos, bool(msg.retain))

    # --- Ciclo de vida --------------------------------------------------------

    def run_forever(self) -> None:
        """Bloqueia ate stop() (ou Ctrl+C) desconectar do broker."""
        s = self._settings
        print(f"Conectando a {s.mqtt_host}:{s.mqtt_port} ...")
        self._client.connect(s.mqtt_host, s.mqtt_port, keepalive=60)
        try:
            self._client.loop_forever()
        except KeyboardInterrupt:
            print("\n[FIM] Encerrando por interrupcao do usuario.")
            self._client.disconnect()

    def stop(self) -> None:
        self._client.disconnect()
