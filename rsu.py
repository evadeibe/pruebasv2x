import collections
import json
import os
import threading
import time
from datetime import datetime
import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError
from flask import Flask, jsonify, request
import paho.mqtt.client as mqtt
import requests
from waitress import serve

from v2x_logic import (
    ControladorCooldown,
    DeduplicadorEventos,
    MetricasV2X,
    clasificar_telemetria,
    validar_esquema_evento,
)

RSU_ID = os.getenv("RSU_ID", "rsu-1")
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
SQS_QUEUE_NAME = os.getenv("SQS_QUEUE_NAME", "v2x-events")
MQTT_BROKER_HOST = os.getenv("MQTT_BROKER_HOST", "127.0.0.1")
MQTT_BROKER_PORT = int(os.getenv("MQTT_BROKER_PORT", "1883"))
HTTP_PORT = int(os.getenv("RSU_HTTP_PORT", "80"))
MAX_BUFFER_SIZE = int(os.getenv("RSU_MAX_BUFFER", "1000"))
EDGE_SAMPLE_RATE = int(os.getenv("EDGE_SAMPLE_RATE", "3"))

app = Flask(__name__)

sqs_buffer = collections.deque()
buffer_lock = threading.Lock()

deduplicador_mqtt = DeduplicadorEventos(ttl_seconds=300.0)
deduplicador_sns = DeduplicadorEventos(ttl_seconds=300.0)
cooldown_alerta_local = ControladorCooldown(cooldown_seconds=8.0)
metricas = MetricasV2X()
contador_rutinario_veh = {}

mqtt_client = None
sqs_client = None
queue_url = None


def publicar_alerta_mqtt_local(tipo_alerta: str, mensaje: str, origen: str = "EDGE_RSU"):
    metricas.inc("alerts_triggered")
    if origen == "V2X_SERVICE_SNS":
        metricas.inc("alerts_sns_relay")
    else:
        metricas.inc("alerts_local")

    if mqtt_client is None:
        return
    alerta_payload = {
        "rsu_id": RSU_ID,
        "alert_type": tipo_alerta,
        "origin": origen,
        "timestamp": time.time(),
        "message": mensaje,
    }
    mqtt_client.publish(f"v2x/{RSU_ID}/alerts", json.dumps(alerta_payload), qos=1)


def encolar_para_sqs(evento: dict):
    """Política FIFO Drop-Oldest protegida por lock y contabilizada en dropped_buffer."""
    with buffer_lock:
        if len(sqs_buffer) >= MAX_BUFFER_SIZE:
            sqs_buffer.popleft()
            metricas.inc("dropped_buffer")
        sqs_buffer.append(evento)


def enviar_lote_sqs_una_vez() -> bool:
    """Extrae hasta 10 eventos del buffer y los envía por lote a SQS. Útil también para tests."""
    global queue_url
    with buffer_lock:
        lote = []
        while sqs_buffer and len(lote) < 10:
            lote.append(sqs_buffer.popleft())

    if not lote:
        return False

    try:
        if queue_url is None and sqs_client is not None:
            resp = sqs_client.get_queue_url(QueueName=SQS_QUEUE_NAME)
            queue_url = resp["QueueUrl"]

        entries = [
            {"Id": f"msg-{idx}", "MessageBody": json.dumps(ev)}
            for idx, ev in enumerate(lote)
        ]
        resp = sqs_client.send_message_batch(QueueUrl=queue_url, Entries=entries)

        fallidos = resp.get("Failed", [])
        if fallidos:
            ids_fallidos = {f["Id"] for f in fallidos}
            with buffer_lock:
                for idx, ev in reversed(list(enumerate(lote))):
                    if f"msg-{idx}" in ids_fallidos:
                        if len(sqs_buffer) < MAX_BUFFER_SIZE:
                            sqs_buffer.appendleft(ev)
                        else:
                            metricas.inc("dropped_buffer")

        exitosos = len(resp.get("Successful", []))
        if exitosos > 0:
            metricas.inc("forwarded_or_persisted", exitosos)
            with buffer_lock:
                restantes = len(sqs_buffer)
            metricas.marcar_recuperacion_red(buffer_restante=restantes)

        return True

    except (ClientError, BotoCoreError, Exception) as e:
        metricas.marcar_fallo_red()
        with buffer_lock:
            for ev in reversed(lote):
                if len(sqs_buffer) < MAX_BUFFER_SIZE:
                    sqs_buffer.appendleft(ev)
                else:
                    metricas.inc("dropped_buffer")
        print(f" [RSU WARN] Desconexión SQS ({type(e).__name__}). Buffer retenido: {len(sqs_buffer)} eventos.")
        return False


def worker_envio_sqs():
    while True:
        procesado = enviar_lote_sqs_una_vez()
        if not procesado:
            time.sleep(0.5)


def on_mqtt_connect(client, userdata, flags, rc):
    if rc == 0:
        topic = f"v2x/{RSU_ID}/telemetry"
        client.subscribe(topic, qos=1)
        print(f" [RSU {RSU_ID}] Conectado a broker MQTT local y suscrito a '{topic}'")


def on_mqtt_message(client, userdata, msg):
    """Callback blindado: valida, deduplica, alerta en local con cooldown y filtra hacia SQS."""
    try:
        metricas.inc("received")
        try:
            evento = json.loads(msg.payload.decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            metricas.inc("invalid")
            print(" [RSU WARN] Mensaje MQTT descartado: JSON malformado")
            return

        valido, motivo = validar_esquema_evento(evento)
        if not valido:
            metricas.inc("invalid")
            print(f" [RSU WARN] Evento inválido descartado: {motivo}")
            return

        event_id = evento["event_id"]
        if deduplicador_mqtt.es_duplicado(event_id):
            metricas.inc("duplicates")
            print(f" [RSU DEDUP] Evento duplicado ignorado: {event_id}")
            return

        metricas.inc("accepted")

        try:
            ts_ev = datetime.fromisoformat(evento["event_time"].replace("Z", "+00:00")).timestamp()
            metricas.registrar_latencia((time.time() - ts_ev) * 1000.0)
        except Exception:
            pass

        vel = float(evento["payload"]["velocidad"])
        acc = float(evento["payload"]["aceleracion"])
        veh_id = evento["source_id"]
        clasificacion = clasificar_telemetria(vel, acc)

        evento["rsu_id"] = RSU_ID
        evento["edge_classification"] = clasificacion
        metricas.inc("processed")

        # 1. Priorización: Frenadas bruscas siempre se reenvían a SQS y disparan alerta local con cooldown
        if clasificacion == "SUDDEN_BRAKING":
            if cooldown_alerta_local.puede_disparar(veh_id):
                msg_local = f"Frenada brusca local en {veh_id} (X={evento['location'].get('posicion_x')}m)"
                print(f" 🚨 [RSU LOCAL ALERT] {msg_local}")
                publicar_alerta_mqtt_local("LOCAL_SUDDEN_BRAKING", msg_local, origen="RSU_EDGE")
            encolar_para_sqs(evento)
            return

        # 2. Muestreo en el Edge (1 de cada EDGE_SAMPLE_RATE mensajes rutinarios por vehículo)
        cuenta = contador_rutinario_veh.get(veh_id, 0) + 1
        contador_rutinario_veh[veh_id] = cuenta
        if cuenta % EDGE_SAMPLE_RATE == 0:
            encolar_para_sqs(evento)
        else:
            metricas.inc("filtered_edge")

    except Exception as e:
        print(f" [RSU ERROR] Excepción controlada en callback MQTT: {e}")


@app.route("/sns-notifications", methods=["POST"])
def sns_notifications():
    try:
        data = json.loads(request.data.decode("utf-8"))
    except ValueError:
        return jsonify({"status": "Invalid JSON"}), 400

    msg_type = request.headers.get("x-amz-sns-message-type")
    msg_id = data.get("MessageId")

    if msg_type == "SubscriptionConfirmation":
        subscribe_url = data.get("SubscribeURL")
        if subscribe_url:
            try:
                requests.get(subscribe_url, timeout=5.0)
                print(f" ✅ [RSU {RSU_ID}] Suscripción SNS confirmada: {subscribe_url}")
            except requests.RequestException as e:
                print(f" [RSU ERROR] Fallo al confirmar SNS (se permitirá reintento): {e}")
                return jsonify({"status": "Confirmation failed"}), 500
        return "OK", 200

    elif msg_type == "Notification":
        # La deduplicación por MessageId se aplica exclusivamente a Notification
        if msg_id and deduplicador_sns.es_duplicado(msg_id):
            metricas.inc("duplicates")
            return "OK (Duplicate SNS Notification ignored)", 200

        mensaje = data.get("Message", "")
        print(f" 📡 [RSU {RSU_ID}] Alerta global SNS -> Difundiendo a vehículos en {RSU_ID}: {mensaje}")
        publicar_alerta_mqtt_local("GLOBAL_SNS_ALERT", mensaje, origen="V2X_SERVICE_SNS")
        return "OK", 200

    return "OK", 200


@app.route("/health", methods=["GET"])
def health():
    with buffer_lock:
        buf_len = len(sqs_buffer)
    return jsonify({"rsu_id": RSU_ID, "status": "OK", "buffered_events": buf_len}), 200


@app.route("/metrics", methods=["GET"])
def get_metrics():
    with buffer_lock:
        buf_len = len(sqs_buffer)
    return jsonify(metricas.resumen(extra={"rsu_id": RSU_ID, "buffer_depth": buf_len})), 200


@app.route("/metrics/reset", methods=["POST"])
def reset_metrics():
    metricas.reset()
    return jsonify({"rsu_id": RSU_ID, "status": "METRICS_RESET"}), 200


def main():
    global sqs_client, mqtt_client
    boto_cfg = Config(connect_timeout=3, read_timeout=5, retries={"max_attempts": 1})
    sqs_client = boto3.client("sqs", region_name=AWS_REGION, config=boto_cfg)

    mqtt_client = mqtt.Client(client_id=f"edge-{RSU_ID}")
    mqtt_client.reconnect_delay_set(min_delay=1, max_delay=5)
    mqtt_client.on_connect = on_mqtt_connect
    mqtt_client.on_message = on_mqtt_message
    mqtt_client.connect_async(MQTT_BROKER_HOST, MQTT_BROKER_PORT, keepalive=60)
    mqtt_client.loop_start()

    t_sqs = threading.Thread(target=worker_envio_sqs, daemon=True)
    t_sqs.start()

    print(f"Iniciando {RSU_ID} (MQTT {MQTT_BROKER_PORT} -> SQS '{SQS_QUEUE_NAME}' | HTTP SNS puerto {HTTP_PORT})...")
    serve(app, host="0.0.0.0", port=HTTP_PORT)


if __name__ == "__main__":
    main()