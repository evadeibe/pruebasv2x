import collections
import json
import os
import threading
import time
import uuid
from datetime import datetime
from decimal import Decimal
import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError
from flask import Flask, jsonify
from waitress import serve

from v2x_logic import (
    ControladorCooldown,
    DeduplicadorEventos,
    DetectorHazard,
    MetricasV2X,
    clasificar_telemetria,
    validar_esquema_evento,
)

AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
SQS_QUEUE_NAME = os.getenv("SQS_QUEUE_NAME", "v2x-events")
SNS_TOPIC_NAME = os.getenv("SNS_TOPIC_NAME", "v2x-alerts")
DYNAMO_EVENTS_TABLE = os.getenv("DYNAMO_EVENTS_TABLE", "v2x-events-store")
DYNAMO_ALERTS_TABLE = os.getenv("DYNAMO_ALERTS_TABLE", "v2x-alerts-store")
LOG_GROUP = os.getenv("CLOUDWATCH_LOG_GROUP", "/v2x-mvp/alerts")
LOG_STREAM = os.getenv("CLOUDWATCH_LOG_STREAM", "v2x-service-cloud")
HTTP_PORT = int(os.getenv("SERVICE_HTTP_PORT", "8080"))

app = Flask(__name__)

sqs = None
logs = None
sns = None
dynamodb = None
queue_url = None
sns_topic_arn = None

state_lock = threading.Lock()
ultimos_eventos = collections.deque(maxlen=50)
ultimas_alertas = collections.deque(maxlen=50)

deduplicador = DeduplicadorEventos(ttl_seconds=300.0)
cooldown_frenada_cloud = ControladorCooldown(cooldown_seconds=8.0)
detector_hazard = DetectorHazard(time_window_s=5.0, max_distance_m=250.0, cooldown_s=12.0)
metricas = MetricasV2X()


def inicializar_aws():
    global sqs, logs, sns, dynamodb, queue_url, sns_topic_arn
    boto_cfg = Config(connect_timeout=3, read_timeout=5, retries={"max_attempts": 2})
    sqs = boto3.client("sqs", region_name=AWS_REGION, config=boto_cfg)
    logs = boto3.client("logs", region_name=AWS_REGION, config=boto_cfg)
    sns = boto3.client("sns", region_name=AWS_REGION, config=boto_cfg)
    dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION, config=boto_cfg)

    queue_url = sqs.get_queue_url(QueueName=SQS_QUEUE_NAME)["QueueUrl"]
    topic_resp = sns.create_topic(Name=SNS_TOPIC_NAME)
    sns_topic_arn = topic_resp["TopicArn"]

    try:
        logs.create_log_group(logGroupName=LOG_GROUP)
    except logs.exceptions.ResourceAlreadyExistsException:
        pass

    try:
        logs.create_log_stream(logGroupName=LOG_GROUP, logStreamName=LOG_STREAM)
    except logs.exceptions.ResourceAlreadyExistsException:
        pass


def enviar_cloudwatch(mensaje: str):
    if logs is None:
        return
    try:
        logs.put_log_events(
            logGroupName=LOG_GROUP,
            logStreamName=LOG_STREAM,
            logEvents=[{"timestamp": int(time.time() * 1000), "message": mensaje}],
        )
    except (ClientError, BotoCoreError) as e:
        print(f" [CW WARN] Error enviando a CloudWatch: {e}")


def publicar_sns(tipo_alerta: str, mensaje: str, metadata: dict, max_intentos: int = 3) -> bool:
    """Publica en SNS con hasta 3 intentos para evitar perder alertas ante fallos puntuales."""
    if sns is None or not sns_topic_arn:
        return False
    payload_str = json.dumps({"alert_type": tipo_alerta, "message": mensaje, "metadata": metadata})
    for intento in range(1, max_intentos + 1):
        try:
            sns.publish(
                TopicArn=sns_topic_arn,
                Subject=f"V2X Alert: {tipo_alerta}",
                Message=payload_str,
            )
            return True
        except (ClientError, BotoCoreError) as e:
            print(f" [SNS WARN] Intento {intento}/{max_intentos} fallido publicando en SNS: {e}")
            if intento < max_intentos:
                time.sleep(0.3 * intento)
    return False


def guardar_evento_dynamodb(nombre_tabla: str, item: dict, pk_field: str = "event_id") -> str:
    if dynamodb is None:
        return "STORED"
    tabla = dynamodb.Table(nombre_tabla)
    item_decimal = json.loads(json.dumps(item), parse_float=Decimal)
    try:
        tabla.put_item(
            Item=item_decimal,
            ConditionExpression=f"attribute_not_exists({pk_field})",
        )
        return "STORED"
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code == "ConditionalCheckFailedException":
            return "DUPLICATE"
        raise


def procesar_evento(body: dict, es_reintento_sqs: bool = False) -> bool:
    if es_reintento_sqs:
        metricas.inc("sqs_redeliveries")
    else:
        metricas.inc("received")

    valido, motivo = validar_esquema_evento(body)
    if not valido:
        metricas.inc("invalid")
        print(f" [WARN] Evento descartado por esquema inválido: {motivo}")
        return True

    event_id = body["event_id"]
    if deduplicador.ya_visto(event_id):
        metricas.inc("duplicates")
        print(f" [DEDUP MEM] Evento duplicado descartado: {event_id}")
        return True

    veh_id = body["source_id"]
    ts_str = body["event_time"]
    try:
        ts_unix = datetime.fromisoformat(ts_str.replace("Z", "+00:00")).timestamp()
        metricas.registrar_latencia((time.time() - ts_unix) * 1000.0)
    except (ValueError, TypeError):
        ts_unix = time.time()

    vel = float(body["payload"]["velocidad"])
    acc = float(body["payload"]["aceleracion"])
    pos_x = float(body["location"]["posicion_x"])

    estado_derivado = clasificar_telemetria(vel, acc)
    body["derived_state"] = estado_derivado

    try:
        resultado_db = guardar_evento_dynamodb(DYNAMO_EVENTS_TABLE, body, pk_field="event_id")
        metricas.marcar_recuperacion_red(buffer_restante=0)
    except (ClientError, BotoCoreError, Exception) as e:
        metricas.marcar_fallo_red()
        print(f" [DYNAMO ERROR] Fallo al persistir evento {event_id} ({e}). Se reintentará desde SQS.")
        return False

    deduplicador.registrar_visto(event_id)

    if resultado_db == "DUPLICATE":
        metricas.inc("duplicates")
        print(f" [DEDUP DYNAMO] Evento ya existente en DynamoDB ignorado: {event_id}")
        return True

    metricas.inc("accepted")
    metricas.inc("processed")
    metricas.inc("forwarded_or_persisted")

    with state_lock:
        ultimos_eventos.append(body)

    if estado_derivado == "SUDDEN_BRAKING":
        if cooldown_frenada_cloud.puede_disparar(veh_id, now=ts_unix):
            msg_frenada = f"[SUDDEN_BRAKING] Frenada brusca en {veh_id} (X={pos_x}m, Accel={acc}m/s²)."
            print(f" 🚨 {msg_frenada}")
            alerta_item = {
                "alert_id": f"alt-{event_id}",
                "event_id": event_id,
                "alert_type": "SUDDEN_BRAKING",
                "source_id": veh_id,
                "timestamp": ts_str,
                "posicion_x": pos_x,
                "message": msg_frenada,
            }
            with state_lock:
                ultimas_alertas.append(alerta_item)
            metricas.inc("alerts_triggered")
            enviar_cloudwatch(msg_frenada)
            publicar_sns("SUDDEN_BRAKING", msg_frenada, alerta_item)
            try:
                guardar_evento_dynamodb(DYNAMO_ALERTS_TABLE, alerta_item, pk_field="alert_id")
            except Exception as e:
                print(f" [DYNAMO WARN] No se pudo guardar alerta {alerta_item['alert_id']}: {e}")

        implicados = detector_hazard.registrar_frenada(veh_id, ts_unix, pos_x)
        if implicados:
            msg_hazard = (
                f"[HAZARD_DETECTED] Peligro en la vía cerca de X={pos_x}m: "
                f"Múltiples frenadas en <=5s ({', '.join(implicados)})"
            )
            print(f" ⚠️ {msg_hazard}")
            hazard_item = {
                "alert_id": f"haz-{int(ts_unix*1000)}-{uuid.uuid4().hex[:6]}",
                "alert_type": "HAZARD_DETECTED",
                "vehicles": implicados,
                "timestamp": ts_str,
                "posicion_x": pos_x,
                "message": msg_hazard,
            }
            with state_lock:
                ultimas_alertas.append(hazard_item)
            metricas.inc("alerts_triggered")
            enviar_cloudwatch(msg_hazard)
            publicar_sns("HAZARD_DETECTED", msg_hazard, hazard_item)
            try:
                guardar_evento_dynamodb(DYNAMO_ALERTS_TABLE, hazard_item, pk_field="alert_id")
            except Exception as e:
                print(f" [DYNAMO WARN] No se pudo guardar hazard {hazard_item['alert_id']}: {e}")
    else:
        print(f" [{estado_derivado}] Vehículo: {veh_id} | X: {pos_x}m | Vel: {vel} | Accel: {acc}")

    return True


def leer_sqs_continuamente():
    print(f"Iniciando lectura por lotes de SQS ({SQS_QUEUE_NAME})...")
    while True:
        try:
            response = sqs.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=5,
                AttributeNames=["ApproximateReceiveCount"],
            )
            mensajes = response.get("Messages", [])
            if not mensajes:
                continue

            entradas_borrado = []
            for msg in mensajes:
                receipt_handle = msg["ReceiptHandle"]
                msg_id = msg["MessageId"]
                receive_count = int(msg.get("Attributes", {}).get("ApproximateReceiveCount", "1"))
                es_reintento = receive_count > 1

                try:
                    body = json.loads(msg["Body"])
                except ValueError:
                    metricas.inc("received")
                    metricas.inc("invalid")
                    print(f" [ERROR] Mensaje con JSON corrupto ID={msg_id}. Se elimina para evitar bloqueo.")
                    entradas_borrado.append({"Id": msg_id, "ReceiptHandle": receipt_handle})
                    continue

                try:
                    debe_borrarse = procesar_evento(body, es_reintento_sqs=es_reintento)
                    if debe_borrarse:
                        entradas_borrado.append({"Id": msg_id, "ReceiptHandle": receipt_handle})
                except Exception as e:
                    print(f" [ERROR] Excepción inesperada procesando mensaje {msg_id}: {e}")

            if entradas_borrado:
                del_resp = sqs.delete_message_batch(QueueUrl=queue_url, Entries=entradas_borrado)
                if del_resp.get("Failed"):
                    print(f" [SQS WARN] Fallo al borrar algunos mensajes del lote: {del_resp['Failed']}")

        except (ClientError, BotoCoreError, Exception) as e:
            metricas.marcar_fallo_red()
            print(f" [SQS ERROR] Error leyendo cola SQS: {e}")
            time.sleep(2.0)


@app.route("/events", methods=["GET"])
def get_events():
    with state_lock:
        datos = list(ultimos_eventos)
    return jsonify(datos), 200


@app.route("/alerts", methods=["GET"])
def get_alerts():
    with state_lock:
        datos = list(ultimas_alertas)
    return jsonify(datos), 200


@app.route("/health", methods=["GET"])
def get_health():
    with state_lock:
        n_ev = len(ultimos_eventos)
        n_al = len(ultimas_alertas)
    return jsonify({
        "service": "V2X-SERVICE",
        "status": "OK",
        "processed_recent_events": n_ev,
        "active_alerts": n_al,
    }), 200


@app.route("/metrics", methods=["GET"])
def get_metrics():
    q_depth = -1
    q_inflight = -1
    if sqs is not None and queue_url is not None:
        try:
            attrs = sqs.get_queue_attributes(
                QueueUrl=queue_url,
                AttributeNames=["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"],
            )["Attributes"]
            q_depth = int(attrs.get("ApproximateNumberOfMessages", 0))
            q_inflight = int(attrs.get("ApproximateNumberOfMessagesNotVisible", 0))
        except Exception:
            pass
    return jsonify(
        metricas.resumen(
            extra={
                "service": "V2X-SERVICE",
                "sqs_queue_depth": q_depth,
                "sqs_in_flight": q_inflight,
            }
        )
    ), 200


@app.route("/metrics/reset", methods=["POST"])
def reset_metrics():
    metricas.reset()
    return jsonify({"service": "V2X-SERVICE", "status": "METRICS_RESET"}), 200


def main():
    inicializar_aws()
    hilo_sqs = threading.Thread(target=leer_sqs_continuamente, daemon=True)
    hilo_sqs.start()
    print(f"Iniciando V2X-SERVICE Cloud API en puerto {HTTP_PORT}...")
    serve(app, host="0.0.0.0", port=HTTP_PORT)


if __name__ == "__main__":
    main()