import argparse
import json
import os
import random
import threading
import time
import uuid
from datetime import datetime, timezone
import paho.mqtt.client as mqtt

from v2x_logic import (
    BRAKING_THRESHOLD,
    CIRCUIT_LENGTH_M,
    determinar_tipo_evento_simulador,
)

print_lock = threading.Lock()
stop_event = threading.Event()


class CanalInyeccionFallos:
    """
    Instancia dedicada por vehículo con registro de métricas de inyección
    y vaciado garantizado (wait_for_publish) antes de hacer handover.
    """
    def __init__(self, drop_prob=0.0, dup_prob=0.0, delay_prob=0.0, max_delay=2.0, reorder_prob=0.0, burst_size=1):
        self.drop_prob = drop_prob
        self.dup_prob = dup_prob
        self.delay_prob = delay_prob
        self.max_delay = max(0.2, float(max_delay))
        self.reorder_prob = reorder_prob
        self.burst_size = max(1, int(burst_size))
        self.buffer_reorden = []
        self.buffer_rafaga = []
        self.timers_pendientes = []  # Lista de [timer, topic, payload_json, event_id]
        self.lock = threading.Lock()

        self.stats = {
            "generated": 0,
            "sent_unique": 0,
            "mqtt_publishes": 0,
            "dropped": 0,
            "delayed": 0,
            "duplicated_extra": 0,
            "reordered": 0,
            "burst_buffered": 0,
        }
        self.sent_event_ids = []

    def _publicar_con_registro(self, mqtt_client: mqtt.Client, topic: str, payload_json: str, event_id: str = None, wait: bool = False):
        info = mqtt_client.publish(topic, payload_json, qos=1)
        if wait and hasattr(info, "wait_for_publish"):
            try:
                info.wait_for_publish(timeout=1.0)
            except Exception:
                pass
        self.stats["mqtt_publishes"] += 1
        if event_id and event_id not in self.sent_event_ids:
            self.sent_event_ids.append(event_id)
            self.stats["sent_unique"] += 1
        return info

    def flush(self, mqtt_client: mqtt.Client):
        """Cancela timers pendientes y publica todo esperando confirmación QoS1 antes de desconectar."""
        with self.lock:
            for item in list(self.timers_pendientes):
                timer, t_d, p_d, eid_d = item
                timer.cancel()
                self._publicar_con_registro(mqtt_client, t_d, p_d, eid_d, wait=True)
            self.timers_pendientes.clear()

            while self.buffer_rafaga:
                t_b, p_b, eid_b = self.buffer_rafaga.pop(0)
                self._publicar_con_registro(mqtt_client, t_b, p_b, eid_b, wait=True)

            while self.buffer_reorden:
                t_old, p_old, eid_old = self.buffer_reorden.pop(0)
                self._publicar_con_registro(mqtt_client, t_old, p_old, eid_old, wait=True)

    def enviar(self, mqtt_client: mqtt.Client, topic: str, payload_json: str, event_id: str = None):
        with self.lock:
            self.stats["generated"] += 1

            # 1. Pérdida simulada
            if random.random() < self.drop_prob:
                self.stats["dropped"] += 1
                return "DROPPED"

            # 2. Retardo simulado con seguimiento para handover seguro
            if random.random() < self.delay_prob:
                self.stats["delayed"] += 1
                delay_s = random.uniform(0.2, self.max_delay)
                cliente_actual = mqtt_client
                registro_timer = [None, topic, payload_json, event_id]

                def _disparar_retardo():
                    with self.lock:
                        if registro_timer in self.timers_pendientes:
                            self.timers_pendientes.remove(registro_timer)
                        else:
                            return
                        self._publicar_con_registro(cliente_actual, topic, payload_json, event_id, wait=False)

                t = threading.Timer(delay_s, _disparar_retardo)
                registro_timer[0] = t
                self.timers_pendientes.append(registro_timer)
                t.start()
                return f"DELAYED({delay_s:.1f}s)"

            # 3. Reordenación simulada
            if random.random() < self.reorder_prob:
                self.stats["reordered"] += 1
                self.buffer_reorden.append((topic, payload_json, event_id))
                return "BUFFERED_REORDER"

            # 4. Ráfagas (Burst)
            if self.burst_size > 1:
                self.stats["burst_buffered"] += 1
                self.buffer_rafaga.append((topic, payload_json, event_id))
                if len(self.buffer_rafaga) < self.burst_size:
                    return f"BURST_WAIT({len(self.buffer_rafaga)}/{self.burst_size})"
                while self.buffer_rafaga:
                    t_b, p_b, eid_b = self.buffer_rafaga.pop(0)
                    self._publicar_con_registro(mqtt_client, t_b, p_b, eid_b, wait=False)
            else:
                self._publicar_con_registro(mqtt_client, topic, payload_json, event_id, wait=False)

            while self.buffer_reorden:
                t_old, p_old, eid_old = self.buffer_reorden.pop(0)
                self._publicar_con_registro(mqtt_client, t_old, p_old, eid_old, wait=False)

            # 5. Duplicación simulada (mismo event_id enviado por segunda vez)
            if random.random() < self.dup_prob:
                self.stats["duplicated_extra"] += 1
                self._publicar_con_registro(mqtt_client, topic, payload_json, event_id=None, wait=False)
                return "DUPLICATED"

            return "MQTT_OK"


def extraer_texto_alerta(alerta_dict: dict) -> str:
    """Desempaqueta el mensaje interno si proviene de un JSON anidado de SNS."""
    raw_msg = alerta_dict.get("message", "")
    if isinstance(raw_msg, str) and raw_msg.strip().startswith("{"):
        try:
            interno = json.loads(raw_msg)
            if isinstance(interno, dict) and "message" in interno:
                return f"[{interno.get('alert_type', 'SNS')}] {interno['message']}"
        except ValueError:
            pass
    return str(raw_msg)


class VehiculoOBU(threading.Thread):
    def __init__(self, veh_id, pos_inicial, vel_crucero_kmh, accel_arranque, t_frenada_s, dt, rsu_endpoints, canal_fallos):
        super().__init__(daemon=True)
        self.veh_id = veh_id
        self.posicion_x = float(pos_inicial) % CIRCUIT_LENGTH_M
        self.velocidad_ms = 0.0
        self.vel_crucero_ms = vel_crucero_kmh / 3.6
        self.accel_arranque = float(accel_arranque)
        self.aceleracion = 0.0
        self.t_frenada_s = float(t_frenada_s)
        self.dt = float(dt)
        self.rsu_endpoints = rsu_endpoints
        self.canal_fallos = canal_fallos

        self.fase = "ARRANQUE"
        self.tiempo_en_crucero = 0.0
        self.tiempo_detenido = 0.0
        self.tick = 0

        self.rsu_actual = "rsu-1" if self.posicion_x < 1000.0 else "rsu-2"
        self.mqtt_client = None
        self._conectar_mqtt(self.rsu_actual)

    def _conectar_mqtt(self, rsu_id: str):
        if self.mqtt_client is not None:
            try:
                self.canal_fallos.flush(self.mqtt_client)
                self.mqtt_client.loop_stop()
                self.mqtt_client.disconnect()
            except Exception:
                pass

        host, port = self.rsu_endpoints[rsu_id]
        client = mqtt.Client(client_id=f"{self.veh_id}-{rsu_id}-{uuid.uuid4().hex[:4]}")
        client.reconnect_delay_set(min_delay=1, max_delay=5)

        def on_connect(c, userdata, flags, rc):
            if rc == 0:
                c.subscribe(f"v2x/{rsu_id}/alerts", qos=1)

        def on_message(c, userdata, msg):
            try:
                alerta = json.loads(msg.payload.decode("utf-8"))
                texto_limpio = extraer_texto_alerta(alerta)
                with print_lock:
                    print(f" 📲 [{self.veh_id}] ALERTA RECIBIDA desde {rsu_id.upper()}: {texto_limpio}")
            except Exception as e:
                with print_lock:
                    print(f" [WARN] Error procesando alerta MQTT en {self.veh_id}: {e}")

        client.on_connect = on_connect
        client.on_message = on_message
        try:
            client.connect_async(host, port, keepalive=60)
            client.loop_start()
        except Exception as e:
            with print_lock:
                print(f" [MQTT WARN] {self.veh_id} fallo inicial preparando {rsu_id} ({host}:{port}): {e}")
        self.mqtt_client = client

    def cerrar(self):
        if self.mqtt_client is not None:
            try:
                self.canal_fallos.flush(self.mqtt_client)
                self.mqtt_client.loop_stop()
                self.mqtt_client.disconnect()
            except Exception:
                pass

    def actualizar_cinematica(self, dt_real: float):
        if self.fase == "ARRANQUE":
            self.aceleracion = self.accel_arranque
            if self.velocidad_ms >= self.vel_crucero_ms:
                self.velocidad_ms = self.vel_crucero_ms
                self.aceleracion = 0.0
                self.fase = "CRUCERO"
                self.tiempo_en_crucero = 0.0
        elif self.fase == "CRUCERO":
            self.aceleracion = 0.0
            self.tiempo_en_crucero += dt_real
            if self.tiempo_en_crucero >= self.t_frenada_s:
                self.fase = "FRENADA"
        elif self.fase == "FRENADA":
            self.aceleracion = -6.0
            if self.velocidad_ms <= 0.0:
                self.velocidad_ms = 0.0
                self.aceleracion = 0.0
                self.fase = "DETENIDO"
                self.tiempo_detenido = 0.0
        elif self.fase == "DETENIDO":
            self.aceleracion = 0.0
            self.velocidad_ms = 0.0
            self.tiempo_detenido += dt_real
            if self.tiempo_detenido >= 3.0:
                self.fase = "ARRANQUE"

        vel_ant = self.velocidad_ms
        self.velocidad_ms = max(0.0, self.velocidad_ms + self.aceleracion * dt_real)
        vel_media = (vel_ant + self.velocidad_ms) / 2.0
        self.posicion_x = (self.posicion_x + vel_media * dt_real) % CIRCUIT_LENGTH_M

    def run(self):
        time.sleep(random.uniform(0.1, 1.0))
        ultimo_t = time.monotonic()

        while not stop_event.is_set():
            ahora = time.monotonic()
            dt_real = ahora - ultimo_t
            ultimo_t = ahora
            self.tick += 1

            self.actualizar_cinematica(dt_real)

            velocidad_kmh = round(self.velocidad_ms * 3.6, 2)
            pos_redondeada = round(self.posicion_x, 2)
            accel_redondeada = round(self.aceleracion, 2)

            rsu_destino = "rsu-1" if self.posicion_x < 1000.0 else "rsu-2"
            if rsu_destino != self.rsu_actual:
                with print_lock:
                    print(f"\n{'='*75}")
                    print(f" 📡 [HANDOVER] {self.veh_id} cambió de zona (X={pos_redondeada}m): {self.rsu_actual} ---> {rsu_destino}")
                    print(f"{'='*75}\n")
                self.rsu_actual = rsu_destino
                self._conectar_mqtt(self.rsu_actual)

            event_type = determinar_tipo_evento_simulador(velocidad_kmh, accel_redondeada, self.tick)
            event_id = f"evt-{uuid.uuid4().hex[:12]}"

            evento_v2x = {
                "event_id": event_id,
                "source_id": self.veh_id,
                "event_type": event_type,
                "event_time": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
                "location": {
                    "segment": "segment-A12",
                    "rsu_zone": self.rsu_actual,
                    "posicion_x": pos_redondeada,
                },
                "payload": {
                    "velocidad": velocidad_kmh,
                    "aceleracion": accel_redondeada,
                    "fase": self.fase,
                },
            }
            payload_json = json.dumps(evento_v2x)
            topic = f"v2x/{self.rsu_actual}/telemetry"

            try:
                estado_envio = self.canal_fallos.enviar(self.mqtt_client, topic, payload_json, event_id=event_id)
            except Exception as e:
                estado_envio = f"MQTT_ERR({type(e).__name__})"

            with print_lock:
                indicador_freno = " [!] FRENADA" if accel_redondeada <= BRAKING_THRESHOLD else ""
                print(
                    f"[{estado_envio:<16}] [{self.rsu_actual.upper()}] {self.veh_id} | "
                    f"Tipo: {event_type:<14} | X: {pos_redondeada:>7.2f}m | "
                    f"V: {velocidad_kmh:>6.2f}km/h | A: {accel_redondeada:>5.2f}m/s²{indicador_freno}"
                )

            time.sleep(self.dt)


def guardar_ground_truth(hilos: list[VehiculoOBU], filename: str = "simulador_ground_truth.json"):
    totales = {
        "generated": 0,
        "sent_unique": 0,
        "mqtt_publishes": 0,
        "dropped": 0,
        "delayed": 0,
        "duplicated_extra": 0,
        "reordered": 0,
        "burst_buffered": 0,
    }
    por_vehiculo = {}
    todos_event_ids = []

    for h in hilos:
        st = h.canal_fallos.stats
        por_vehiculo[h.veh_id] = dict(st)
        for k in totales:
            totales[k] += st.get(k, 0)
        todos_event_ids.extend(h.canal_fallos.sent_event_ids)

    reporte = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "totals": totales,
        "per_vehicle": por_vehiculo,
        "sent_event_ids_count": len(todos_event_ids),
        "sent_event_ids": todos_event_ids,
    }
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(reporte, f, indent=2)
    print(f"\n📊 [GROUND TRUTH] Resumen guardado en '{filename}': {totales}")


def parse_args():
    parser = argparse.ArgumentParser(description="Simulador V2X OBU con MQTT, circuito cerrado e inyección de fallos")
    parser.add_argument("--num-vehicles", type=int, default=int(os.getenv("V2X_NUM_VEHICLES", "5")), help="Número de vehículos (>=5)")
    parser.add_argument("--dt", type=float, default=float(os.getenv("V2X_DT", "1.0")), help="Intervalo de envío (s)")
    parser.add_argument("--duration", type=float, default=0.0, help="Duración máxima en segundos (0 = infinito)")
    parser.add_argument("--rsu1-host", default=os.getenv("RSU1_HOST", "localhost"))
    parser.add_argument("--rsu1-port", type=int, default=int(os.getenv("RSU1_PORT", "1883")))
    parser.add_argument("--rsu2-host", default=os.getenv("RSU2_HOST", "localhost"))
    parser.add_argument("--rsu2-port", type=int, default=int(os.getenv("RSU2_PORT", "1884")))
    parser.add_argument("--drop-prob", type=float, default=0.0, help="Probabilidad de pérdida [0-1]")
    parser.add_argument("--dup-prob", type=float, default=0.0, help="Probabilidad de duplicación [0-1]")
    parser.add_argument("--delay-prob", type=float, default=0.0, help="Probabilidad de retardo [0-1]")
    parser.add_argument("--max-delay", type=float, default=2.0, help="Retardo máximo inyectado en segundos")
    parser.add_argument("--reorder-prob", type=float, default=0.0, help="Probabilidad de reordenación [0-1]")
    parser.add_argument("--burst-size", type=int, default=1, help="Tamaño de ráfaga (1 = sin ráfaga)")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    num_veh = max(5, args.num_vehicles)
    rsu_endpoints = {
        "rsu-1": (args.rsu1_host, args.rsu1_port),
        "rsu-2": (args.rsu2_host, args.rsu2_port),
    }

    nombres_vehiculos = [f"veh-{i:03d}" for i in range(1, num_veh + 1)]
    config_flota = []

    with open("vehiculos_config.txt", "w", encoding="utf-8") as f:
        f.write("=== Configuración de Flota V2X ===\n")
        for idx, veh_id in enumerate(nombres_vehiculos):
            if idx % 2 == 0:
                pos_inicial = round(random.uniform(50.0, 850.0), 2)
            else:
                pos_inicial = round(random.uniform(1050.0, 1850.0), 2)

            vel_crucero = round(random.uniform(60.0, 110.0), 2)
            accel = round(random.uniform(2.0, 4.0), 2)
            t_freno = round(random.uniform(4.0, 10.0), 2)
            config_flota.append((veh_id, pos_inicial, vel_crucero, accel, t_freno))
            f.write(f"{veh_id}: X0={pos_inicial}m, Vc={vel_crucero}km/h, A={accel}m/s2, Tfreno={t_freno}s\n")

    hilos = []
    for cfg in config_flota:
        canal_individual = CanalInyeccionFallos(
            drop_prob=args.drop_prob,
            dup_prob=args.dup_prob,
            delay_prob=args.delay_prob,
            max_delay=args.max_delay,
            reorder_prob=args.reorder_prob,
            burst_size=args.burst_size,
        )
        h = VehiculoOBU(*cfg, dt=args.dt, rsu_endpoints=rsu_endpoints, canal_fallos=canal_individual)
        hilos.append(h)
        h.start()

    inicio_sim = time.monotonic()
    try:
        while True:
            if args.duration > 0 and (time.monotonic() - inicio_sim) >= args.duration:
                break
            time.sleep(0.5)
    except KeyboardInterrupt:
        pass
    finally:
        stop_event.set()
        for h in hilos:
            h.join(timeout=1.5)
            h.cerrar()
        guardar_ground_truth(hilos)