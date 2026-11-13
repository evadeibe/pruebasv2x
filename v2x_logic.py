import collections
import os
import threading
import time

REQUIRED_FIELDS = {"event_id", "source_id", "event_type", "event_time", "location", "payload"}
VALID_EVENT_TYPES = {"POSITION", "SPEED", "SUDDEN_BRAKING", "HAZARD_DETECTED", "CRUISE", "STOPPED"}
BRAKING_THRESHOLD = -4.5  # Umbral unificado en todo el sistema (m/s^2)
CIRCUIT_LENGTH_M = 2000.0


def distancia_circular(pos1: float, pos2: float, longitud: float = CIRCUIT_LENGTH_M) -> float:
    """Calcula la distancia mínima entre dos puntos en una carretera circular cerrada."""
    diff = abs(float(pos1) - float(pos2)) % longitud
    return min(diff, longitud - diff)


def clasificar_telemetria(velocidad_kmh: float, aceleracion_ms2: float) -> str:
    """Clasifica el estado físico usando el umbral unificado BRAKING_THRESHOLD."""
    if aceleracion_ms2 <= BRAKING_THRESHOLD:
        return "SUDDEN_BRAKING"
    elif BRAKING_THRESHOLD < aceleracion_ms2 < -0.05:
        return "NORMAL_DECELERATION"
    elif abs(aceleracion_ms2) <= 0.05:
        return "STOPPED" if velocidad_kmh <= 0.5 else "CRUISE"
    else:
        return "ROUTINE_ACCELERATION"


def determinar_tipo_evento_simulador(velocidad_kmh: float, aceleracion_ms2: float, tick: int) -> str:
    """Genera al menos 3 tipos de eventos válidos en el simulador."""
    if aceleracion_ms2 <= BRAKING_THRESHOLD:
        return "SUDDEN_BRAKING"
    if tick % 2 == 0:
        return "POSITION"
    return "SPEED"


def validar_esquema_evento(evento: dict) -> tuple[bool, str]:
    """Valida campos obligatorios, pertenencia a VALID_EVENT_TYPES y tipos numéricos."""
    if not isinstance(evento, dict):
        return False, "El evento no es un diccionario JSON válido"

    faltantes = REQUIRED_FIELDS - set(evento.keys())
    if faltantes:
        return False, f"Faltan campos obligatorios: {sorted(list(faltantes))}"

    if not isinstance(evento.get("event_id"), str) or not evento["event_id"].strip():
        return False, "event_id debe ser un string no vacío"

    if evento.get("event_type") not in VALID_EVENT_TYPES:
        return False, f"event_type inválido: {evento.get('event_type')}"

    location = evento.get("location")
    if not isinstance(location, dict) or "posicion_x" not in location:
        return False, "location debe ser un dict con 'posicion_x'"

    payload = evento.get("payload")
    if not isinstance(payload, dict) or "velocidad" not in payload or "aceleracion" not in payload:
        return False, "Payload malformado: requiere 'velocidad' y 'aceleracion'"

    try:
        float(location["posicion_x"])
        float(payload["velocidad"])
        float(payload["aceleracion"])
    except (ValueError, TypeError):
        return False, "posicion_x, velocidad y aceleracion deben ser valores numéricos"

    return True, "OK"


class DeduplicadorEventos:
    """Control de idempotencia separando comprobación y registro, con limpieza periódica."""
    def __init__(self, ttl_seconds: float = 300.0, cleanup_interval: float = 15.0):
        self.ttl = ttl_seconds
        self.cleanup_interval = cleanup_interval
        self.vistos = {}
        self.ultimo_cleanup = 0.0
        self.lock = threading.Lock()

    def _limpiar_expirados(self, now: float):
        if now - self.ultimo_cleanup >= self.cleanup_interval:
            expirados = [eid for eid, ts in self.vistos.items() if now - ts > self.ttl]
            for eid in expirados:
                del self.vistos[eid]
            self.ultimo_cleanup = now

    def ya_visto(self, event_id: str, now: float = None) -> bool:
        now = time.time() if now is None else now
        with self.lock:
            self._limpiar_expirados(now)
            return event_id in self.vistos

    def registrar_visto(self, event_id: str, now: float = None):
        now = time.time() if now is None else now
        with self.lock:
            self.vistos[event_id] = now

    def es_duplicado(self, event_id: str, now: float = None) -> bool:
        now = time.time() if now is None else now
        with self.lock:
            self._limpiar_expirados(now)
            if event_id in self.vistos:
                return True
            self.vistos[event_id] = now
            return False


class ControladorCooldown:
    """Evita tormentas de alertas aplicando un tiempo de enfriamiento por clave."""
    def __init__(self, cooldown_seconds: float = 10.0):
        self.cooldown = cooldown_seconds
        self.ultimos_disparos = {}
        self.lock = threading.Lock()

    def puede_disparar(self, clave: str, now: float = None) -> bool:
        now = time.time() if now is None else now
        with self.lock:
            ultimo = self.ultimos_disparos.get(clave)
            if ultimo is not None and (now - ultimo) < self.cooldown:
                return False
            self.ultimos_disparos[clave] = now
            return True


class DetectorHazard:
    """
    Detecta peligro en la vía considerando ventana temporal (abs),
    distancia circular en el circuito de 2000m y cooldown espacial por proximidad
    (sin efectos de borde entre tramos fijos).
    """
    def __init__(self, time_window_s: float = 5.0, max_distance_m: float = 250.0, cooldown_s: float = 12.0):
        self.time_window_s = time_window_s
        self.max_distance_m = max_distance_m
        self.cooldown_s = cooldown_s
        self.frenadas = {}  # source_id -> (ts_unix, posicion_x)
        self.hazards_recientes = []  # lista de (ts_unix, posicion_x)
        self.lock = threading.Lock()

    def registrar_frenada(self, source_id: str, ts_unix: float, posicion_x: float) -> list[str]:
        pos_x = float(posicion_x) % CIRCUIT_LENGTH_M
        with self.lock:
            self.frenadas[source_id] = (ts_unix, pos_x)
            a_borrar = [
                vid for vid, (t, _) in self.frenadas.items()
                if abs(ts_unix - t) > self.time_window_s
            ]
            for vid in a_borrar:
                del self.frenadas[vid]

            # Limpiar historial de hazards cuyo cooldown ya expiró
            self.hazards_recientes = [
                (ht, hpos) for (ht, hpos) in self.hazards_recientes
                if abs(ts_unix - ht) < self.cooldown_s
            ]

            implicados = []
            for vid, (t, pos) in self.frenadas.items():
                if (
                    abs(ts_unix - t) <= self.time_window_s
                    and distancia_circular(pos_x, pos) <= self.max_distance_m
                ):
                    implicados.append(vid)

            implicados_unicos = sorted(list(set(implicados)))
            if len(implicados_unicos) >= 2:
                # Comprobar si ya se disparó un hazard cercano dentro del tiempo de cooldown
                for ht, hpos in self.hazards_recientes:
                    if (
                        abs(ts_unix - ht) < self.cooldown_s
                        and distancia_circular(pos_x, hpos) <= self.max_distance_m
                    ):
                        return []

                self.hazards_recientes.append((ts_unix, pos_x))
                for vid in implicados_unicos:
                    self.frenadas.pop(vid, None)
                return implicados_unicos
            return []


def obtener_uso_recursos() -> dict:
    """Obtiene memoria RSS (MB) y tiempo de CPU (s) del proceso sin librerías externas."""
    rss_mb = 0.0
    cpu_s = round(time.process_time(), 3)
    try:
        with open("/proc/self/status", "r", encoding="utf-8") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    kb = float(line.split()[1])
                    rss_mb = round(kb / 1024.0, 2)
                    break
    except Exception:
        pass
    return {"memory_rss_mb": rss_mb, "cpu_process_time_s": cpu_s, "pid": os.getpid()}


class MetricasV2X:
    """Contadores, latencia p95, tiempo de drenado post-recuperación y reset para experimentos."""
    def __init__(self, max_latencias: int = 5000):
        self.max_latencias = max_latencias
        self.lock = threading.Lock()
        self._init_state()

    def _init_state(self):
        self.contadores = {
            "received": 0,
            "sqs_redeliveries": 0,
            "accepted": 0,
            "processed": 0,
            "invalid": 0,
            "duplicates": 0,
            "forwarded_or_persisted": 0,
            "filtered_edge": 0,
            "dropped_buffer": 0,
            "alerts_triggered": 0,
            "alerts_local": 0,
            "alerts_sns_relay": 0,
        }
        self.latencias_ms = collections.deque(maxlen=self.max_latencias)
        self.fallo_inicio_ts = None
        self.recuperacion_inicio_ts = None
        self.last_outage_duration_s = 0.0
        self.last_drain_recovery_time_s = 0.0

    def reset(self):
        with self.lock:
            self._init_state()

    def inc(self, clave: str, cantidad: int = 1):
        with self.lock:
            self.contadores[clave] = self.contadores.get(clave, 0) + cantidad

    def registrar_latencia(self, latencia_ms: float):
        if latencia_ms >= 0:
            with self.lock:
                self.latencias_ms.append(latencia_ms)

    def marcar_fallo_red(self):
        with self.lock:
            if self.fallo_inicio_ts is None:
                self.fallo_inicio_ts = time.time()
                self.recuperacion_inicio_ts = None

    def marcar_recuperacion_red(self, buffer_restante: int = 0):
        """
        Calcula tanto la duración de la caída como el tiempo que tarda
        en vaciarse el buffer desde que vuelve la conexión (drain recovery time).
        """
        now = time.time()
        with self.lock:
            if self.fallo_inicio_ts is not None:
                self.last_outage_duration_s = round(now - self.fallo_inicio_ts, 3)
                self.fallo_inicio_ts = None
                self.recuperacion_inicio_ts = now

            if self.recuperacion_inicio_ts is not None and buffer_restante == 0:
                self.last_drain_recovery_time_s = round(now - self.recuperacion_inicio_ts, 3)
                self.recuperacion_inicio_ts = None

    def resumen(self, extra: dict = None) -> dict:
        with self.lock:
            lats = sorted(list(self.latencias_ms))
            if lats:
                idx_p95 = min(len(lats) - 1, int(len(lats) * 0.95))
                p95 = round(lats[idx_p95], 2)
                avg = round(sum(lats) / len(lats), 2)
            else:
                p95 = 0.0
                avg = 0.0
            data = {
                "counters": dict(self.contadores),
                "latency_samples": len(lats),
                "latency_avg_ms": avg,
                "latency_p95_ms": p95,
                "last_outage_duration_s": self.last_outage_duration_s,
                "last_recovery_time_s": self.last_drain_recovery_time_s,
                "resources": obtener_uso_recursos(),
            }
            if extra:
                data.update(extra)
            return data