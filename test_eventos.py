import json
import unittest
from unittest.mock import MagicMock, patch

from v2x_logic import (
    ControladorCooldown,
    DeduplicadorEventos,
    DetectorHazard,
    MetricasV2X,
    clasificar_telemetria,
    determinar_tipo_evento_simulador,
    distancia_circular,
    validar_esquema_evento,
)
from productor_v2x import CanalInyeccionFallos
import consumidor_v2x
import rsu


class TestV2XCompleteSuite(unittest.TestCase):

    def test_clasificacion_estados_cinematicos(self):
        self.assertEqual(clasificar_telemetria(80.0, -6.0), "SUDDEN_BRAKING")
        self.assertEqual(clasificar_telemetria(80.0, -4.5), "SUDDEN_BRAKING")
        self.assertEqual(clasificar_telemetria(60.0, -2.0), "NORMAL_DECELERATION")
        self.assertEqual(clasificar_telemetria(90.0, 0.0), "CRUISE")
        self.assertEqual(clasificar_telemetria(0.0, 0.0), "STOPPED")
        self.assertEqual(clasificar_telemetria(40.0, 2.5), "ROUTINE_ACCELERATION")

    def test_determinar_tipo_evento_simulador(self):
        self.assertEqual(determinar_tipo_evento_simulador(50.0, -5.0, tick=1), "SUDDEN_BRAKING")
        self.assertEqual(determinar_tipo_evento_simulador(80.0, 0.0, tick=2), "POSITION")
        self.assertEqual(determinar_tipo_evento_simulador(80.0, 1.2, tick=3), "SPEED")

    def test_validacion_esquema_y_tipos_estrictos(self):
        base = {
            "event_id": "evt-1234",
            "source_id": "veh-001",
            "event_type": "POSITION",
            "event_time": "2026-11-13T10:00:00Z",
            "location": {"posicion_x": 200.0},
            "payload": {"velocidad": 80.0, "aceleracion": 0.0},
        }
        self.assertTrue(validar_esquema_evento(base)[0])
        self.assertFalse(validar_esquema_evento(dict(base, event_type="UNKNOWN_TYPE"))[0])
        self.assertFalse(validar_esquema_evento(dict(base, payload={"velocidad": "abc", "aceleracion": 0.0}))[0])
        self.assertFalse(validar_esquema_evento(dict(base, location="segment-A12"))[0])

    def test_distancia_circular_y_hazard_en_cierre_de_circuito(self):
        self.assertAlmostEqual(distancia_circular(1990.0, 10.0), 20.0)
        detector = DetectorHazard(time_window_s=5.0, max_distance_m=250.0, cooldown_s=10.0)
        self.assertEqual(detector.registrar_frenada("veh-001", ts_unix=1000.0, posicion_x=1990.0), [])
        # veh-002 frena en 10m (a 20m reales en el circuito circular de 2000m) -> DEBE detectar hazard
        self.assertEqual(detector.registrar_frenada("veh-002", ts_unix=1001.0, posicion_x=10.0), ["veh-001", "veh-002"])
        # Cooldown espacial continuo impide otro hazard inmediato a pocos metros
        detector.registrar_frenada("veh-001", ts_unix=1002.0, posicion_x=1995.0)
        self.assertEqual(detector.registrar_frenada("veh-002", ts_unix=1002.5, posicion_x=15.0), [])

    def test_canal_inyeccion_fallos_aislado_y_flush(self):
        mock_mqtt = MagicMock()
        canal_dup = CanalInyeccionFallos(dup_prob=1.0)
        estado = canal_dup.enviar(mock_mqtt, "v2x/rsu-1/telemetry", '{"id":1}', event_id="evt-1")
        self.assertEqual(estado, "DUPLICATED")
        self.assertEqual(mock_mqtt.publish.call_count, 2)
        self.assertEqual(canal_dup.stats["sent_unique"], 1)
        self.assertEqual(canal_dup.stats["duplicated_extra"], 1)

    def test_procesar_evento_reintenta_si_falla_dynamodb(self):
        consumidor_v2x.deduplicador = DeduplicadorEventos()
        evento = {
            "event_id": "evt-fail-db-01",
            "source_id": "veh-001",
            "event_type": "SUDDEN_BRAKING",
            "event_time": "2026-11-13T10:00:00Z",
            "location": {"posicion_x": 300.0},
            "payload": {"velocidad": 50.0, "aceleracion": -6.0},
        }
        with patch.object(consumidor_v2x, "guardar_evento_dynamodb", side_effect=RuntimeError("Dynamo Down")):
            self.assertFalse(consumidor_v2x.procesar_evento(evento))
            self.assertFalse(consumidor_v2x.deduplicador.ya_visto("evt-fail-db-01"))

        with patch.object(consumidor_v2x, "guardar_evento_dynamodb", return_value="STORED"), \
             patch.object(consumidor_v2x, "enviar_cloudwatch"), \
             patch.object(consumidor_v2x, "publicar_sns"):
            self.assertTrue(consumidor_v2x.procesar_evento(evento))
            self.assertTrue(consumidor_v2x.deduplicador.ya_visto("evt-fail-db-01"))

    def test_rsu_muestreo_rutinario_y_prioridad_frenada(self):
        rsu.sqs_buffer.clear()
        rsu.deduplicador_mqtt = DeduplicadorEventos()
        rsu.cooldown_alerta_local = ControladorCooldown(cooldown_seconds=10.0)
        rsu.metricas = MetricasV2X()
        rsu.contador_rutinario_veh.clear()
        rsu.mqtt_client = MagicMock()

        # 3 eventos rutinarios (muestreo 1/3 -> 2 filtrados, 1 encolado)
        for i in range(1, 4):
            ev = {
                "event_id": f"evt-rut-{i}",
                "source_id": "veh-001",
                "event_type": "POSITION",
                "event_time": "2026-11-13T10:00:00Z",
                "location": {"posicion_x": 100.0 * i},
                "payload": {"velocidad": 70.0, "aceleracion": 0.5},
            }
            msg = MagicMock()
            msg.payload = json.dumps(ev).encode("utf-8")
            rsu.on_mqtt_message(None, None, msg)

        self.assertEqual(len(rsu.sqs_buffer), 1)
        self.assertEqual(rsu.metricas.contadores["filtered_edge"], 2)

        # 2 eventos de frenada seguidos del mismo vehículo -> ambos van a SQS, pero solo 1 alerta local por cooldown
        for i in range(1, 3):
            ev_freno = {
                "event_id": f"evt-brk-{i}",
                "source_id": "veh-001",
                "event_type": "SUDDEN_BRAKING",
                "event_time": "2026-11-13T10:00:05Z",
                "location": {"posicion_x": 400.0},
                "payload": {"velocidad": 40.0, "aceleracion": -6.0},
            }
            msg = MagicMock()
            msg.payload = json.dumps(ev_freno).encode("utf-8")
            rsu.on_mqtt_message(None, None, msg)

        self.assertEqual(len(rsu.sqs_buffer), 3)
        self.assertEqual(rsu.metricas.contadores["alerts_local"], 1)

    def test_rsu_sns_deduplicacion_solo_en_notification(self):
        rsu.deduplicador_sns = DeduplicadorEventos()
        client = rsu.app.test_client()

        # 1. SubscriptionConfirmation falla en el primer intento (500) y se reintenta con el mismo MessageId -> éxito (200)
        sub_payload = {"MessageId": "sns-sub-1", "SubscribeURL": "http://example.com/confirm"}
        with patch("rsu.requests.get", side_effect=rsu.requests.RequestException("Timeout")):
            resp1 = client.post(
                "/sns-notifications",
                data=json.dumps(sub_payload),
                headers={"x-amz-sns-message-type": "SubscriptionConfirmation"},
            )
            self.assertEqual(resp1.status_code, 500)

        with patch("rsu.requests.get") as mock_get:
            resp2 = client.post(
                "/sns-notifications",
                data=json.dumps(sub_payload),
                headers={"x-amz-sns-message-type": "SubscriptionConfirmation"},
            )
            self.assertEqual(resp2.status_code, 200)
            self.assertEqual(mock_get.call_count, 1)

        # 2. Notification con el mismo MessageId dos veces -> la segunda se ignora como duplicado
        rsu.mqtt_client = MagicMock()
        notif_payload = {"MessageId": "sns-notif-99", "Message": "Alerta hielo"}
        client.post("/sns-notifications", data=json.dumps(notif_payload), headers={"x-amz-sns-message-type": "Notification"})
        client.post("/sns-notifications", data=json.dumps(notif_payload), headers={"x-amz-sns-message-type": "Notification"})
        self.assertEqual(rsu.mqtt_client.publish.call_count, 1)

    def test_rsu_buffer_orden_y_reencolado_ante_fallo_sqs(self):
        rsu.sqs_buffer.clear()
        rsu.encolar_para_sqs({"event_id": "evt-1"})
        rsu.encolar_para_sqs({"event_id": "evt-2"})

        rsu.sqs_client = MagicMock()
        rsu.queue_url = "https://sqs.us-east-1.amazonaws.com/123456789012/v2x-events"
        rsu.sqs_client.send_message_batch.side_effect = RuntimeError("Network unreachable")

        ok = rsu.enviar_lote_sqs_una_vez()
        self.assertFalse(ok)
        # Los mensajes deben haberse reencolado exactamente en su orden original [evt-1, evt-2]
        self.assertEqual([e["event_id"] for e in rsu.sqs_buffer], ["evt-1", "evt-2"])


if __name__ == "__main__":
    unittest.main()