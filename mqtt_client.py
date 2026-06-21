import os
import json
import logging
import threading
import time
from datetime import datetime
from typing import Optional, Callable

import paho.mqtt.client as mqtt

from influx_store import write_sensor_data, write_alert, init_influx

logger = logging.getLogger(__name__)

MQTT_BROKER_HOST = os.environ.get('MQTT_BROKER_HOST', 'localhost')
MQTT_BROKER_PORT = int(os.environ.get('MQTT_BROKER_PORT', '1883'))
MQTT_USERNAME = os.environ.get('MQTT_USERNAME', '')
MQTT_PASSWORD = os.environ.get('MQTT_PASSWORD', '')
MQTT_TOPIC = os.environ.get('MQTT_TOPIC', '/sensor/+/data')
MQTT_CLIENT_ID = os.environ.get('MQTT_CLIENT_ID', 'iot_gateway')
MQTT_KEEPALIVE = int(os.environ.get('MQTT_KEEPALIVE', '60'))

TEMPERATURE_THRESHOLD = float(os.environ.get('TEMPERATURE_THRESHOLD', '35.0'))

_mqtt_client: Optional[mqtt.Client] = None
_mqtt_thread: Optional[threading.Thread] = None
_running = False
_on_message_callback: Optional[Callable[[dict], None]] = None
_active_temp_alerts: set = set()
_last_heartbeat: dict = {}
_heartbeat_lock = threading.Lock()
_alert_lock = threading.Lock()


def _extract_device_id(topic: str) -> Optional[str]:
    parts = topic.strip('/').split('/')
    if len(parts) >= 2 and parts[0] == 'sensor':
        return parts[1]
    return None


def _validate_payload(payload: dict) -> bool:
    if not isinstance(payload, dict):
        return False
    if 'temperature' not in payload or 'humidity' not in payload:
        return False
    try:
        float(payload['temperature'])
        float(payload['humidity'])
    except (TypeError, ValueError):
        return False
    return True


def _check_temperature_alert(device_id: str, temperature: float, device_name: str = None) -> Optional[dict]:
    global _active_temp_alerts
    if temperature > TEMPERATURE_THRESHOLD:
        with _alert_lock:
            if device_id not in _active_temp_alerts:
                _active_temp_alerts.add(device_id)
                name = device_name or device_id
                alert_msg = f'{name} 温度过高: {temperature:.1f}°C (阈值: {TEMPERATURE_THRESHOLD}°C)'
                alert = {
                    'device_id': device_id,
                    'alert_type': 'temperature',
                    'message': alert_msg,
                    'timestamp': datetime.now().isoformat()
                }
                try:
                    write_alert(alert['device_id'], alert['alert_type'], alert['message'])
                except Exception as e:
                    logger.error('Failed to write temperature alert: %s', e)
                return alert
    else:
        with _alert_lock:
            if device_id in _active_temp_alerts:
                _active_temp_alerts.discard(device_id)
                logger.info('Temperature alert cleared for %s', device_id)
    return None


def _on_connect(client, userdata, flags, rc):
    if rc == 0:
        logger.info('Connected to MQTT broker at %s:%d', MQTT_BROKER_HOST, MQTT_BROKER_PORT)
        client.subscribe(MQTT_TOPIC, qos=1)
        logger.info('Subscribed to MQTT topic: %s', MQTT_TOPIC)
    else:
        logger.error('Failed to connect to MQTT broker, return code: %d', rc)


def _on_disconnect(client, userdata, rc):
    if rc != 0:
        logger.warning('Unexpected MQTT disconnect (rc=%d), will reconnect...', rc)
    else:
        logger.info('MQTT client disconnected gracefully')


def _on_message(client, userdata, msg):
    global _last_heartbeat
    try:
        topic = msg.topic
        device_id = _extract_device_id(topic)
        if device_id is None:
            logger.warning('Could not extract device_id from topic: %s', topic)
            return

        payload_str = msg.payload.decode('utf-8', errors='replace')
        try:
            payload = json.loads(payload_str)
        except json.JSONDecodeError as e:
            logger.error('Invalid JSON payload on topic %s: %s', topic, e)
            return

        if not _validate_payload(payload):
            logger.error('Invalid payload structure on topic %s: %s', topic, payload_str)
            return

        temperature = float(payload['temperature'])
        humidity = float(payload['humidity'])
        device_name = payload.get('device_name', device_id)

        timestamp = None
        if 'timestamp' in payload:
            try:
                if isinstance(payload['timestamp'], (int, float)):
                    timestamp = datetime.fromtimestamp(float(payload['timestamp']))
                else:
                    timestamp = datetime.fromisoformat(str(payload['timestamp']))
            except (ValueError, TypeError):
                timestamp = None

        if timestamp is None:
            timestamp = datetime.utcnow()

        try:
            write_sensor_data(device_id, temperature, humidity, timestamp)
        except Exception as e:
            logger.error('Failed to write sensor data for %s: %s', device_id, e)
            return

        with _heartbeat_lock:
            _last_heartbeat[device_id] = datetime.now()

        alert = _check_temperature_alert(device_id, temperature, device_name)

        if _on_message_callback is not None:
            try:
                message_data = {
                    'device_id': device_id,
                    'temperature': temperature,
                    'humidity': humidity,
                    'timestamp': timestamp.isoformat(),
                    'alert': alert
                }
                _on_message_callback(message_data)
            except Exception as e:
                logger.error('Error in on_message callback: %s', e, exc_info=True)

        logger.debug('Processed MQTT message from %s: temp=%.2f, humidity=%.2f',
                     device_id, temperature, humidity)

    except Exception as e:
        logger.error('Error processing MQTT message: %s', e, exc_info=True)


def _mqtt_loop_forever():
    global _running, _mqtt_client
    while _running:
        try:
            logger.info('Starting MQTT client loop...')
            _mqtt_client.loop_forever(retry_first_connection=True)
        except Exception as e:
            logger.error('MQTT loop error: %s', e, exc_info=True)
        if _running:
            logger.info('MQTT loop exited, waiting 5s before restart...')
            time.sleep(5)


def set_message_callback(callback: Optional[Callable[[dict], None]]) -> None:
    global _on_message_callback
    _on_message_callback = callback


def get_last_heartbeat() -> dict:
    with _heartbeat_lock:
        return dict(_last_heartbeat)


def start_mqtt_gateway(callback: Optional[Callable[[dict], None]] = None) -> bool:
    global _mqtt_client, _mqtt_thread, _running

    if _running and _mqtt_client is not None:
        logger.warning('MQTT gateway already running')
        return True

    try:
        init_influx()
    except Exception as e:
        logger.warning('InfluxDB init failed (will retry on write): %s', e)

    set_message_callback(callback)

    client = mqtt.Client(
        client_id=MQTT_CLIENT_ID,
        clean_session=True,
        protocol=mqtt.MQTTv311
    )

    if MQTT_USERNAME:
        client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

    client.on_connect = _on_connect
    client.on_disconnect = _on_disconnect
    client.on_message = _on_message
    client.reconnect_delay_set(min_delay=1, max_delay=30)

    try:
        logger.info('Connecting to MQTT broker at %s:%d...', MQTT_BROKER_HOST, MQTT_BROKER_PORT)
        client.connect(
            host=MQTT_BROKER_HOST,
            port=MQTT_BROKER_PORT,
            keepalive=MQTT_KEEPALIVE
        )
    except Exception as e:
        logger.error('Failed to connect to MQTT broker: %s', e)
        return False

    _mqtt_client = client
    _running = True

    _mqtt_thread = threading.Thread(
        target=_mqtt_loop_forever,
        name='mqtt-gateway',
        daemon=True
    )
    _mqtt_thread.start()

    logger.info('MQTT gateway started successfully')
    return True


def stop_mqtt_gateway() -> None:
    global _mqtt_client, _mqtt_thread, _running

    if not _running:
        return

    logger.info('Stopping MQTT gateway...')
    _running = False

    if _mqtt_client is not None:
        try:
            _mqtt_client.disconnect()
            _mqtt_client.loop_stop()
        except Exception as e:
            logger.warning('Error disconnecting MQTT client: %s', e)
        _mqtt_client = None

    if _mqtt_thread is not None:
        _mqtt_thread.join(timeout=5)
        _mqtt_thread = None

    logger.info('MQTT gateway stopped')


def is_running() -> bool:
    return _running
