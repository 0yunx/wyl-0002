import random
import os
import logging
from apscheduler.schedulers.background import BackgroundScheduler
from models import insert_sensor_data, check_heartbeat, get_all_devices
from message_bus import publish_alert
from mqtt_client import start_mqtt_gateway, stop_mqtt_gateway, is_running as is_mqtt_running

logger = logging.getLogger(__name__)

sensor_states = {}
_scheduler_instance = None
_mqtt_started = False


def init_sensor_states():
    global sensor_states
    for i in range(1, 11):
        device_id = f'sensor_{i:02d}'
        sensor_states[device_id] = {
            'enabled': True,
            'base_temp': random.uniform(20.0, 28.0),
            'base_humidity': random.uniform(40.0, 70.0),
            'temp_boost': False
        }
    logger.info('Initialized %d sensor states', len(sensor_states))


def simulate_sensor_data():
    global sensor_states
    devices = get_all_devices()
    count = 0

    for device in devices:
        device_id = device['id']
        state = sensor_states.get(device_id)

        if not state or not state['enabled']:
            continue

        try:
            temp_variation = random.uniform(-1.0, 1.0)
            humidity_variation = random.uniform(-2.0, 2.0)

            temperature = state['base_temp'] + temp_variation
            humidity = max(0.0, min(100.0, state['base_humidity'] + humidity_variation))

            if state['temp_boost']:
                temperature = random.uniform(36.0, 42.0)

            insert_sensor_data(device_id, round(temperature, 2), round(humidity, 2))
            count += 1
        except Exception as e:
            logger.error('Failed to simulate data for %s: %s', device_id, e, exc_info=True)

    if count > 0:
        logger.debug('Simulated data for %d sensors', count)


def heartbeat_check_job():
    try:
        alerts = check_heartbeat()
        if alerts:
            for alert in alerts:
                try:
                    publish_alert(alert)
                except Exception as e:
                    logger.error('Failed to publish alert %s: %s', alert, e, exc_info=True)
    except Exception as e:
        logger.error('Heartbeat check job failed: %s', e, exc_info=True)


def set_sensor_enabled(device_id, enabled):
    global sensor_states
    if device_id in sensor_states:
        sensor_states[device_id]['enabled'] = enabled
        logger.info('Sensor %s simulator %s', device_id, 'enabled' if enabled else 'disabled')
        return True
    logger.warning('Attempted to toggle non-existent sensor: %s', device_id)
    return False


def set_sensor_temp_boost(device_id, boost):
    global sensor_states
    if device_id in sensor_states:
        sensor_states[device_id]['temp_boost'] = boost
        logger.info('Sensor %s temperature boost %s', device_id, 'enabled' if boost else 'disabled')
        return True
    logger.warning('Attempted to boost non-existent sensor: %s', device_id)
    return False


def get_sensor_states():
    global sensor_states
    return sensor_states.copy()


def should_start_scheduler() -> bool:
    mode = os.environ.get('SCHEDULER_MODE', 'auto').lower()
    if mode == 'always':
        logger.info('Scheduler mode: always (starting)')
        return True
    if mode == 'never':
        logger.info('Scheduler mode: never (skipping)')
        return False
    if mode == 'auto':
        is_gunicorn = 'gunicorn' in os.environ.get('SERVER_SOFTWARE', '').lower()
        if is_gunicorn:
            worker_id = os.environ.get('GUNICORN_WORKER_ID', '')
            should_start = worker_id == '0' or worker_id == ''
            logger.info('Scheduler mode: auto (gunicorn worker %s) %s',
                       worker_id, 'starting' if should_start else 'skipping')
            return should_start
        logger.info('Scheduler mode: auto (development) starting')
        return True
    return False


def start_scheduler():
    global _scheduler_instance, _mqtt_started
    if _scheduler_instance is not None:
        logger.warning('Scheduler already running, returning existing instance')
        return _scheduler_instance

    if not should_start_scheduler():
        logger.info('This worker will not start the scheduler (SCHEDULER_MODE=%s)',
                   os.environ.get('SCHEDULER_MODE', 'auto'))
        return None

    use_mqtt = os.environ.get('USE_MQTT', 'true').lower() == 'true'
    if use_mqtt and not _mqtt_started:
        logger.info('Starting MQTT gateway...')
        try:
            mqtt_ok = start_mqtt_gateway(callback=on_mqtt_message)
            if mqtt_ok:
                _mqtt_started = True
                logger.info('MQTT gateway started successfully')
            else:
                logger.warning('MQTT gateway failed to start, falling back to simulator')
        except Exception as e:
            logger.error('Failed to start MQTT gateway: %s', e, exc_info=True)

    init_sensor_states()

    scheduler = BackgroundScheduler(logger=logger)

    if not use_mqtt or not _mqtt_started:
        scheduler.add_job(
            simulate_sensor_data,
            'interval',
            seconds=3,
            id='simulate_sensor_data',
            replace_existing=True,
            misfire_grace_time=10,
            coalesce=True
        )
        logger.info('Added job: simulate_sensor_data (every 3s)')

    scheduler.add_job(
        heartbeat_check_job,
        'interval',
        seconds=1,
        id='heartbeat_check',
        replace_existing=True,
        misfire_grace_time=5,
        coalesce=True
    )
    logger.info('Added job: heartbeat_check (every 1s)')

    scheduler.start()
    _scheduler_instance = scheduler
    logger.info('Scheduler started successfully')
    return scheduler


def stop_scheduler():
    global _scheduler_instance, _mqtt_started
    if _scheduler_instance is not None:
        logger.info('Shutting down scheduler...')
        _scheduler_instance.shutdown(wait=False)
        _scheduler_instance = None
        logger.info('Scheduler stopped')

    if _mqtt_started:
        logger.info('Stopping MQTT gateway...')
        try:
            stop_mqtt_gateway()
        except Exception as e:
            logger.warning('Error stopping MQTT gateway: %s', e)
        _mqtt_started = False
        logger.info('MQTT gateway stopped')


def on_mqtt_message(message_data: dict) -> None:
    alert = message_data.get('alert')
    if alert:
        try:
            publish_alert(alert)
        except Exception as e:
            logger.error('Failed to publish alert from MQTT: %s', e, exc_info=True)
