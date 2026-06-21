import random
import os
from apscheduler.schedulers.background import BackgroundScheduler
from models import insert_sensor_data, check_heartbeat, get_all_devices
from message_bus import publish_alert

sensor_states = {}
_scheduler_instance = None


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


def simulate_sensor_data():
    global sensor_states
    devices = get_all_devices()

    for device in devices:
        device_id = device['id']
        state = sensor_states.get(device_id)

        if not state or not state['enabled']:
            continue

        temp_variation = random.uniform(-1.0, 1.0)
        humidity_variation = random.uniform(-2.0, 2.0)

        temperature = state['base_temp'] + temp_variation
        humidity = max(0.0, min(100.0, state['base_humidity'] + humidity_variation))

        if state['temp_boost']:
            temperature = random.uniform(36.0, 42.0)

        insert_sensor_data(device_id, round(temperature, 2), round(humidity, 2))


def heartbeat_check_job():
    alerts = check_heartbeat()
    if alerts:
        for alert in alerts:
            publish_alert(alert)


def set_sensor_enabled(device_id, enabled):
    global sensor_states
    if device_id in sensor_states:
        sensor_states[device_id]['enabled'] = enabled
        return True
    return False


def set_sensor_temp_boost(device_id, boost):
    global sensor_states
    if device_id in sensor_states:
        sensor_states[device_id]['temp_boost'] = boost
        return True
    return False


def get_sensor_states():
    global sensor_states
    return sensor_states.copy()


def should_start_scheduler() -> bool:
    mode = os.environ.get('SCHEDULER_MODE', 'auto').lower()
    if mode == 'always':
        return True
    if mode == 'never':
        return False
    if mode == 'auto':
        is_gunicorn = 'gunicorn' in os.environ.get('SERVER_SOFTWARE', '').lower()
        if is_gunicorn:
            worker_id = os.environ.get('GUNICORN_WORKER_ID', '')
            return worker_id == '0' or worker_id == ''
        return True
    return False


def start_scheduler():
    global _scheduler_instance
    if _scheduler_instance is not None:
        return _scheduler_instance

    if not should_start_scheduler():
        print('[INFO] 此 worker 不启动调度器 (SCHEDULER_MODE={})'.format(
            os.environ.get('SCHEDULER_MODE', 'auto')))
        return None

    init_sensor_states()

    scheduler = BackgroundScheduler()

    scheduler.add_job(
        simulate_sensor_data,
        'interval',
        seconds=3,
        id='simulate_sensor_data',
        replace_existing=True
    )

    scheduler.add_job(
        heartbeat_check_job,
        'interval',
        seconds=1,
        id='heartbeat_check',
        replace_existing=True
    )

    scheduler.start()
    _scheduler_instance = scheduler
    print('[INFO] 调度器已启动')
    return scheduler


def stop_scheduler():
    global _scheduler_instance
    if _scheduler_instance is not None:
        _scheduler_instance.shutdown()
        _scheduler_instance = None
        print('[INFO] 调度器已停止')
