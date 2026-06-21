import random
import time
from apscheduler.schedulers.background import BackgroundScheduler
from models import insert_sensor_data, check_heartbeat, get_all_devices

sensor_states = {}
alert_callback = None

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
    global alert_callback
    alerts = check_heartbeat()
    if alerts and alert_callback:
        for alert in alerts:
            alert_callback(alert)

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

def set_alert_callback(callback):
    global alert_callback
    alert_callback = callback

def start_scheduler():
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
    return scheduler
