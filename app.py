import json
import queue
import threading
from flask import Flask, render_template, jsonify, Response, request
from models import init_db, get_all_devices, get_recent_alerts, TEMPERATURE_THRESHOLD, HEARTBEAT_TIMEOUT
from scheduler import start_scheduler, set_sensor_enabled, set_sensor_temp_boost, get_sensor_states, set_alert_callback

app = Flask(__name__, static_folder='static', static_url_path='/static')

alert_queues = []
queue_lock = threading.Lock()

def broadcast_alert(alert):
    with queue_lock:
        for q in alert_queues[:]:
            try:
                q.put(alert)
            except:
                alert_queues.remove(q)

def event_stream():
    q = queue.Queue(maxsize=100)
    with queue_lock:
        alert_queues.append(q)

    try:
        yield 'data: ' + json.dumps({'type': 'connected', 'message': 'SSE 连接已建立'}) + '\n\n'

        while True:
            try:
                alert = q.get(timeout=30)
                yield 'data: ' + json.dumps({'type': 'alert', 'data': alert}) + '\n\n'
            except queue.Empty:
                yield 'data: ' + json.dumps({'type': 'ping'}) + '\n\n'
    except GeneratorExit:
        with queue_lock:
            if q in alert_queues:
                alert_queues.remove(q)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/devices')
def api_devices():
    devices = get_all_devices()
    sensor_states = get_sensor_states()
    for device in devices:
        state = sensor_states.get(device['id'], {})
        device['simulator_enabled'] = state.get('enabled', True)
        device['temp_boost'] = state.get('temp_boost', False)
    return jsonify(devices)

@app.route('/api/alerts')
def api_alerts():
    alerts = get_recent_alerts(20)
    return jsonify(alerts)

@app.route('/api/config')
def api_config():
    return jsonify({
        'temperature_threshold': TEMPERATURE_THRESHOLD,
        'heartbeat_timeout': HEARTBEAT_TIMEOUT
    })

@app.route('/api/sensor/<device_id>/disable', methods=['POST'])
def disable_sensor(device_id):
    success = set_sensor_enabled(device_id, False)
    return jsonify({'success': success, 'message': '传感器已禁用' if success else '传感器不存在'})

@app.route('/api/sensor/<device_id>/enable', methods=['POST'])
def enable_sensor(device_id):
    success = set_sensor_enabled(device_id, True)
    return jsonify({'success': success, 'message': '传感器已启用' if success else '传感器不存在'})

@app.route('/api/sensor/<device_id>/boost', methods=['POST'])
def boost_sensor_temp(device_id):
    data = request.get_json() or {}
    boost = data.get('boost', True)
    success = set_sensor_temp_boost(device_id, boost)
    return jsonify({'success': success, 'message': f'温度提升已{"启用" if boost else "禁用"}' if success else '传感器不存在'})

@app.route('/events')
def sse_events():
    return Response(
        event_stream(),
        mimetype='text/event-stream',
        headers={
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'Access-Control-Allow-Origin': '*'
        }
    )

if __name__ == '__main__':
    init_db()
    set_alert_callback(broadcast_alert)
    scheduler = start_scheduler()
    print('IoT 心跳监控网关已启动')
    print(f'温度阈值: {TEMPERATURE_THRESHOLD}°C')
    print(f'心跳超时: {HEARTBEAT_TIMEOUT}秒')
    print('访问 http://localhost:5000 查看监控面板')

    try:
        app.run(host='0.0.0.0', port=5000, debug=False, use_reloader=False)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
