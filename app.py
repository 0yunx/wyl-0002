import json
import queue
import logging
import os
from flask import Flask, render_template, jsonify, Response, request, g, send_from_directory
from models import init_db, get_all_devices, get_recent_alerts, TEMPERATURE_THRESHOLD, HEARTBEAT_TIMEOUT
from scheduler import start_scheduler, stop_scheduler, set_sensor_enabled, set_sensor_temp_boost, get_sensor_states
from message_bus import subscribe_alerts, unsubscribe_alerts
from auth import init_app as init_auth, is_auth_required
from influx_store import query_recent_sensor_data

logger = logging.getLogger(__name__)

GRAFANA_URL = os.environ.get('GRAFANA_URL', 'http://localhost:3000')
GRAFANA_DASHBOARD_UID = os.environ.get('GRAFANA_DASHBOARD_UID', 'iot-monitor-dashboard')


def setup_logging():
    log_level = os.environ.get('LOG_LEVEL', 'INFO').upper()
    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler()
        ]
    )


def create_app() -> Flask:
    setup_logging()

    app = Flask(__name__, static_folder='static', static_url_path='/static')

    init_auth(app)

    init_db()
    start_scheduler()

    bus_type = 'Redis' if os.environ.get('USE_REDIS', 'false').lower() == 'true' else '内存'
    auth_status = '启用' if is_auth_required() else '禁用'
    mqtt_status = '启用' if os.environ.get('USE_MQTT', 'true').lower() == 'true' else '禁用'
    use_gunicorn = 'gunicorn' in os.environ.get('SERVER_SOFTWARE', '').lower()
    workers = int(os.environ.get('GUNICORN_WORKERS', '1'))
    logger.info('=' * 60)
    logger.info('IoT MQTT 设备网关已启动')
    logger.info('温度阈值: %s°C', TEMPERATURE_THRESHOLD)
    logger.info('心跳超时: %s秒', HEARTBEAT_TIMEOUT)
    logger.info('消息总线: %s', bus_type)
    logger.info('MQTT 网关: %s', mqtt_status)
    logger.info('时序存储: InfluxDB (%s) → 传感器数据 + 告警', os.environ.get('INFLUXDB_URL', 'http://localhost:8086'))
    logger.info('元数据存储: SQLite (%s) → 仅设备表 (id/name/status)', 'iot_monitor.db')
    logger.info('⚠️  双存储写路径: 每次上报 → InfluxDB 写时序 + SQLite 更新设备心跳')
    if use_gunicorn:
        logger.info('Gunicorn workers: %d (建议保持 1 避免 MQTT 重复订阅 & SQLite 并发写放大)', workers)
        if workers > 1:
            logger.warning('⚠️  workers=%d > 1: 多 worker 将重复启动 MQTT/MQTT会重复订阅造成消息风暴，SQLite 多进程写会放大锁竞争')
    logger.info('API 认证: %s', auth_status)
    logger.info('访问 http://localhost:5000 查看监控入口')
    logger.info('Grafana 仪表盘: %s/d/%s', GRAFANA_URL, GRAFANA_DASHBOARD_UID)
    logger.info('=' * 60)

    def event_stream(client_ip, client_id):
        q = queue.Queue(maxsize=100)

        def on_alert(alert):
            try:
                q.put(alert, block=False)
            except queue.Full:
                logger.warning('SSE client %s queue full, dropping alert', client_id)
            except Exception as e:
                logger.error('Error putting alert to SSE client %s queue: %s', client_id, e, exc_info=True)

        subscribe_alerts(on_alert)
        logger.info('SSE client connected: ip=%s, client_id=%s', client_ip, client_id)
        connected_clients = int(os.environ.get('SSE_CLIENTS', '0')) + 1
        os.environ['SSE_CLIENTS'] = str(connected_clients)

        try:
            yield 'data: ' + json.dumps({'type': 'connected', 'message': 'SSE 连接已建立'}, ensure_ascii=False) + '\n\n'

            while True:
                try:
                    alert = q.get(timeout=30)
                    yield 'data: ' + json.dumps({'type': 'alert', 'data': alert}, ensure_ascii=False) + '\n\n'
                except queue.Empty:
                    yield 'data: ' + json.dumps({'type': 'ping'}, ensure_ascii=False) + '\n\n'
        except GeneratorExit:
            logger.info('SSE client disconnected: ip=%s, client_id=%s', client_ip, client_id)
        except Exception as e:
            logger.error('SSE stream error for client %s: %s', client_id, e, exc_info=True)
        finally:
            unsubscribe_alerts(on_alert)
            connected_clients = max(0, int(os.environ.get('SSE_CLIENTS', '0')) - 1)
            os.environ['SSE_CLIENTS'] = str(connected_clients)
            logger.debug('SSE client cleaned up, remaining: %d', connected_clients)

    @app.route('/')
    def index():
        logger.debug('Page request from %s', request.remote_addr)
        return render_template('index.html')

    @app.route('/api/devices')
    def api_devices():
        try:
            devices = get_all_devices()
            sensor_states = get_sensor_states()
            for device in devices:
                state = sensor_states.get(device['id'], {})
                device['simulator_enabled'] = state.get('enabled', True)
                device['temp_boost'] = state.get('temp_boost', False)
            logger.debug('Devices API request from %s, returned %d devices', request.remote_addr, len(devices))
            return jsonify(devices)
        except Exception as e:
            logger.error('Devices API error: %s', e, exc_info=True)
            return jsonify({'error': 'Internal server error', 'message': str(e)}), 500

    @app.route('/api/alerts')
    def api_alerts():
        try:
            alerts = get_recent_alerts(20)
            logger.debug('Alerts API request from %s, returned %d alerts', request.remote_addr, len(alerts))
            return jsonify(alerts)
        except Exception as e:
            logger.error('Alerts API error: %s', e, exc_info=True)
            return jsonify({'error': 'Internal server error', 'message': str(e)}), 500

    @app.route('/api/config')
    def api_config():
        bus_type = 'redis' if os.environ.get('USE_REDIS', 'false').lower() == 'true' else 'memory'
        mqtt_enabled = os.environ.get('USE_MQTT', 'true').lower() == 'true'
        return jsonify({
            'temperature_threshold': TEMPERATURE_THRESHOLD,
            'heartbeat_timeout': HEARTBEAT_TIMEOUT,
            'message_bus': bus_type,
            'auth_required': is_auth_required(),
            'mqtt_enabled': mqtt_enabled,
            'mqtt_broker': os.environ.get('MQTT_BROKER_HOST', 'localhost'),
            'mqtt_port': int(os.environ.get('MQTT_BROKER_PORT', '1883')),
            'mqtt_topic': os.environ.get('MQTT_TOPIC', '/sensor/+/data'),
            'influxdb_url': os.environ.get('INFLUXDB_URL', 'http://localhost:8086'),
            'influxdb_bucket': os.environ.get('INFLUXDB_BUCKET', 'iot_monitor'),
            'grafana_url': GRAFANA_URL,
            'grafana_dashboard_uid': GRAFANA_DASHBOARD_UID
        })

    @app.route('/api/sensor/<device_id>/history')
    def api_sensor_history(device_id):
        try:
            minutes = int(request.args.get('minutes', 5))
            data = query_recent_sensor_data(device_id=device_id, minutes=minutes)
            return jsonify(data)
        except Exception as e:
            logger.error('Sensor history API error: %s', e, exc_info=True)
            return jsonify({'error': 'Internal server error', 'message': str(e)}), 500

    @app.route('/api/grafana/dashboard')
    def api_grafana_dashboard():
        dashboard_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'grafana_dash.json')
        if os.path.exists(dashboard_path):
            return send_from_directory(
                os.path.dirname(dashboard_path),
                'grafana_dash.json',
                mimetype='application/json',
                as_attachment=True,
                download_name='grafana_dash.json'
            )
        return jsonify({'error': 'Dashboard file not found'}), 404

    @app.route('/grafana')
    def grafana_redirect():
        dashboard_url = f'{GRAFANA_URL}/d/{GRAFANA_DASHBOARD_UID}'
        return render_template('grafana_redirect.html', dashboard_url=dashboard_url)

    @app.route('/api/sensor/<device_id>/disable', methods=['POST'])
    def disable_sensor(device_id):
        try:
            success = set_sensor_enabled(device_id, False)
            if success:
                logger.info('Sensor %s disabled by %s', device_id, request.remote_addr)
            else:
                logger.warning('Failed to disable sensor %s: not found', device_id)
            return jsonify({'success': success, 'message': '传感器已禁用' if success else '传感器不存在'})
        except Exception as e:
            logger.error('Disable sensor %s error: %s', device_id, e, exc_info=True)
            return jsonify({'error': 'Internal server error', 'message': str(e)}), 500

    @app.route('/api/sensor/<device_id>/enable', methods=['POST'])
    def enable_sensor(device_id):
        try:
            success = set_sensor_enabled(device_id, True)
            if success:
                logger.info('Sensor %s enabled by %s', device_id, request.remote_addr)
            else:
                logger.warning('Failed to enable sensor %s: not found', device_id)
            return jsonify({'success': success, 'message': '传感器已启用' if success else '传感器不存在'})
        except Exception as e:
            logger.error('Enable sensor %s error: %s', device_id, e, exc_info=True)
            return jsonify({'error': 'Internal server error', 'message': str(e)}), 500

    @app.route('/api/sensor/<device_id>/boost', methods=['POST'])
    def boost_sensor_temp(device_id):
        try:
            data = request.get_json() or {}
            boost = data.get('boost', True)
            success = set_sensor_temp_boost(device_id, boost)
            if success:
                logger.info('Sensor %s temperature boost %s by %s', device_id,
                           'enabled' if boost else 'disabled', request.remote_addr)
            else:
                logger.warning('Failed to set boost for sensor %s: not found', device_id)
            return jsonify({'success': success, 'message': f'温度提升已{"启用" if boost else "禁用"}' if success else '传感器不存在'})
        except Exception as e:
            logger.error('Boost sensor %s error: %s', device_id, e, exc_info=True)
            return jsonify({'error': 'Internal server error', 'message': str(e)}), 500

    @app.route('/events')
    def sse_events():
        client_ip = request.remote_addr
        client_id = id(request)
        return Response(
            event_stream(client_ip, client_id),
            mimetype='text/event-stream',
            headers={
                'Cache-Control': 'no-cache',
                'Connection': 'keep-alive',
                'Access-Control-Allow-Origin': '*',
                'X-Accel-Buffering': 'no'
            }
        )

    @app.errorhandler(404)
    def not_found(e):
        logger.warning('404 Not Found: %s %s', request.method, request.path)
        return jsonify({'error': 'Not found', 'message': 'The requested resource does not exist'}), 404

    @app.errorhandler(500)
    def internal_error(e):
        logger.error('500 Internal Error: %s %s - %s', request.method, request.path, e, exc_info=True)
        return jsonify({'error': 'Internal server error', 'message': str(e)}), 500

    return app


if __name__ == '__main__':
    app = create_app()
    try:
        app.run(host='0.0.0.0', port=5000, debug=False, use_reloader=False, threaded=True)
    except (KeyboardInterrupt, SystemExit):
        logger.info('Shutting down...')
        stop_scheduler()
        logger.info('Shutdown complete')
