import json
import os
import logging
import asyncio
import random
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any, List

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, Query, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

import timescale_store as ts
from message_bus import (
    subscribe_alerts_async,
    unsubscribe_alerts_async,
    publish_alert_async,
    close_async_message_bus,
    ALERT_CHANNEL,
)

logger = logging.getLogger(__name__)

TEMPERATURE_THRESHOLD = float(os.environ.get('TEMPERATURE_THRESHOLD', '35.0'))
HEARTBEAT_TIMEOUT = int(os.environ.get('HEARTBEAT_TIMEOUT', '30'))
GRAFANA_URL = os.environ.get('GRAFANA_URL', 'http://localhost:3000')
GRAFANA_DASHBOARD_UID = os.environ.get('GRAFANA_DASHBOARD_UID', 'iot-monitor-dashboard')

sensor_states: Dict[str, Dict[str, Any]] = {}
ws_connections: Dict[str, WebSocket] = {}
_ws_id_counter = 0
_ws_lock = asyncio.Lock()

_scheduler_task: Optional[asyncio.Task] = None
_heartbeat_task: Optional[asyncio.Task] = None


def setup_logging():
    log_level = os.environ.get('LOG_LEVEL', 'INFO').upper()
    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler()],
    )


setup_logging()


def init_sensor_states():
    global sensor_states
    for i in range(1, 11):
        device_id = f'sensor_{i:02d}'
        sensor_states[device_id] = {
            'enabled': True,
            'base_temp': random.uniform(20.0, 28.0),
            'base_humidity': random.uniform(40.0, 70.0),
            'temp_boost': False,
        }
    logger.info('初始化 %d 个传感器状态', len(sensor_states))


def set_sensor_enabled(device_id: str, enabled: bool) -> bool:
    global sensor_states
    if device_id in sensor_states:
        sensor_states[device_id]['enabled'] = enabled
        logger.info('传感器 %s 模拟器 %s', device_id, '启用' if enabled else '禁用')
        return True
    logger.warning('尝试切换不存在的传感器: %s', device_id)
    return False


def set_sensor_temp_boost(device_id: str, boost: bool) -> bool:
    global sensor_states
    if device_id in sensor_states:
        sensor_states[device_id]['temp_boost'] = boost
        logger.info('传感器 %s 温度提升 %s', device_id, '启用' if boost else '禁用')
        return True
    logger.warning('尝试提升不存在的传感器温度: %s', device_id)
    return False


async def broadcast_to_ws(message: Dict[str, Any]):
    async with _ws_lock:
        connections = list(ws_connections.items())
    for ws_id, websocket in connections:
        try:
            await websocket.send_json(message)
        except Exception as e:
            logger.warning('向 WS 客户端 %s 广播失败: %s', ws_id, e)


async def on_alert_broadcast(alert: Dict[str, Any]) -> None:
    msg = {'type': 'alert', 'data': alert}
    await broadcast_to_ws(msg)


async def simulate_sensor_data_task():
    logger.info('传感器模拟器任务已启动 (每 3 秒)')
    try:
        while True:
            await asyncio.sleep(3)
            try:
                count = 0
                for device_id, state in sensor_states.items():
                    if not state['enabled']:
                        continue
                    try:
                        temp_variation = random.uniform(-1.0, 1.0)
                        humidity_variation = random.uniform(-2.0, 2.0)
                        temperature = state['base_temp'] + temp_variation
                        humidity = max(0.0, min(100.0, state['base_humidity'] + humidity_variation))
                        if state['temp_boost']:
                            temperature = random.uniform(36.0, 42.0)
                        await ts.write_sensor_data(
                            device_id,
                            round(temperature, 2),
                            round(humidity, 2),
                        )
                        count += 1
                    except Exception as e:
                        logger.error('模拟传感器 %s 数据失败: %s', device_id, e, exc_info=True)
                if count > 0:
                    logger.debug('已模拟 %d 个传感器数据', count)
            except Exception as e:
                logger.error('传感器模拟器循环错误: %s', e, exc_info=True)
    except asyncio.CancelledError:
        logger.info('传感器模拟器任务已取消')
    finally:
        logger.info('传感器模拟器任务已停止')


async def heartbeat_check_task():
    logger.info('心跳检查任务已启动 (每 1 秒)')
    try:
        while True:
            await asyncio.sleep(1)
            try:
                alerts = await ts.check_heartbeat()
                if alerts:
                    for alert in alerts:
                        try:
                            await publish_alert_async(alert)
                        except Exception as e:
                            logger.error('发布告警 %s 失败: %s', alert, e, exc_info=True)
            except Exception as e:
                logger.error('心跳检查循环错误: %s', e, exc_info=True)
    except asyncio.CancelledError:
        logger.info('心跳检查任务已取消')
    finally:
        logger.info('心跳检查任务已停止')


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info('=' * 60)
    logger.info('IoT MQTT 设备网关 (FastAPI/ASGI) 启动中...')
    logger.info('温度阈值: %s°C', TEMPERATURE_THRESHOLD)
    logger.info('心跳超时: %s秒', HEARTBEAT_TIMEOUT)

    use_redis = os.environ.get('USE_REDIS', 'false').lower() == 'true'
    use_mqtt = os.environ.get('USE_MQTT', 'true').lower() == 'true'
    bus_type = 'Redis' if use_redis else '内存'
    logger.info('消息总线: %s%s', bus_type, ' (集群版兼容)' if use_redis else '')
    logger.info('MQTT 网关: %s', '启用' if use_mqtt else '禁用')
    logger.info(
        '时序存储: TimescaleDB (PostgreSQL) → host=%s port=%s db=%s',
        os.environ.get('TIMESCALE_HOST', 'localhost'),
        os.environ.get('TIMESCALE_PORT', '5432'),
        os.environ.get('TIMESCALE_DB', 'iot_monitor'),
    )
    logger.info('asyncpg 连接池: [%s-%s]',
                os.environ.get('TIMESCALE_POOL_MIN', '2'),
                os.environ.get('TIMESCALE_POOL_MAX', '10'))

    try:
        await ts.init_timescale()
        logger.info('TimescaleDB 初始化成功')
    except Exception as e:
        logger.warning('TimescaleDB 初始化警告 (若建表脚本未执行可忽略): %s', e)

    init_sensor_states()

    await subscribe_alerts_async(on_alert_broadcast)
    logger.info('已订阅告警频道: %s', ALERT_CHANNEL)

    global _scheduler_task, _heartbeat_task
    _scheduler_task = asyncio.create_task(simulate_sensor_data_task(), name='sensor-simulator')
    _heartbeat_task = asyncio.create_task(heartbeat_check_task(), name='heartbeat-check')

    logger.info('Grafana 仪表盘: %s/d/%s', GRAFANA_URL, GRAFANA_DASHBOARD_UID)
    logger.info('WebSocket 端点: ws://localhost:8000/ws')
    logger.info('HTTP API: http://localhost:8000')
    logger.info('=' * 60)

    yield

    logger.info('正在关闭 FastAPI 应用...')

    if _scheduler_task and not _scheduler_task.done():
        _scheduler_task.cancel()
        try:
            await _scheduler_task
        except asyncio.CancelledError:
            pass
    if _heartbeat_task and not _heartbeat_task.done():
        _heartbeat_task.cancel()
        try:
            await _heartbeat_task
        except asyncio.CancelledError:
            pass

    await unsubscribe_alerts_async(on_alert_broadcast)
    await close_async_message_bus()
    await ts.close_pool()
    logger.info('FastAPI 应用关闭完成')


app = FastAPI(
    title='IoT 监控系统',
    description='基于 FastAPI + TimescaleDB + WebSocket 的物联网监控平台',
    version='2.0.0',
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=True,
    allow_methods=['*'],
    allow_headers=['*'],
)

_templates_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'templates')
templates = Jinja2Templates(directory=_templates_dir)


async def handle_ws_ping(websocket: WebSocket, payload: Dict[str, Any], ws_id: str):
    await websocket.send_json({'type': 'pong', 'timestamp': datetime.now(timezone.utc).isoformat()})
    logger.debug('WS %s: ping → pong', ws_id)


async def handle_confirm_alert(websocket: WebSocket, payload: Dict[str, Any], ws_id: str):
    alert_id = payload.get('alert_id')
    acknowledged_by = payload.get('by', f'ws:{ws_id}')

    if not alert_id:
        await websocket.send_json({
            'type': 'error',
            'command': 'confirm_alert',
            'message': '缺少 alert_id 参数',
        })
        return

    success = await ts.confirm_alert_by_id(alert_id, acknowledged_by)

    await websocket.send_json({
        'type': 'confirm_ack',
        'success': success,
        'alert_id': alert_id,
    })

    if success:
        await broadcast_to_ws({
            'type': 'alert_cleared',
            'alert_id': alert_id,
        })
        logger.info('WS %s: 告警 %s 已确认', ws_id, alert_id)
    else:
        logger.warning('WS %s: 告警确认失败 %s', ws_id, alert_id)


async def handle_switch_view(websocket: WebSocket, payload: Dict[str, Any], ws_id: str):
    device_id = payload.get('device_id')
    view_mode = payload.get('view_mode', 'detail')

    if not device_id:
        devices = await ts.get_all_devices()
        await websocket.send_json({
            'type': 'view_changed',
            'view_mode': view_mode,
            'devices': devices,
        })
        logger.info('WS %s: 切换到设备列表视图', ws_id)
        return

    device = await ts.get_device_status(device_id)
    history = await ts.query_recent_sensor_data(device_id=device_id, minutes=5)

    await websocket.send_json({
        'type': 'view_changed',
        'view_mode': view_mode,
        'device': device,
        'device_id': device_id,
        'history': history,
    })
    logger.info('WS %s: 切换到设备 %s 视图 (%s)', ws_id, device_id, view_mode)


async def handle_list_devices(websocket: WebSocket, payload: Dict[str, Any], ws_id: str):
    devices = await ts.get_all_devices()
    for device in devices:
        state = sensor_states.get(device['id'], {})
        device['simulator_enabled'] = state.get('enabled', True)
        device['temp_boost'] = state.get('temp_boost', False)
    await websocket.send_json({'type': 'devices_list', 'data': devices})
    logger.debug('WS %s: 返回 %d 个设备', ws_id, len(devices))


async def handle_list_alerts(websocket: WebSocket, payload: Dict[str, Any], ws_id: str):
    include_ack = payload.get('include_acknowledged', False)
    limit = int(payload.get('limit', 20))
    alerts = await ts.query_recent_alerts(limit=limit, include_acknowledged=include_ack)
    await websocket.send_json({
        'type': 'alerts_list',
        'data': alerts,
        'include_acknowledged': include_ack,
    })
    logger.debug('WS %s: 返回 %d 条告警', ws_id, len(alerts))


async def handle_toggle_sensor(websocket: WebSocket, payload: Dict[str, Any], ws_id: str):
    device_id = payload.get('device_id')
    enabled = payload.get('enabled', True)

    if not device_id:
        await websocket.send_json({
            'type': 'error',
            'command': 'toggle_sensor',
            'message': '缺少 device_id 参数',
        })
        return

    success = set_sensor_enabled(device_id, enabled)
    await websocket.send_json({
        'type': 'sensor_toggled',
        'device_id': device_id,
        'enabled': enabled,
        'success': success,
    })
    if success:
        await broadcast_to_ws({
            'type': 'sensor_state_changed',
            'device_id': device_id,
            'enabled': enabled,
            'by': ws_id,
        })


async def handle_toggle_boost(websocket: WebSocket, payload: Dict[str, Any], ws_id: str):
    device_id = payload.get('device_id')
    boost = payload.get('boost', True)

    if not device_id:
        await websocket.send_json({
            'type': 'error',
            'command': 'toggle_boost',
            'message': '缺少 device_id 参数',
        })
        return

    success = set_sensor_temp_boost(device_id, boost)
    await websocket.send_json({
        'type': 'boost_toggled',
        'device_id': device_id,
        'boost': boost,
        'success': success,
    })
    if success:
        await broadcast_to_ws({
            'type': 'boost_state_changed',
            'device_id': device_id,
            'boost': boost,
            'by': ws_id,
        })


WS_COMMAND_HANDLERS = {
    'ping': handle_ws_ping,
    'confirm_alert': handle_confirm_alert,
    'switch_view': handle_switch_view,
    'list_devices': handle_list_devices,
    'list_alerts': handle_list_alerts,
    'toggle_sensor': handle_toggle_sensor,
    'toggle_boost': handle_toggle_boost,
}


@app.get('/', response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse('index.html', {'request': request})


@app.get('/api/devices')
async def api_devices():
    try:
        devices = await ts.get_all_devices()
        for device in devices:
            state = sensor_states.get(device['id'], {})
            device['simulator_enabled'] = state.get('enabled', True)
            device['temp_boost'] = state.get('temp_boost', False)
        logger.debug('Devices API: 返回 %d 个设备', len(devices))
        return JSONResponse(devices)
    except Exception as e:
        logger.error('Devices API 错误: %s', e, exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get('/api/alerts')
async def api_alerts(
    limit: int = Query(20, ge=1, le=100),
    include_acknowledged: bool = Query(False),
):
    try:
        alerts = await ts.query_recent_alerts(limit=limit, include_acknowledged=include_acknowledged)
        logger.debug('Alerts API: 返回 %d 条告警', len(alerts))
        return JSONResponse(alerts)
    except Exception as e:
        logger.error('Alerts API 错误: %s', e, exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get('/api/config')
async def api_config():
    use_redis = os.environ.get('USE_REDIS', 'false').lower() == 'true'
    use_mqtt = os.environ.get('USE_MQTT', 'true').lower() == 'true'
    return JSONResponse({
        'temperature_threshold': TEMPERATURE_THRESHOLD,
        'heartbeat_timeout': HEARTBEAT_TIMEOUT,
        'message_bus': 'redis' if use_redis else 'memory',
        'mqtt_enabled': use_mqtt,
        'mqtt_broker': os.environ.get('MQTT_BROKER_HOST', 'localhost'),
        'mqtt_port': int(os.environ.get('MQTT_BROKER_PORT', '1883')),
        'mqtt_topic': os.environ.get('MQTT_TOPIC', '/sensor/+/data'),
        'timescaledb_host': os.environ.get('TIMESCALE_HOST', 'localhost'),
        'timescaledb_port': int(os.environ.get('TIMESCALE_PORT', '5432')),
        'timescaledb_db': os.environ.get('TIMESCALE_DB', 'iot_monitor'),
        'grafana_url': GRAFANA_URL,
        'grafana_dashboard_uid': GRAFANA_DASHBOARD_UID,
    })


@app.get('/api/sensor/{device_id}/history')
async def api_sensor_history(device_id: str, minutes: int = Query(5, ge=1, le=1440)):
    try:
        data = await ts.query_recent_sensor_data(device_id=device_id, minutes=minutes)
        return JSONResponse(data)
    except Exception as e:
        logger.error('Sensor history API 错误 (device=%s): %s', device_id, e, exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get('/api/grafana/dashboard')
async def api_grafana_dashboard():
    dashboard_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'grafana_dash.json')
    if os.path.exists(dashboard_path):
        return FileResponse(
            dashboard_path,
            media_type='application/json',
            filename='grafana_dash.json',
        )
    raise HTTPException(status_code=404, detail='Dashboard file not found')


@app.get('/grafana', response_class=HTMLResponse)
async def grafana_redirect(request: Request):
    dashboard_url = f'{GRAFANA_URL}/d/{GRAFANA_DASHBOARD_UID}'
    return templates.TemplateResponse(
        'grafana_redirect.html',
        {'request': request, 'dashboard_url': dashboard_url},
    )


@app.post('/api/sensor/{device_id}/disable')
async def disable_sensor(device_id: str):
    try:
        success = set_sensor_enabled(device_id, False)
        return JSONResponse({
            'success': success,
            'message': '传感器已禁用' if success else '传感器不存在',
        })
    except Exception as e:
        logger.error('禁用传感器 %s 错误: %s', device_id, e, exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.post('/api/sensor/{device_id}/enable')
async def enable_sensor(device_id: str):
    try:
        success = set_sensor_enabled(device_id, True)
        return JSONResponse({
            'success': success,
            'message': '传感器已启用' if success else '传感器不存在',
        })
    except Exception as e:
        logger.error('启用传感器 %s 错误: %s', device_id, e, exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.post('/api/sensor/{device_id}/boost')
async def boost_sensor_temp(device_id: str, request: Request):
    try:
        body = await request.json()
        boost = body.get('boost', True) if body else True
    except Exception:
        boost = True
    try:
        success = set_sensor_temp_boost(device_id, boost)
        return JSONResponse({
            'success': success,
            'message': f'温度提升已{"启用" if boost else "禁用"}' if success else '传感器不存在',
        })
    except Exception as e:
        logger.error('Boost 传感器 %s 错误: %s', device_id, e, exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.post('/api/alerts/{alert_id}/confirm')
async def confirm_alert_api(alert_id: str, request: Request):
    try:
        body = await request.json()
        acknowledged_by = body.get('by', 'api') if body else 'api'
    except Exception:
        acknowledged_by = 'api'
    try:
        success = await ts.confirm_alert_by_id(alert_id, acknowledged_by)
        if success:
            await broadcast_to_ws({
                'type': 'alert_cleared',
                'alert_id': alert_id,
            })
        return JSONResponse({
            'success': success,
            'alert_id': alert_id,
            'message': '告警已确认' if success else '告警确认失败或已确认',
        })
    except Exception as e:
        logger.error('确认告警 %s 错误: %s', alert_id, e, exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.websocket('/ws')
async def websocket_endpoint(websocket: WebSocket):
    global _ws_id_counter
    await websocket.accept()

    async with _ws_lock:
        _ws_id_counter += 1
        ws_id = f'ws_{_ws_id_counter}'
        ws_connections[ws_id] = websocket

    client_host = websocket.client.host if websocket.client else 'unknown'
    logger.info('WS 客户端已连接: id=%s ip=%s 总连接数=%d',
                ws_id, client_host, len(ws_connections))

    try:
        await websocket.send_json({
            'type': 'connected',
            'message': 'WebSocket 连接已建立',
            'ws_id': ws_id,
        })

        while True:
            try:
                data = await websocket.receive_text()
                try:
                    msg = json.loads(data)
                except json.JSONDecodeError:
                    await websocket.send_json({
                        'type': 'error',
                        'message': '无效的 JSON 格式',
                    })
                    continue

                command = msg.get('command') or msg.get('action')
                if not command:
                    if msg.get('type') == 'ping' or (isinstance(msg, dict) and len(msg) == 0):
                        command = 'ping'
                    else:
                        await websocket.send_json({
                            'type': 'error',
                            'message': '缺少 command/action 字段',
                        })
                        continue

                handler = WS_COMMAND_HANDLERS.get(command)
                if handler is None:
                    await websocket.send_json({
                        'type': 'error',
                        'message': f'未知命令: {command}',
                        'available_commands': list(WS_COMMAND_HANDLERS.keys()),
                    })
                    continue

                try:
                    await handler(websocket, msg, ws_id)
                except Exception as e:
                    logger.error('WS 命令处理错误 %s: %s', command, e, exc_info=True)
                    await websocket.send_json({
                        'type': 'error',
                        'command': command,
                        'message': str(e),
                    })

            except WebSocketDisconnect:
                break

    except Exception as e:
        logger.error('WS 连接异常 id=%s: %s', ws_id, e, exc_info=True)
    finally:
        async with _ws_lock:
            ws_connections.pop(ws_id, None)
        logger.info('WS 客户端已断开: id=%s ip=%s 剩余连接=%d',
                    ws_id, client_host, len(ws_connections))


if __name__ == '__main__':
    port = int(os.environ.get('PORT', '8000'))
    host = os.environ.get('HOST', '0.0.0.0')
    uvicorn.run(
        app,
        host=host,
        port=port,
        log_level=os.environ.get('LOG_LEVEL', 'info').lower(),
        access_log=True,
    )
