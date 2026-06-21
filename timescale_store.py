import os
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Any

import psycopg
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

logger = logging.getLogger(__name__)

HEARTBEAT_TIMEOUT = int(os.environ.get('HEARTBEAT_TIMEOUT', '30'))
TEMPERATURE_THRESHOLD_GLOBAL = float(os.environ.get('TEMPERATURE_THRESHOLD', '35.0'))

DB_HOST = os.environ.get('TIMESCALE_HOST', 'localhost')
DB_PORT = int(os.environ.get('TIMESCALE_PORT', '5432'))
DB_USER = os.environ.get('TIMESCALE_USER', 'postgres')
DB_PASSWORD = os.environ.get('TIMESCALE_PASSWORD', 'postgres')
DB_NAME = os.environ.get('TIMESCALE_DB', 'iot_monitor')
DB_POOL_MIN = int(os.environ.get('TIMESCALE_POOL_MIN', '2'))
DB_POOL_MAX = int(os.environ.get('TIMESCALE_POOL_MAX', '10'))

_conninfo = (
    f'host={DB_HOST} port={DB_PORT} dbname={DB_NAME} '
    f'user={DB_USER} password={DB_PASSWORD}'
)

_pool: Optional[AsyncConnectionPool] = None


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


async def get_pool() -> AsyncConnectionPool:
    global _pool
    if _pool is None:
        _pool = AsyncConnectionPool(
            conninfo=_conninfo,
            min_size=DB_POOL_MIN,
            max_size=DB_POOL_MAX,
            open=False,
            kwargs={'row_factory': dict_row},
        )
        await _pool.open()
        logger.info(
            'TimescaleDB 连接池已初始化 (psycopg3): host=%s port=%s db=%s pool=[%d-%d]',
            DB_HOST, DB_PORT, DB_NAME, DB_POOL_MIN, DB_POOL_MAX,
        )
    return _pool


async def close_pool() -> None:
    global _pool
    if _pool is not None:
        try:
            await _pool.close()
        except Exception as e:
            logger.warning('关闭连接池时出现警告: %s', e)
        finally:
            _pool = None
            logger.info('TimescaleDB 连接池已关闭')


async def ensure_extension() -> None:
    pool = await get_pool()
    async with pool.connection() as conn:
        await conn.execute('CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE')
    logger.info('TimescaleDB 扩展已确保启用')


async def init_timescale() -> None:
    await ensure_extension()
    logger.info('TimescaleDB 初始化完成')


async def write_sensor_data(
    device_id: str,
    temperature: float,
    humidity: float,
    timestamp: Optional[datetime] = None,
) -> None:
    if timestamp is None:
        timestamp = _utcnow()
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)

    pool = await get_pool()
    async with pool.connection() as conn:
        await conn.execute(
            '''
            INSERT INTO sensor_data (time, device_id, temperature, humidity)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (time, device_id) DO UPDATE SET
                temperature = EXCLUDED.temperature,
                humidity = EXCLUDED.humidity
            ''',
            (timestamp, device_id, float(temperature), float(humidity)),
        )
        await conn.execute(
            '''
            INSERT INTO devices (id, name, status, last_heartbeat, temperature, humidity, last_report)
            VALUES (%s, %s, 'online', %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                last_heartbeat = EXCLUDED.last_heartbeat,
                temperature = EXCLUDED.temperature,
                humidity = EXCLUDED.humidity,
                last_report = EXCLUDED.last_report,
                status = CASE
                    WHEN devices.status = 'offline' THEN 'online'
                    ELSE devices.status
                END
            ''',
            (device_id, device_id, timestamp, float(temperature), float(humidity), timestamp),
        )
    logger.debug(
        '写入传感器数据到 TimescaleDB: %s temp=%.2f humidity=%.2f',
        device_id, temperature, humidity,
    )


async def write_alert(
    device_id: str,
    alert_type: str,
    message: str,
    timestamp: Optional[datetime] = None,
) -> None:
    if timestamp is None:
        timestamp = _utcnow()
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)

    pool = await get_pool()
    async with pool.connection() as conn:
        await conn.execute(
            '''
            INSERT INTO alerts (time, device_id, alert_type, message, acknowledged)
            VALUES (%s, %s, %s, %s, FALSE)
            ON CONFLICT (time, device_id, alert_type) DO NOTHING
            ''',
            (timestamp, device_id, alert_type, str(message)),
        )
    logger.debug('写入告警到 TimescaleDB: %s type=%s', device_id, alert_type)


async def confirm_alert(
    time: datetime,
    device_id: str,
    alert_type: str,
    acknowledged_by: Optional[str] = None,
) -> bool:
    if time.tzinfo is None:
        time = time.replace(tzinfo=timezone.utc)

    ack_time = _utcnow()
    pool = await get_pool()
    async with pool.connection() as conn:
        cur = await conn.execute(
            '''
            UPDATE alerts
            SET acknowledged = TRUE,
                acknowledged_at = %s,
                acknowledged_by = %s
            WHERE time = %s
              AND device_id = %s
              AND alert_type = %s
              AND acknowledged = FALSE
            ''',
            (ack_time, acknowledged_by, time, device_id, alert_type),
        )
    rows_affected = cur.rowcount
    success = rows_affected is not None and rows_affected > 0
    if success:
        logger.info(
            '告警已确认: device=%s type=%s time=%s by=%s',
            device_id, alert_type, time.isoformat(), acknowledged_by,
        )
    else:
        logger.warning(
            '告警确认失败或已确认: device=%s type=%s time=%s',
            device_id, alert_type, time.isoformat(),
        )
    return success


async def confirm_alert_by_id(
    alert_id: str,
    acknowledged_by: Optional[str] = None,
) -> bool:
    try:
        parts = alert_id.split('|', 2)
        if len(parts) != 3:
            return False
        time_str, device_id, alert_type = parts
        time = datetime.fromisoformat(time_str.replace('Z', '+00:00'))
        return await confirm_alert(time, device_id, alert_type, acknowledged_by)
    except (ValueError, KeyError) as e:
        logger.error('解析 alert_id 失败: %s error=%s', alert_id, e)
        return False


async def query_recent_sensor_data(
    device_id: Optional[str] = None,
    minutes: int = 5,
) -> List[Dict[str, Any]]:
    stop = _utcnow()
    start = stop - timedelta(minutes=minutes)

    pool = await get_pool()
    async with pool.connection() as conn:
        if device_id:
            cur = await conn.execute(
                '''
                SELECT time, device_id, temperature, humidity
                FROM sensor_data
                WHERE time BETWEEN %s AND %s
                  AND device_id = %s
                ORDER BY time ASC
                ''',
                (start, stop, device_id),
            )
            rows = await cur.fetchall()
        else:
            cur = await conn.execute(
                '''
                SELECT time, device_id, temperature, humidity
                FROM sensor_data
                WHERE time BETWEEN %s AND %s
                ORDER BY time ASC
                ''',
                (start, stop),
            )
            rows = await cur.fetchall()

    result = []
    for row in rows:
        d = dict(row)
        d['timestamp'] = d.pop('time').isoformat()
        result.append(d)
    return result


async def query_latest_sensor_data(
    device_id: Optional[str] = None,
) -> Dict[str, Dict[str, Any]]:
    stop = _utcnow()
    start = stop - timedelta(minutes=max(5, HEARTBEAT_TIMEOUT * 2))

    pool = await get_pool()
    async with pool.connection() as conn:
        if device_id:
            cur = await conn.execute(
                '''
                SELECT DISTINCT ON (device_id)
                    time, device_id, temperature, humidity
                FROM sensor_data
                WHERE time BETWEEN %s AND %s
                  AND device_id = %s
                ORDER BY device_id, time DESC
                ''',
                (start, stop, device_id),
            )
            rows = await cur.fetchall()
        else:
            cur = await conn.execute(
                '''
                SELECT DISTINCT ON (device_id)
                    time, device_id, temperature, humidity
                FROM sensor_data
                WHERE time BETWEEN %s AND %s
                ORDER BY device_id, time DESC
                ''',
                (start, stop),
            )
            rows = await cur.fetchall()

    latest = {}
    for row in rows:
        d = dict(row)
        t = d.pop('time')
        ts = t.isoformat()
        dev_id = d.pop('device_id')
        latest[dev_id] = {
            **d,
            'timestamp': ts,
            'last_report': ts,
            'last_heartbeat': ts,
        }
    return latest


async def query_recent_alerts(
    limit: int = 20,
    include_acknowledged: bool = False,
) -> List[Dict[str, Any]]:
    stop = _utcnow()
    start = stop - timedelta(hours=24)

    pool = await get_pool()
    async with pool.connection() as conn:
        if include_acknowledged:
            cur = await conn.execute(
                '''
                SELECT a.time, a.device_id, a.alert_type, a.message,
                       a.acknowledged, a.acknowledged_at, a.acknowledged_by,
                       d.name AS device_name
                FROM alerts a
                LEFT JOIN devices d ON a.device_id = d.id
                WHERE a.time BETWEEN %s AND %s
                ORDER BY a.time DESC
                LIMIT %s
                ''',
                (start, stop, limit),
            )
        else:
            cur = await conn.execute(
                '''
                SELECT a.time, a.device_id, a.alert_type, a.message,
                       a.acknowledged, a.acknowledged_at, a.acknowledged_by,
                       d.name AS device_name
                FROM alerts a
                LEFT JOIN devices d ON a.device_id = d.id
                WHERE a.time BETWEEN %s AND %s
                  AND a.acknowledged = FALSE
                ORDER BY a.time DESC
                LIMIT %s
                ''',
                (start, stop, limit),
            )
        rows = await cur.fetchall()

    alerts = []
    for row in rows:
        d = dict(row)
        t = d.pop('time')
        d['timestamp'] = t.isoformat()
        d['alert_id'] = f"{t.isoformat()}|{d['device_id']}|{d['alert_type']}"
        d['device_name'] = d.get('device_name') or d['device_id']
        alerts.append(d)
    return alerts


async def query_device_ids_with_active_alerts(
    alert_type: Optional[str] = None,
) -> set:
    stop = _utcnow()
    start = stop - timedelta(hours=1)

    pool = await get_pool()
    async with pool.connection() as conn:
        if alert_type:
            cur = await conn.execute(
                '''
                SELECT DISTINCT device_id
                FROM alerts
                WHERE time BETWEEN %s AND %s
                  AND acknowledged = FALSE
                  AND alert_type = %s
                ''',
                (start, stop, alert_type),
            )
        else:
            cur = await conn.execute(
                '''
                SELECT DISTINCT device_id
                FROM alerts
                WHERE time BETWEEN %s AND %s
                  AND acknowledged = FALSE
                ''',
                (start, stop),
            )
        rows = await cur.fetchall()

    return {r['device_id'] for r in rows}


async def get_all_devices() -> List[Dict[str, Any]]:
    pool = await get_pool()
    async with pool.connection() as conn:
        cur = await conn.execute(
            '''
            SELECT id, name, status, last_heartbeat,
                   temperature, humidity, last_report
            FROM devices
            ORDER BY id
            ''',
        )
        rows = await cur.fetchall()

    devices = []
    for row in rows:
        d = dict(row)
        if d.get('last_heartbeat'):
            d['last_heartbeat'] = d['last_heartbeat'].isoformat()
        if d.get('last_report'):
            d['last_report'] = d['last_report'].isoformat()
        devices.append(d)
    logger.debug('获取 %d 个设备', len(devices))
    return devices


async def get_device_status(device_id: str) -> Optional[Dict[str, Any]]:
    pool = await get_pool()
    async with pool.connection() as conn:
        cur = await conn.execute(
            '''
            SELECT id, name, status, last_heartbeat,
                   temperature, humidity, last_report
            FROM devices WHERE id = %s
            ''',
            (device_id,),
        )
        row = await cur.fetchone()

    if row is None:
        return None
    d = dict(row)
    if d.get('last_heartbeat'):
        d['last_heartbeat'] = d['last_heartbeat'].isoformat()
    if d.get('last_report'):
        d['last_report'] = d['last_report'].isoformat()
    return d


async def update_device_heartbeat(device_id: str) -> bool:
    timestamp = _utcnow()
    pool = await get_pool()
    async with pool.connection() as conn:
        cur = await conn.execute(
            '''
            UPDATE devices
            SET last_heartbeat = %s, status = 'online'
            WHERE id = %s
            ''',
            (timestamp, device_id),
        )
    rows_affected = cur.rowcount
    if rows_affected is not None and rows_affected > 0:
        logger.debug('更新设备 %s 心跳', device_id)
    return rows_affected is not None and rows_affected > 0


async def check_heartbeat() -> List[Dict[str, Any]]:
    timeout_time = _utcnow() - timedelta(seconds=HEARTBEAT_TIMEOUT)
    pool = await get_pool()
    alerts: List[Dict[str, Any]] = []

    async with pool.connection() as conn:
        async with conn.transaction():
            cur = await conn.execute(
                '''
                SELECT id, name FROM devices
                WHERE status = 'online'
                  AND (last_heartbeat < %s OR last_heartbeat IS NULL)
                FOR UPDATE
                ''',
                (timeout_time,),
            )
            offline_devices = await cur.fetchall()

            for device in offline_devices:
                device_id = device['id']
                device_name = device['name']

                await conn.execute(
                    "UPDATE devices SET status = 'offline' WHERE id = %s",
                    (device_id,),
                )

                alert_msg = f'{device_name} 已离线，超过 {HEARTBEAT_TIMEOUT} 秒无心跳'
                alert_time = _utcnow()
                await conn.execute(
                    '''
                    INSERT INTO alerts (time, device_id, alert_type, message, acknowledged)
                    VALUES (%s, %s, 'offline', %s, FALSE)
                    ON CONFLICT (time, device_id, alert_type) DO NOTHING
                    ''',
                    (alert_time, device_id, alert_msg),
                )

                alerts.append({
                    'device_id': device_id,
                    'alert_type': 'offline',
                    'message': alert_msg,
                    'timestamp': alert_time.isoformat(),
                    'alert_id': f"{alert_time.isoformat()}|{device_id}|offline",
                    'acknowledged': False,
                    'device_name': device_name,
                })
                logger.warning('设备离线告警: %s', alert_msg)

            cur = await conn.execute(
                '''
                SELECT id, name, temperature
                FROM devices
                WHERE status = 'online' AND temperature > %s
                ''',
                (TEMPERATURE_THRESHOLD_GLOBAL,),
            )
            over_temp_devices = await cur.fetchall()

            for device in over_temp_devices:
                device_id = device['id']
                device_name = device['name']
                temperature = device['temperature']

                alert_msg = f'{device_name} 温度过高: {temperature:.1f}°C (阈值: {TEMPERATURE_THRESHOLD_GLOBAL}°C)'
                alert_time = _utcnow()
                await conn.execute(
                    '''
                    INSERT INTO alerts (time, device_id, alert_type, message, acknowledged)
                    VALUES (%s, %s, 'temperature', %s, FALSE)
                    ON CONFLICT (time, device_id, alert_type) DO NOTHING
                    ''',
                    (alert_time, device_id, alert_msg),
                )

                alerts.append({
                    'device_id': device_id,
                    'alert_type': 'temperature',
                    'message': alert_msg,
                    'timestamp': alert_time.isoformat(),
                    'alert_id': f"{alert_time.isoformat()}|{device_id}|temperature",
                    'acknowledged': False,
                    'device_name': device_name,
                })
                logger.warning('温度告警: %s', alert_msg)

    if alerts:
        logger.info('生成 %d 条告警', len(alerts))
    return alerts
