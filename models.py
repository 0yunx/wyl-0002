import sqlite3
import threading
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

DB_PATH = 'iot_monitor.db'
TEMPERATURE_THRESHOLD = 35.0
HEARTBEAT_TIMEOUT = 30

db_lock = threading.Lock()
_wal_initialized = False
_wal_init_lock = threading.Lock()


def _init_wal(conn: sqlite3.Connection) -> None:
    global _wal_initialized
    with _wal_init_lock:
        if not _wal_initialized:
            try:
                cursor = conn.cursor()
                cursor.execute('PRAGMA journal_mode=WAL')
                result = cursor.fetchone()
                cursor.execute('PRAGMA synchronous=NORMAL')
                cursor.execute('PRAGMA busy_timeout=5000')
                cursor.execute('PRAGMA cache_size=-64000')
                cursor.execute('PRAGMA temp_store=MEMORY')
                cursor.execute('PRAGMA mmap_size=2147483648')
                logger.info('SQLite WAL mode enabled: %s, synchronous=NORMAL, busy_timeout=5000ms',
                            result[0] if result else 'unknown')
                _wal_initialized = True
            except Exception as e:
                logger.warning('Failed to enable SQLite WAL mode: %s', e)


def init_db():
    with db_lock:
        conn = sqlite3.connect(DB_PATH)
        try:
            _init_wal(conn)
            cursor = conn.cursor()

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS devices (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'online',
                    last_heartbeat DATETIME,
                    temperature REAL,
                    humidity REAL,
                    last_report DATETIME
                )
            ''')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS sensor_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    device_id TEXT NOT NULL,
                    temperature REAL NOT NULL,
                    humidity REAL NOT NULL,
                    timestamp DATETIME NOT NULL,
                    FOREIGN KEY (device_id) REFERENCES devices (id)
                )
            ''')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS alerts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    device_id TEXT NOT NULL,
                    alert_type TEXT NOT NULL,
                    message TEXT NOT NULL,
                    timestamp DATETIME NOT NULL,
                    acknowledged INTEGER DEFAULT 0,
                    FOREIGN KEY (device_id) REFERENCES devices (id)
                )
            ''')

            cursor.execute('CREATE INDEX IF NOT EXISTS idx_sensor_data_device_time ON sensor_data(device_id, timestamp DESC)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_alerts_timestamp ON alerts(timestamp DESC)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_devices_status ON devices(status)')

            cursor.execute('SELECT COUNT(*) FROM devices')
            if cursor.fetchone()[0] == 0:
                for i in range(1, 11):
                    device_id = f'sensor_{i:02d}'
                    cursor.execute('''
                        INSERT INTO devices (id, name, status, last_heartbeat)
                        VALUES (?, ?, 'online', ?)
                    ''', (device_id, f'温湿度传感器 {i}', datetime.now().isoformat()))
                logger.info('Initialized 10 devices in database')

            conn.commit()
            logger.info('Database initialized successfully')
        except Exception as e:
            logger.error('Database initialization failed: %s', e, exc_info=True)
            conn.rollback()
            raise
        finally:
            conn.close()


def get_db_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    _init_wal(conn)
    return conn


def insert_sensor_data(device_id, temperature, humidity):
    timestamp = datetime.now().isoformat()
    with db_lock:
        conn = get_db_connection()
        try:
            cursor = conn.cursor()

            cursor.execute('''
                INSERT INTO sensor_data (device_id, temperature, humidity, timestamp)
                VALUES (?, ?, ?, ?)
            ''', (device_id, temperature, humidity, timestamp))

            cursor.execute('''
                UPDATE devices
                SET temperature = ?, humidity = ?, last_report = ?, last_heartbeat = ?
                WHERE id = ?
            ''', (temperature, humidity, timestamp, timestamp, device_id))

            conn.commit()
            logger.debug('Inserted sensor data for %s: temp=%.1f, humidity=%.1f',
                        device_id, temperature, humidity)
        except Exception as e:
            logger.error('Failed to insert sensor data for %s: %s', device_id, e, exc_info=True)
            conn.rollback()
            raise
        finally:
            conn.close()

    return {'device_id': device_id, 'temperature': temperature, 'humidity': humidity, 'timestamp': timestamp}


def check_heartbeat():
    with db_lock:
        conn = get_db_connection()
        try:
            cursor = conn.cursor()

            timeout_time = (datetime.now() - timedelta(seconds=HEARTBEAT_TIMEOUT)).isoformat()

            cursor.execute('''
                SELECT id, name FROM devices
                WHERE status = 'online' AND (last_heartbeat < ? OR last_heartbeat IS NULL)
            ''', (timeout_time,))

            offline_devices = cursor.fetchall()
            alerts = []

            for device in offline_devices:
                device_id = device['id']
                device_name = device['name']

                cursor.execute('''
                    UPDATE devices SET status = 'offline' WHERE id = ?
                ''', (device_id,))

                cursor.execute('''
                    SELECT COUNT(*) FROM alerts
                    WHERE device_id = ? AND alert_type = 'offline' AND acknowledged = 0
                ''', (device_id,))

                if cursor.fetchone()[0] == 0:
                    alert_msg = f'{device_name} 已离线，超过 {HEARTBEAT_TIMEOUT} 秒无心跳'
                    cursor.execute('''
                        INSERT INTO alerts (device_id, alert_type, message, timestamp)
                        VALUES (?, 'offline', ?, ?)
                    ''', (device_id, alert_msg, datetime.now().isoformat()))

                    alerts.append({
                        'id': cursor.lastrowid,
                        'device_id': device_id,
                        'alert_type': 'offline',
                        'message': alert_msg,
                        'timestamp': datetime.now().isoformat()
                    })
                    logger.warning('Device offline alert: %s', alert_msg)

            cursor.execute('''
                SELECT d.id, d.name, d.temperature, d.last_heartbeat
                FROM devices d
                WHERE d.status = 'online' AND d.temperature > ?
            ''', (TEMPERATURE_THRESHOLD,))

            over_temp_devices = cursor.fetchall()

            for device in over_temp_devices:
                device_id = device['id']
                device_name = device['name']
                temperature = device['temperature']

                cursor.execute('''
                    SELECT COUNT(*) FROM alerts
                    WHERE device_id = ? AND alert_type = 'temperature' AND acknowledged = 0
                ''', (device_id,))

                if cursor.fetchone()[0] == 0:
                    alert_msg = f'{device_name} 温度过高: {temperature:.1f}°C (阈值: {TEMPERATURE_THRESHOLD}°C)'
                    cursor.execute('''
                        INSERT INTO alerts (device_id, alert_type, message, timestamp)
                        VALUES (?, 'temperature', ?, ?)
                    ''', (device_id, alert_msg, datetime.now().isoformat()))

                    alerts.append({
                        'id': cursor.lastrowid,
                        'device_id': device_id,
                        'alert_type': 'temperature',
                        'message': alert_msg,
                        'timestamp': datetime.now().isoformat()
                    })
                    logger.warning('Temperature alert: %s', alert_msg)

            conn.commit()
            if alerts:
                logger.info('Generated %d alerts', len(alerts))
            return alerts
        except Exception as e:
            logger.error('Heartbeat check failed: %s', e, exc_info=True)
            conn.rollback()
            return []
        finally:
            conn.close()


def get_all_devices():
    with db_lock:
        conn = get_db_connection()
        try:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM devices ORDER BY id')
            devices = [dict(row) for row in cursor.fetchall()]
            logger.debug('Fetched %d devices', len(devices))
            return devices
        except Exception as e:
            logger.error('Failed to fetch devices: %s', e, exc_info=True)
            raise
        finally:
            conn.close()


def get_recent_alerts(limit=20):
    with db_lock:
        conn = get_db_connection()
        try:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT a.*, d.name as device_name
                FROM alerts a
                JOIN devices d ON a.device_id = d.id
                ORDER BY a.timestamp DESC
                LIMIT ?
            ''', (limit,))
            alerts = [dict(row) for row in cursor.fetchall()]
            logger.debug('Fetched %d recent alerts', len(alerts))
            return alerts
        except Exception as e:
            logger.error('Failed to fetch alerts: %s', e, exc_info=True)
            raise
        finally:
            conn.close()


def get_device_status(device_id):
    with db_lock:
        conn = get_db_connection()
        try:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM devices WHERE id = ?', (device_id,))
            device = cursor.fetchone()
            return dict(device) if device else None
        except Exception as e:
            logger.error('Failed to fetch device %s status: %s', device_id, e, exc_info=True)
            raise
        finally:
            conn.close()


def update_device_heartbeat(device_id):
    with db_lock:
        conn = get_db_connection()
        try:
            cursor = conn.cursor()
            timestamp = datetime.now().isoformat()
            cursor.execute('''
                UPDATE devices
                SET last_heartbeat = ?, status = 'online'
                WHERE id = ?
            ''', (timestamp, device_id))
            conn.commit()
            logger.debug('Updated heartbeat for device %s', device_id)
        except Exception as e:
            logger.error('Failed to update heartbeat for %s: %s', device_id, e, exc_info=True)
            conn.rollback()
            raise
        finally:
            conn.close()
