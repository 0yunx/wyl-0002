import sqlite3
import threading
import json
from datetime import datetime, timedelta

DB_PATH = 'iot_monitor.db'
TEMPERATURE_THRESHOLD = 35.0
HEARTBEAT_TIMEOUT = 30

db_lock = threading.Lock()

def init_db():
    with db_lock:
        conn = sqlite3.connect(DB_PATH)
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

        cursor.execute('SELECT COUNT(*) FROM devices')
        if cursor.fetchone()[0] == 0:
            for i in range(1, 11):
                device_id = f'sensor_{i:02d}'
                cursor.execute('''
                    INSERT INTO devices (id, name, status, last_heartbeat)
                    VALUES (?, ?, 'online', ?)
                ''', (device_id, f'温湿度传感器 {i}', datetime.now().isoformat()))

        conn.commit()
        conn.close()

def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def insert_sensor_data(device_id, temperature, humidity):
    timestamp = datetime.now().isoformat()
    with db_lock:
        conn = get_db_connection()
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
        conn.close()

    return {'device_id': device_id, 'temperature': temperature, 'humidity': humidity, 'timestamp': timestamp}

def check_heartbeat():
    with db_lock:
        conn = get_db_connection()
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

        cursor.execute('''
            SELECT d.id, d.name, d.temperature, d.last_heartbeat
            FROM devices d
            JOIN sensor_data s ON d.id = s.device_id
            WHERE d.status = 'online' AND d.temperature > ?
            AND s.timestamp = (SELECT MAX(timestamp) FROM sensor_data WHERE device_id = d.id)
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

        conn.commit()
        conn.close()

    return alerts

def get_all_devices():
    with db_lock:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM devices ORDER BY id')
        devices = [dict(row) for row in cursor.fetchall()]
        conn.close()
    return devices

def get_recent_alerts(limit=20):
    with db_lock:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT a.*, d.name as device_name
            FROM alerts a
            JOIN devices d ON a.device_id = d.id
            ORDER BY a.timestamp DESC
            LIMIT ?
        ''', (limit,))
        alerts = [dict(row) for row in cursor.fetchall()]
        conn.close()
    return alerts

def get_device_status(device_id):
    with db_lock:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM devices WHERE id = ?', (device_id,))
        device = cursor.fetchone()
        conn.close()
    return dict(device) if device else None

def update_device_heartbeat(device_id):
    with db_lock:
        conn = get_db_connection()
        cursor = conn.cursor()
        timestamp = datetime.now().isoformat()
        cursor.execute('''
            UPDATE devices
            SET last_heartbeat = ?, status = 'online'
            WHERE id = ?
        ''', (timestamp, device_id))
        conn.commit()
        conn.close()
