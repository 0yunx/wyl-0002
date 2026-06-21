import os
import logging
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

HEARTBEAT_TIMEOUT = int(os.environ.get('HEARTBEAT_TIMEOUT', '30'))

logger = logging.getLogger(__name__)

INFLUXDB_URL = os.environ.get('INFLUXDB_URL', 'http://localhost:8086')
INFLUXDB_TOKEN = os.environ.get('INFLUXDB_TOKEN', '')
INFLUXDB_ORG = os.environ.get('INFLUXDB_ORG', 'iot')
INFLUXDB_BUCKET = os.environ.get('INFLUXDB_BUCKET', 'iot_monitor')

MEASUREMENT_SENSOR = 'sensor_data'
MEASUREMENT_ALERT = 'alerts'

_client: Optional[InfluxDBClient] = None
_write_api = None
_query_api = None


def get_client() -> InfluxDBClient:
    global _client, _write_api, _query_api
    if _client is None:
        _client = InfluxDBClient(
            url=INFLUXDB_URL,
            token=INFLUXDB_TOKEN,
            org=INFLUXDB_ORG
        )
        _write_api = _client.write_api(write_options=SYNCHRONOUS)
        _query_api = _client.query_api()
        logger.info('InfluxDB client initialized: url=%s, org=%s, bucket=%s',
                    INFLUXDB_URL, INFLUXDB_ORG, INFLUXDB_BUCKET)
    return _client


def ensure_bucket() -> None:
    try:
        client = get_client()
        buckets_api = client.buckets_api()
        existing = buckets_api.find_bucket_by_name(INFLUXDB_BUCKET)
        if existing is None:
            orgs_api = client.organizations_api()
            orgs = orgs_api.find_organizations()
            org_id = None
            for org in orgs:
                if org.name == INFLUXDB_ORG:
                    org_id = org.id
                    break
            if org_id is None and orgs:
                org_id = orgs[0].id
            if org_id:
                from influxdb_client.domain import BucketRetentionRules
                retention_rules = BucketRetentionRules(
                    type='expire',
                    every_seconds=7 * 24 * 3600
                )
                buckets_api.create_bucket(
                    bucket_name=INFLUXDB_BUCKET,
                    org_id=org_id,
                    retention_rules=[retention_rules]
                )
                logger.info('Created InfluxDB bucket: %s', INFLUXDB_BUCKET)
            else:
                logger.warning('Could not create bucket: no org found')
        else:
            logger.debug('InfluxDB bucket already exists: %s', INFLUXDB_BUCKET)
    except Exception as e:
        logger.warning('Failed to ensure InfluxDB bucket (may already exist): %s', e)


def init_influx() -> None:
    get_client()
    ensure_bucket()


def write_sensor_data(device_id: str, temperature: float, humidity: float,
                      timestamp: Optional[datetime] = None) -> None:
    if timestamp is None:
        timestamp = datetime.utcnow()
    point = (
        Point(MEASUREMENT_SENSOR)
        .tag('device_id', device_id)
        .field('temperature', float(temperature))
        .field('humidity', float(humidity))
        .time(timestamp, WritePrecision.NS)
    )
    try:
        get_client()
        _write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)
        logger.debug('Wrote sensor data to InfluxDB: %s temp=%.2f humidity=%.2f',
                     device_id, temperature, humidity)
    except Exception as e:
        logger.error('Failed to write sensor data to InfluxDB: %s', e, exc_info=True)
        raise


def write_alert(device_id: str, alert_type: str, message: str,
                timestamp: Optional[datetime] = None) -> None:
    if timestamp is None:
        timestamp = datetime.utcnow()
    point = (
        Point(MEASUREMENT_ALERT)
        .tag('device_id', device_id)
        .tag('alert_type', alert_type)
        .tag('acknowledged', 'false')
        .field('message', str(message))
        .field('acknowledged', 0)
        .time(timestamp, WritePrecision.NS)
    )
    try:
        get_client()
        _write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)
        logger.debug('Wrote alert to InfluxDB: %s type=%s', device_id, alert_type)
    except Exception as e:
        logger.error('Failed to write alert to InfluxDB: %s', e, exc_info=True)
        raise


def query_recent_sensor_data(device_id: Optional[str] = None,
                             minutes: int = 5) -> List[Dict[str, Any]]:
    client = get_client()
    stop = datetime.utcnow()
    start = stop - timedelta(minutes=minutes)

    flux_query = f'''
    from(bucket: "{INFLUXDB_BUCKET}")
      |> range(start: {int(start.timestamp())}, stop: {int(stop.timestamp())})
      |> filter(fn: (r) => r["_measurement"] == "{MEASUREMENT_SENSOR}")
    '''
    if device_id:
        flux_query += f'  |> filter(fn: (r) => r["device_id"] == "{device_id}")\n'
    flux_query += '''
      |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
      |> sort(columns: ["_time"], desc: false)
    '''

    try:
        result = _query_api.query(query=flux_query, org=INFLUXDB_ORG)
        records = []
        for table in result:
            for record in table.records:
                records.append({
                    'device_id': record.values.get('device_id'),
                    'temperature': record.values.get('temperature'),
                    'humidity': record.values.get('humidity'),
                    'timestamp': record.get_time().isoformat()
                })
        return records
    except Exception as e:
        logger.error('Failed to query recent sensor data: %s', e, exc_info=True)
        return []


def query_latest_sensor_data(device_id: Optional[str] = None) -> Dict[str, Dict[str, Any]]:
    client = get_client()
    stop = datetime.utcnow()
    start = stop - timedelta(minutes=max(5, HEARTBEAT_TIMEOUT * 2))

    flux_query = f'''
    from(bucket: "{INFLUXDB_BUCKET}")
      |> range(start: {int(start.timestamp())}, stop: {int(stop.timestamp())})
      |> filter(fn: (r) => r["_measurement"] == "{MEASUREMENT_SENSOR}")
    '''
    if device_id:
        flux_query += f'  |> filter(fn: (r) => r["device_id"] == "{device_id}")\n'
    flux_query += f'''
      |> group(columns: ["device_id"])
      |> last()
      |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
      |> keep(columns: ["device_id", "temperature", "humidity", "_time"])
    '''

    try:
        result = _query_api.query(query=flux_query, org=INFLUXDB_ORG)
        latest = {}
        for table in result:
            for record in table.records:
                dev_id = record.values.get('device_id')
                if dev_id:
                    latest[dev_id] = {
                        'temperature': record.values.get('temperature'),
                        'humidity': record.values.get('humidity'),
                        'timestamp': record.get_time().isoformat(),
                        'last_report': record.get_time().isoformat(),
                        'last_heartbeat': record.get_time().isoformat()
                    }
        return latest
    except Exception as e:
        logger.error('Failed to query latest sensor data: %s', e, exc_info=True)
        return {}


def query_recent_alerts(limit: int = 20) -> List[Dict[str, Any]]:
    client = get_client()
    stop = datetime.utcnow()
    start = stop - timedelta(hours=24)

    flux_query = f'''
    from(bucket: "{INFLUXDB_BUCKET}")
      |> range(start: {int(start.timestamp())}, stop: {int(stop.timestamp())})
      |> filter(fn: (r) => r["_measurement"] == "{MEASUREMENT_ALERT}")
      |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
      |> sort(columns: ["_time"], desc: true)
      |> limit(n: {limit})
    '''

    try:
        result = _query_api.query(query=flux_query, org=INFLUXDB_ORG)
        alerts = []
        for table in result:
            for record in table.records:
                alerts.append({
                    'device_id': record.values.get('device_id'),
                    'alert_type': record.values.get('alert_type'),
                    'message': record.values.get('message'),
                    'timestamp': record.get_time().isoformat(),
                    'acknowledged': record.values.get('acknowledged', 0)
                })
        return alerts
    except Exception as e:
        logger.error('Failed to query recent alerts: %s', e, exc_info=True)
        return []


def query_device_ids_with_active_alerts(alert_type: Optional[str] = None) -> set:
    client = get_client()
    stop = datetime.utcnow()
    start = stop - timedelta(hours=1)

    flux_query = f'''
    from(bucket: "{INFLUXDB_BUCKET}")
      |> range(start: {int(start.timestamp())}, stop: {int(stop.timestamp())})
      |> filter(fn: (r) => r["_measurement"] == "{MEASUREMENT_ALERT}")
      |> filter(fn: (r) => r["acknowledged"] == "false")
    '''
    if alert_type:
        flux_query += f'  |> filter(fn: (r) => r["alert_type"] == "{alert_type}")\n'
    flux_query += '''
      |> keep(columns: ["device_id"])
      |> distinct(column: "device_id")
    '''

    try:
        result = _query_api.query(query=flux_query, org=INFLUXDB_ORG)
        device_ids = set()
        for table in result:
            for record in table.records:
                dev_id = record.values.get('device_id')
                if dev_id:
                    device_ids.add(dev_id)
        return device_ids
    except Exception as e:
        logger.error('Failed to query device IDs with active alerts: %s', e, exc_info=True)
        return set()


def close_influx() -> None:
    global _client, _write_api, _query_api
    if _client is not None:
        try:
            if _write_api is not None:
                _write_api.close()
            _client.close()
            logger.info('InfluxDB client closed')
        except Exception as e:
            logger.warning('Error closing InfluxDB client: %s', e)
        finally:
            _client = None
            _write_api = None
            _query_api = None
