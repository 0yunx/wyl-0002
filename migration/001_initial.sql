-- TimescaleDB 初始化脚本 - IoT 监控系统
-- 基于 PostgreSQL + TimescaleDB 扩展的时序数据库 schema
-- 迁移来源: InfluxDB (sensor_data, alerts) + SQLite (devices)

-- 启用 TimescaleDB 扩展
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- ============================================================
-- 设备元数据表 (替代 SQLite devices 表)
-- ============================================================
CREATE TABLE IF NOT EXISTS devices (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'online',
    last_heartbeat TIMESTAMPTZ,
    temperature DOUBLE PRECISION,
    humidity DOUBLE PRECISION,
    last_report TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_devices_status ON devices(status);
CREATE INDEX IF NOT EXISTS idx_devices_last_heartbeat ON devices(last_heartbeat);

-- 自动更新 updated_at 触发器函数
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS update_devices_updated_at ON devices;
CREATE TRIGGER update_devices_updated_at
    BEFORE UPDATE ON devices
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- ============================================================
-- 传感器时序数据表 (替代 InfluxDB sensor_data measurement)
-- ============================================================
CREATE TABLE IF NOT EXISTS sensor_data (
    time TIMESTAMPTZ NOT NULL,
    device_id TEXT NOT NULL,
    temperature DOUBLE PRECISION NOT NULL,
    humidity DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (time, device_id)
);

-- 创建 hypertable, 按 time 列分区, 1 天一个 chunk
SELECT create_hypertable(
    'sensor_data',
    'time',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

-- 按 device_id 创建分布式维度索引 (提高按设备查询性能)
CREATE INDEX IF NOT EXISTS idx_sensor_data_device_time
    ON sensor_data (device_id, time DESC);

-- 启用 TimescaleDB 压缩 (节省存储)
ALTER TABLE sensor_data SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'device_id',
    timescaledb.compress_orderby = 'time DESC'
);

-- 添加压缩策略: 超过 7 天的数据自动压缩
SELECT add_compression_policy(
    'sensor_data',
    INTERVAL '7 days',
    if_not_exists => TRUE
);

-- 添加数据保留策略: 30 天后自动删除
SELECT add_retention_policy(
    'sensor_data',
    INTERVAL '30 days',
    if_not_exists => TRUE
);

-- ============================================================
-- 告警时序数据表 (替代 InfluxDB alerts measurement)
-- 新增: acknowledged 字段支持告警确认功能
-- ============================================================
CREATE TABLE IF NOT EXISTS alerts (
    time TIMESTAMPTZ NOT NULL,
    device_id TEXT NOT NULL,
    alert_type TEXT NOT NULL,
    message TEXT NOT NULL,
    acknowledged BOOLEAN NOT NULL DEFAULT FALSE,
    acknowledged_at TIMESTAMPTZ,
    acknowledged_by TEXT,
    PRIMARY KEY (time, device_id, alert_type)
);

-- 创建 hypertable, 按 time 列分区, 1 天一个 chunk
SELECT create_hypertable(
    'alerts',
    'time',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

-- 复合索引: 按设备 + 确认状态 + 时间查询 (Grafana 面板用)
CREATE INDEX IF NOT EXISTS idx_alerts_device_ack_time
    ON alerts (device_id, acknowledged, time DESC);

-- 单独索引: 按确认状态快速过滤未确认告警
CREATE INDEX IF NOT EXISTS idx_alerts_acknowledged_time
    ON alerts (acknowledged, time DESC);

-- 启用压缩
ALTER TABLE alerts SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'device_id,alert_type',
    timescaledb.compress_orderby = 'time DESC'
);

-- 添加压缩策略: 超过 7 天的数据自动压缩
SELECT add_compression_policy(
    'alerts',
    INTERVAL '7 days',
    if_not_exists => TRUE
);

-- 添加数据保留策略: 90 天后自动删除 (告警保留时间更长)
SELECT add_retention_policy(
    'alerts',
    INTERVAL '90 days',
    if_not_exists => TRUE
);

-- ============================================================
-- 连续聚合视图: 传感器数据按 1 分钟聚合
-- 用于 Grafana 面板长时间范围查询优化
-- ============================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS sensor_data_1min
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 minute', time) AS bucket,
    device_id,
    AVG(temperature) AS avg_temperature,
    MAX(temperature) AS max_temperature,
    MIN(temperature) AS min_temperature,
    AVG(humidity) AS avg_humidity,
    MAX(humidity) AS max_humidity,
    MIN(humidity) AS min_humidity,
    COUNT(*) AS sample_count
FROM sensor_data
GROUP BY bucket, device_id
WITH NO DATA;

-- 添加连续聚合刷新策略: 每 5 分钟刷新一次, 刷新最近 1 小时数据
SELECT add_continuous_aggregate_policy(
    'sensor_data_1min',
    start_offset => INTERVAL '1 hour',
    end_offset => INTERVAL '1 minute',
    schedule_interval => INTERVAL '5 minutes',
    if_not_exists => TRUE
);

-- ============================================================
-- 初始化默认设备数据
-- ============================================================
INSERT INTO devices (id, name, status, last_heartbeat)
SELECT
    'sensor_' || TO_CHAR(i, 'FM00') AS id,
    '温湿度传感器 ' || i AS name,
    'online' AS status,
    NOW() AS last_heartbeat
FROM generate_series(1, 10) AS i
ON CONFLICT (id) DO NOTHING;
