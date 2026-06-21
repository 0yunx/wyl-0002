# IoT 设备心跳监控网关

轻量级 IoT 设备心跳监控系统，模拟 10 台温湿度传感器，支持实时监控、离线检测和阈值告警。

## 功能特性

- **设备模拟**: 10 台温湿度传感器每 3 秒自动上报数据
- **心跳检测**: 超过 30 秒无心跳自动标记设备离线
- **阈值告警**: 温度超过 35°C 自动触发告警
- **实时推送**: 通过 SSE（Server-Sent Events）实时推送告警到前端
- **可视化面板**: 设备状态卡片 + 告警流实时展示
- **交互控制**: 支持手动断开/恢复传感器、调节温度
- **水平扩展**: 支持 Gunicorn 多 worker + Redis Pub/Sub 跨进程广播

## 技术栈

- **后端**: Flask + APScheduler + SQLite
- **消息总线**: 内存模式（单进程） / Redis Pub/Sub（多进程）
- **部署**: Gunicorn + 多 worker
- **前端**: 原生 JavaScript (无框架)
- **通信**: SSE 实时推送

## 文件结构

```
wyl-0002/
├── app.py              # Flask 路由与 SSE 推送
├── scheduler.py        # 定时任务与传感器模拟
├── models.py           # 数据层 (SQLite)
├── message_bus.py      # 消息总线抽象（内存/Redis）
├── wsgi.py             # WSGI 入口（Gunicorn 用）
├── gunicorn_conf.py    # Gunicorn 配置
├── requirements.txt    # 顶层依赖
├── requirements.lock   # 完整依赖锁定（生产部署用）
├── templates/
│   └── index.html      # 前端单页面
└── static/             # 静态资源目录
```

## 架构设计

### 消息总线抽象

为了解决 Gunicorn 多 worker 下 SSE 广播失效的问题，使用了消息总线抽象层：

```
┌─────────────────┐     publish     ┌─────────────────┐     subscribe     ┌─────────────────┐
│  调度器产生告警  │ ──────────────> │   消息总线      │ <────────────── │  SSE 连接 (Worker 1)
└─────────────────┘                  │  (Memory/Redis) │                  └─────────────────┘
                                      └─────────────────┘
                                               ▲
                                               │ subscribe
                                               ▼
                                      ┌─────────────────┐
                                      │ SSE 连接 (Worker N)│
                                      └─────────────────┘
```

- **内存模式** (`InMemoryMessageBus`)：单进程开发用，线程安全
- **Redis 模式** (`RedisMessageBus`)：多进程生产用，跨 worker 广播

### 调度器单例

多 worker 环境下，调度器只在主进程（或指定 worker）启动，避免重复执行定时任务。

## 快速开始

### 1. 安装依赖

开发环境：
```bash
pip install -r requirements.txt
```

生产环境（推荐）：
```bash
pip install -r requirements.lock
```

### 2. 开发模式启动（单进程）

```bash
python app.py
```

### 3. 生产模式启动（多 worker + Redis）

前置条件：安装并启动 Redis

```bash
# Windows (PowerShell)
$env:USE_REDIS="true"
$env:REDIS_URL="redis://localhost:6379/0"
gunicorn -c gunicorn_conf.py wsgi:application

# Linux/Mac
USE_REDIS=true REDIS_URL=redis://localhost:6379/0 gunicorn -c gunicorn_conf.py wsgi:application
```

### 4. 访问监控面板

打开浏览器访问: http://localhost:5000

## 环境变量

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `USE_REDIS` | `false` | 是否启用 Redis 消息总线，多 worker 必须设为 `true` |
| `REDIS_URL` | `redis://localhost:6379/0` | Redis 连接地址 |
| `SCHEDULER_MODE` | `auto` | 调度器启动模式: `always` / `never` / `auto` |
| `GUNICORN_WORKERS` | CPU*2+1 | Gunicorn worker 数量 |

## 验证步骤

### 场景 1: 查看设备卡片

- 启动服务后访问根路径
- 页面应显示 10 张设备卡片，每张卡片显示温度、湿度和状态
- 所有设备初始状态为"在线"（绿色边框）

### 场景 2: 离线告警测试

1. 点击任意设备卡片上的「断开」按钮
2. 等待约 30 秒（心跳超时时间）
3. 该设备卡片会变为红色（离线状态）
4. 右侧告警流会出现一条离线告警记录

### 场景 3: 温度阈值告警测试

1. 点击任意在线设备卡片上的「升温」按钮
2. 等待最多 3 秒（数据上报周期）
3. 该设备卡片会变为橙色（告警状态）
4. 温度数值会闪烁显示
5. 右侧告警流会出现一条温度过高告警记录

## API 接口

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/` | 前端页面 |
| GET | `/api/devices` | 获取所有设备状态 |
| GET | `/api/alerts` | 获取最近告警 |
| GET | `/api/config` | 获取系统配置 |
| GET | `/events` | SSE 事件流 |
| POST | `/api/sensor/{id}/disable` | 禁用传感器模拟 |
| POST | `/api/sensor/{id}/enable` | 启用传感器模拟 |
| POST | `/api/sensor/{id}/boost` | 启用/禁用温度提升 |

## 配置参数

可在 `models.py` 中调整:

- `TEMPERATURE_THRESHOLD`: 温度告警阈值（默认 35°C）
- `HEARTBEAT_TIMEOUT`: 心跳超时时间（默认 30 秒）
- `DB_PATH`: SQLite 数据库文件路径

## 定时任务

在 `scheduler.py` 中配置:

- **传感器数据模拟**: 每 3 秒执行一次
- **心跳检测**: 每秒执行一次

## 数据库表结构

### devices 设备表

| 字段 | 类型 | 说明 |
|------|------|------|
| id | TEXT | 设备ID |
| name | TEXT | 设备名称 |
| status | TEXT | 状态 (online/offline) |
| last_heartbeat | DATETIME | 最后心跳时间 |
| temperature | REAL | 最新温度 |
| humidity | REAL | 最新湿度 |
| last_report | DATETIME | 最后上报时间 |

### sensor_data 数据表

| 字段 | 类型 | 说明 |
|------|------|------|
| id | INTEGER | 主键 |
| device_id | TEXT | 设备ID |
| temperature | REAL | 温度 |
| humidity | REAL | 湿度 |
| timestamp | DATETIME | 上报时间 |

### alerts 告警表

| 字段 | 类型 | 说明 |
|------|------|------|
| id | INTEGER | 主键 |
| device_id | TEXT | 设备ID |
| alert_type | TEXT | 告警类型 (offline/temperature) |
| message | TEXT | 告警消息 |
| timestamp | DATETIME | 告警时间 |
| acknowledged | INTEGER | 是否已确认 |

## 依赖管理

- `requirements.txt`: 顶层直接依赖，版本号明确
- `requirements.lock`: 完整依赖锁定文件，包含所有传递依赖的精确版本

生产环境部署务必使用 `requirements.lock` 确保可复现：

```bash
pip install -r requirements.lock
```

更新 lock 文件：

```bash
pip install -r requirements.txt
pip freeze > requirements.lock
```

## 停止服务

按 `Ctrl+C` 停止服务，定时任务会自动优雅关闭。
