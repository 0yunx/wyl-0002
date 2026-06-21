import json
import os
import threading
import queue
import logging
import asyncio
from typing import Optional, Callable, Awaitable

logger = logging.getLogger(__name__)

ALERT_CHANNEL = 'iot_monitor:alerts'


class MessageBus:
    def publish(self, channel: str, message: dict) -> None:
        raise NotImplementedError

    def subscribe(self, channel: str, callback: Callable[[dict], None]) -> None:
        raise NotImplementedError

    def unsubscribe(self, channel: str, callback: Callable[[dict], None]) -> None:
        raise NotImplementedError


class AsyncMessageBus:
    async def publish(self, channel: str, message: dict) -> None:
        raise NotImplementedError

    def subscribe(self, channel: str, callback: Callable[[dict], Awaitable[None]]) -> None:
        raise NotImplementedError

    def unsubscribe(self, channel: str, callback: Callable[[dict], Awaitable[None]]) -> None:
        raise NotImplementedError


class InMemoryMessageBus(MessageBus):
    def __init__(self):
        self._subscribers: dict[str, list[Callable[[dict], None]]] = {}
        self._lock = threading.Lock()

    def publish(self, channel: str, message: dict) -> None:
        with self._lock:
            callbacks = self._subscribers.get(channel, [])[:]

        logger.debug('Publishing to channel %s, %d subscribers', channel, len(callbacks))

        for cb in callbacks:
            try:
                cb(message)
            except queue.Full:
                logger.info('Subscriber queue full, dropping message for channel %s', channel)
            except Exception as e:
                logger.error('Error in subscriber callback for channel %s: %s', channel, e, exc_info=True)

    def subscribe(self, channel: str, callback: Callable[[dict], None]) -> None:
        with self._lock:
            if channel not in self._subscribers:
                self._subscribers[channel] = []
            self._subscribers[channel].append(callback)
        logger.info('Subscribed to channel %s, total subscribers: %d', channel, len(self._subscribers[channel]))

    def unsubscribe(self, channel: str, callback: Callable[[dict], None]) -> None:
        with self._lock:
            if channel in self._subscribers:
                try:
                    self._subscribers[channel].remove(callback)
                    logger.info('Unsubscribed from channel %s, remaining subscribers: %d',
                                channel, len(self._subscribers[channel]))
                except ValueError:
                    logger.warning('Attempted to unsubscribe unknown callback from channel %s', channel)


def _parse_cluster_nodes(nodes_str: str):
    nodes = []
    for node in nodes_str.split(','):
        node = node.strip()
        if not node:
            continue
        if '://' in node:
            from urllib.parse import urlparse
            p = urlparse(node)
            nodes.append({'host': p.hostname, 'port': p.port or 6379})
        else:
            parts = node.split(':')
            nodes.append({'host': parts[0], 'port': int(parts[1]) if len(parts) > 1 else 6379})
    return nodes


class RedisMessageBus(MessageBus):
    def __init__(self, redis_url: str = None, cluster_nodes: str = None):
        import redis
        self._cluster_mode = False
        if cluster_nodes is None:
            cluster_nodes = os.environ.get('REDIS_CLUSTER_NODES', '')
        if cluster_nodes:
            try:
                from redis.cluster import RedisCluster
                startup_nodes = _parse_cluster_nodes(cluster_nodes)
                self._redis = RedisCluster(
                    startup_nodes=startup_nodes,
                    decode_responses=True,
                    password=os.environ.get('REDIS_PASSWORD', None),
                )
                self._cluster_mode = True
                logger.info('Redis 集群模式已启用, 节点数: %d', len(startup_nodes))
            except Exception as e:
                logger.warning('Redis 集群初始化失败, 回退到单机模式: %s', e)
                self._cluster_mode = False
                if redis_url is None:
                    redis_url = os.environ.get('REDIS_URL', 'redis://localhost:6379/0')
                self._redis = redis.Redis.from_url(redis_url, decode_responses=True)
        else:
            if redis_url is None:
                redis_url = os.environ.get('REDIS_URL', 'redis://localhost:6379/0')
            self._redis = redis.Redis.from_url(redis_url, decode_responses=True)

        self._pubsub = self._redis.pubsub()
        self._thread: Optional[threading.Thread] = None
        self._subscribers: dict[str, list[Callable[[dict], None]]] = {}
        self._lock = threading.Lock()
        self._running = False

    def publish(self, channel: str, message: dict) -> None:
        try:
            payload = json.dumps(message, ensure_ascii=False)
            receivers = self._redis.publish(channel, payload)
            logger.debug('Published to channel %s, %d receivers (cluster=%s)',
                         channel, receivers, self._cluster_mode)
        except Exception as e:
            logger.error('Failed to publish to Redis channel %s: %s', channel, e, exc_info=True)
            raise

    def subscribe(self, channel: str, callback: Callable[[dict], None]) -> None:
        with self._lock:
            if channel not in self._subscribers:
                self._subscribers[channel] = []
                self._pubsub.subscribe(**{channel: self._handle_message})
                logger.info('Subscribed to Redis channel %s (cluster=%s)', channel, self._cluster_mode)
                if not self._running:
                    self._running = True
                    self._thread = threading.Thread(target=self._listen, daemon=True, name='redis-pubsub')
                    self._thread.start()
                    logger.info('Redis pubsub listener thread started')
            self._subscribers[channel].append(callback)
            logger.info('Added callback to channel %s, total: %d', channel, len(self._subscribers[channel]))

    def unsubscribe(self, channel: str, callback: Callable[[dict], None]) -> None:
        with self._lock:
            if channel in self._subscribers:
                try:
                    self._subscribers[channel].remove(callback)
                    logger.info('Removed callback from channel %s, remaining: %d',
                                channel, len(self._subscribers[channel]))
                except ValueError:
                    logger.warning('Attempted to unsubscribe unknown callback from channel %s', channel)

    def _handle_message(self, message):
        if message['type'] != 'message':
            return

        channel = message['channel']
        try:
            data = json.loads(message['data'])
        except (json.JSONDecodeError, TypeError) as e:
            logger.error('Failed to parse message from channel %s: %s', channel, e)
            return

        with self._lock:
            callbacks = self._subscribers.get(channel, [])[:]

        logger.debug('Received message on channel %s, dispatching to %d subscribers', channel, len(callbacks))

        for cb in callbacks:
            try:
                cb(data)
            except queue.Full:
                logger.info('Subscriber queue full, dropping message from channel %s', channel)
            except Exception as e:
                logger.error('Error in subscriber callback for channel %s: %s', channel, e, exc_info=True)

    def _listen(self):
        logger.info('Redis pubsub listener started (cluster=%s)', self._cluster_mode)
        try:
            for _ in self._pubsub.listen():
                pass
        except Exception as e:
            logger.error('Redis pubsub listener error: %s', e, exc_info=True)
            self._running = False
        finally:
            logger.info('Redis pubsub listener stopped')


class AsyncInMemoryMessageBus(AsyncMessageBus):
    def __init__(self):
        self._subscribers: dict[str, list[Callable[[dict], Awaitable[None]]]] = {}
        self._lock = asyncio.Lock()

    async def publish(self, channel: str, message: dict) -> None:
        async with self._lock:
            callbacks = list(self._subscribers.get(channel, []))

        logger.debug('Async publish to channel %s, %d subscribers', channel, len(callbacks))

        for cb in callbacks:
            try:
                await cb(message)
            except Exception as e:
                logger.error('Async subscriber callback error channel %s: %s', channel, e, exc_info=True)

    async def subscribe(self, channel: str, callback: Callable[[dict], Awaitable[None]]) -> None:
        async with self._lock:
            if channel not in self._subscribers:
                self._subscribers[channel] = []
            self._subscribers[channel].append(callback)
        logger.info('Async subscribed to channel %s, total subscribers: %d',
                    channel, len(self._subscribers[channel]))

    async def unsubscribe(self, channel: str, callback: Callable[[dict], Awaitable[None]]) -> None:
        async with self._lock:
            if channel in self._subscribers:
                try:
                    self._subscribers[channel].remove(callback)
                    logger.info('Async unsubscribed from channel %s, remaining subscribers: %d',
                                channel, len(self._subscribers[channel]))
                except ValueError:
                    logger.warning('Async attempt to unsubscribe unknown callback from channel %s', channel)


class AsyncRedisMessageBus(AsyncMessageBus):
    def __init__(self, redis_url: str = None, cluster_nodes: str = None):
        import redis.asyncio as aioredis
        self._cluster_mode = False
        if cluster_nodes is None:
            cluster_nodes = os.environ.get('REDIS_CLUSTER_NODES', '')
        if cluster_nodes:
            try:
                from redis.asyncio.cluster import RedisCluster
                startup_nodes = _parse_cluster_nodes(cluster_nodes)
                self._redis = RedisCluster(
                    startup_nodes=startup_nodes,
                    decode_responses=True,
                    password=os.environ.get('REDIS_PASSWORD', None),
                )
                self._cluster_mode = True
                logger.info('Async Redis 集群模式已启用, 节点数: %d', len(startup_nodes))
            except Exception as e:
                logger.warning('Async Redis 集群初始化失败, 回退到单机模式: %s', e)
                self._cluster_mode = False
                if redis_url is None:
                    redis_url = os.environ.get('REDIS_URL', 'redis://localhost:6379/0')
                self._redis = aioredis.Redis.from_url(redis_url, decode_responses=True)
        else:
            if redis_url is None:
                redis_url = os.environ.get('REDIS_URL', 'redis://localhost:6379/0')
            self._redis = aioredis.Redis.from_url(redis_url, decode_responses=True)

        self._pubsub = self._redis.pubsub()
        self._task: Optional[asyncio.Task] = None
        self._subscribers: dict[str, list[Callable[[dict], Awaitable[None]]]] = {}
        self._lock = asyncio.Lock()

    async def publish(self, channel: str, message: dict) -> None:
        try:
            payload = json.dumps(message, ensure_ascii=False)
            receivers = await self._redis.publish(channel, payload)
            logger.debug('Async published to channel %s, %d receivers (cluster=%s)',
                         channel, receivers, self._cluster_mode)
        except Exception as e:
            logger.error('Failed async publish to Redis channel %s: %s', channel, e, exc_info=True)
            raise

    async def subscribe(self, channel: str, callback: Callable[[dict], Awaitable[None]]) -> None:
        async with self._lock:
            if channel not in self._subscribers:
                self._subscribers[channel] = []
                await self._pubsub.subscribe(**{channel: self._handle_message})
                logger.info('Async subscribed to Redis channel %s (cluster=%s)', channel, self._cluster_mode)
                if self._task is None or self._task.done():
                    self._task = asyncio.create_task(self._listen(), name='async-redis-pubsub')
                    logger.info('Async Redis pubsub listener task started')
            self._subscribers[channel].append(callback)
            logger.info('Async added callback to channel %s, total: %d',
                        channel, len(self._subscribers[channel]))

    async def unsubscribe(self, channel: str, callback: Callable[[dict], Awaitable[None]]) -> None:
        async with self._lock:
            if channel in self._subscribers:
                try:
                    self._subscribers[channel].remove(callback)
                    logger.info('Async removed callback from channel %s, remaining: %d',
                                channel, len(self._subscribers[channel]))
                except ValueError:
                    logger.warning('Async attempt to unsubscribe unknown callback from channel %s', channel)

    async def _handle_message(self, message):
        if message['type'] != 'message':
            return

        channel = message['channel']
        try:
            data = json.loads(message['data'])
        except (json.JSONDecodeError, TypeError) as e:
            logger.error('Failed async parse message from channel %s: %s', channel, e)
            return

        async with self._lock:
            callbacks = list(self._subscribers.get(channel, []))

        logger.debug('Async received message on channel %s, dispatching to %d subscribers',
                     channel, len(callbacks))

        for cb in callbacks:
            try:
                await cb(data)
            except Exception as e:
                logger.error('Async subscriber callback error channel %s: %s', channel, e, exc_info=True)

    async def _listen(self):
        logger.info('Async Redis pubsub listener started (cluster=%s)', self._cluster_mode)
        try:
            async for _ in self._pubsub.listen():
                pass
        except asyncio.CancelledError:
            logger.info('Async Redis pubsub listener cancelled')
        except Exception as e:
            logger.error('Async Redis pubsub listener error: %s', e, exc_info=True)
        finally:
            logger.info('Async Redis pubsub listener stopped')

    async def close(self):
        if self._task is not None and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        try:
            await self._pubsub.close()
        except Exception:
            pass
        try:
            await self._redis.close()
        except Exception:
            pass
        logger.info('Async Redis message bus closed')


_default_bus: Optional[MessageBus] = None
_bus_lock = threading.Lock()

_async_default_bus: Optional[AsyncMessageBus] = None
_async_bus_lock = threading.Lock()


def get_message_bus() -> MessageBus:
    global _default_bus
    if _default_bus is None:
        with _bus_lock:
            if _default_bus is None:
                use_redis = os.environ.get('USE_REDIS', 'false').lower() == 'true'
                if use_redis:
                    try:
                        _default_bus = RedisMessageBus()
                        logger.info('Using Redis message bus (sync)')
                    except Exception as e:
                        logger.warning('Redis connection failed, falling back to memory mode: %s', e)
                        _default_bus = InMemoryMessageBus()
                else:
                    _default_bus = InMemoryMessageBus()
                    logger.info('Using in-memory message bus (sync)')
    return _default_bus


def get_async_message_bus() -> AsyncMessageBus:
    global _async_default_bus
    if _async_default_bus is None:
        with _async_bus_lock:
            if _async_default_bus is None:
                use_redis = os.environ.get('USE_REDIS', 'false').lower() == 'true'
                if use_redis:
                    try:
                        _async_default_bus = AsyncRedisMessageBus()
                        logger.info('Using Redis message bus (async)')
                    except Exception as e:
                        logger.warning('Async Redis connection failed, falling back to memory mode: %s', e)
                        _async_default_bus = AsyncInMemoryMessageBus()
                else:
                    _async_default_bus = AsyncInMemoryMessageBus()
                    logger.info('Using in-memory message bus (async)')
    return _async_default_bus


async def close_async_message_bus():
    global _async_default_bus
    if isinstance(_async_default_bus, AsyncRedisMessageBus):
        await _async_default_bus.close()
    _async_default_bus = None
    logger.info('Async message bus reset')


def publish_alert(alert: dict) -> None:
    bus = get_message_bus()
    bus.publish(ALERT_CHANNEL, alert)


async def publish_alert_async(alert: dict) -> None:
    bus = get_async_message_bus()
    await bus.publish(ALERT_CHANNEL, alert)


def subscribe_alerts(callback: Callable[[dict], None]) -> None:
    bus = get_message_bus()
    bus.subscribe(ALERT_CHANNEL, callback)


async def subscribe_alerts_async(callback: Callable[[dict], Awaitable[None]]) -> None:
    bus = get_async_message_bus()
    await bus.subscribe(ALERT_CHANNEL, callback)


def unsubscribe_alerts(callback: Callable[[dict], None]) -> None:
    bus = get_message_bus()
    bus.unsubscribe(ALERT_CHANNEL, callback)


async def unsubscribe_alerts_async(callback: Callable[[dict], Awaitable[None]]) -> None:
    bus = get_async_message_bus()
    await bus.unsubscribe(ALERT_CHANNEL, callback)
