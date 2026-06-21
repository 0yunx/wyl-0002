import json
import os
import threading
import queue
import logging
from typing import Optional, Callable

logger = logging.getLogger(__name__)

ALERT_CHANNEL = 'iot_monitor:alerts'


class MessageBus:
    def publish(self, channel: str, message: dict) -> None:
        raise NotImplementedError

    def subscribe(self, channel: str, callback: Callable[[dict], None]) -> None:
        raise NotImplementedError

    def unsubscribe(self, channel: str, callback: Callable[[dict], None]) -> None:
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


class RedisMessageBus(MessageBus):
    def __init__(self, redis_url: str = None):
        import redis
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
            logger.debug('Published to channel %s, %d receivers', channel, receivers)
        except Exception as e:
            logger.error('Failed to publish to Redis channel %s: %s', channel, e, exc_info=True)
            raise

    def subscribe(self, channel: str, callback: Callable[[dict], None]) -> None:
        with self._lock:
            if channel not in self._subscribers:
                self._subscribers[channel] = []
                self._pubsub.subscribe(**{channel: self._handle_message})
                logger.info('Subscribed to Redis channel %s', channel)
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
        logger.info('Redis pubsub listener started')
        try:
            for _ in self._pubsub.listen():
                pass
        except Exception as e:
            logger.error('Redis pubsub listener error: %s', e, exc_info=True)
            self._running = False
        finally:
            logger.info('Redis pubsub listener stopped')


_default_bus: Optional[MessageBus] = None
_bus_lock = threading.Lock()


def get_message_bus() -> MessageBus:
    global _default_bus
    if _default_bus is None:
        with _bus_lock:
            if _default_bus is None:
                use_redis = os.environ.get('USE_REDIS', 'false').lower() == 'true'
                if use_redis:
                    try:
                        _default_bus = RedisMessageBus()
                        logger.info('Using Redis message bus')
                    except Exception as e:
                        logger.warning('Redis connection failed, falling back to memory mode: %s', e)
                        _default_bus = InMemoryMessageBus()
                else:
                    _default_bus = InMemoryMessageBus()
                    logger.info('Using in-memory message bus')
    return _default_bus


def publish_alert(alert: dict) -> None:
    bus = get_message_bus()
    bus.publish(ALERT_CHANNEL, alert)


def subscribe_alerts(callback: Callable[[dict], None]) -> None:
    bus = get_message_bus()
    bus.subscribe(ALERT_CHANNEL, callback)


def unsubscribe_alerts(callback: Callable[[dict], None]) -> None:
    bus = get_message_bus()
    bus.unsubscribe(ALERT_CHANNEL, callback)
