import json
import os
import threading
import queue
from typing import Optional, Callable

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
        for cb in callbacks:
            try:
                cb(message)
            except Exception:
                pass

    def subscribe(self, channel: str, callback: Callable[[dict], None]) -> None:
        with self._lock:
            if channel not in self._subscribers:
                self._subscribers[channel] = []
            self._subscribers[channel].append(callback)

    def unsubscribe(self, channel: str, callback: Callable[[dict], None]) -> None:
        with self._lock:
            if channel in self._subscribers:
                try:
                    self._subscribers[channel].remove(callback)
                except ValueError:
                    pass


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
        self._redis.publish(channel, json.dumps(message, ensure_ascii=False))

    def subscribe(self, channel: str, callback: Callable[[dict], None]) -> None:
        with self._lock:
            if channel not in self._subscribers:
                self._subscribers[channel] = []
                self._pubsub.subscribe(**{channel: self._handle_message})
                if not self._running:
                    self._running = True
                    self._thread = threading.Thread(target=self._listen, daemon=True)
                    self._thread.start()
            self._subscribers[channel].append(callback)

    def unsubscribe(self, channel: str, callback: Callable[[dict], None]) -> None:
        with self._lock:
            if channel in self._subscribers:
                try:
                    self._subscribers[channel].remove(callback)
                except ValueError:
                    pass

    def _handle_message(self, message):
        if message['type'] == 'message':
            try:
                data = json.loads(message['data'])
            except (json.JSONDecodeError, TypeError):
                return
            channel = message['channel']
            with self._lock:
                callbacks = self._subscribers.get(channel, [])[:]
            for cb in callbacks:
                try:
                    cb(data)
                except Exception:
                    pass

    def _listen(self):
        try:
            for _ in self._pubsub.listen():
                pass
        except Exception:
            self._running = False


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
                    except Exception as e:
                        print(f'[WARN] Redis 连接失败，回退到内存模式: {e}')
                        _default_bus = InMemoryMessageBus()
                else:
                    _default_bus = InMemoryMessageBus()
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
