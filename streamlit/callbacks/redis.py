import asyncio
import functools
import itertools
import operator
import threading
from typing import Tuple, Callable, Union, Optional, List, Awaitable, Pattern
from weakref import WeakValueDictionary
import aioredis
from aioredis import Redis, RedisError
from streamlit import StopException
from streamlit.callbacks.base_connection import _BaseConnection, _TimeBuffering

from streamlit.callbacks.callbacks import _wrapper, _get_loop
from streamlit.script_runner import RerunException

_redis_connections = WeakValueDictionary()
_redis_connections_lock = threading.Lock()
reconnect_time_seconds = 1.0


class _RedisConnection(_BaseConnection):
    def __init__(self, address: Tuple[str, int]):
        super().__init__(address)
        self.closed_event_wait = None
        self.add_callback_event = asyncio.Future(loop=_get_loop())
        self.add_event_wait = None

    @staticmethod
    def get_reconnect_time_seconds() -> Union[float, int]:
        return reconnect_time_seconds

    def _get_event(self, connection: Redis) -> Awaitable:
        if self.closed_event_wait is None:
            self.closed_event_wait = asyncio.ensure_future(connection.wait_closed())
        if self.add_event_wait is None:
            self.add_callback_event = asyncio.Future()
            self.add_event_wait = asyncio.ensure_future(self.add_callback_event)
            if len(self.callbacks):
                self.add_callback_event.set_result(self.callbacks[0])
        return asyncio.wait([self.add_event_wait, self.closed_event_wait], return_when=asyncio.FIRST_COMPLETED)

    def _handle_event(self, connection: Redis, event_result):
        done, pending = event_result
        for task in done:
            if task is self.closed_event_wait:
                self.closed_event_wait = None
                raise StopException("Closed event")
            if task is self.add_event_wait:
                self.add_callback_event = asyncio.Future()
                self.add_event_wait = asyncio.ensure_future(self.add_callback_event)
                tup = task.result()

                ix = next((i for i, v in enumerate(self.callbacks) if v is tup), None)
                if ix is None:
                    continue

                asyncio.ensure_future(
                    asyncio.gather(*(connection.psubscribe(i) if i.is_pattern else connection.subscribe(i)
                                     for i in map(operator.itemgetter(1), self.callbacks[ix:]))))

    def _connect(self) -> Awaitable:
        return aioredis.create_redis_pool(self.address)

    def _send_messages(self, connection: Optional[Redis], messages: List):
        if connection is None:
            for channel, message, callback in messages:
                try:
                    callback(args=[False])
                except StopException or RerunException:
                    pass
            return

        for channel, message, callback in messages:
            async def write_data(c, d, cb):
                try:
                    await connection.publish(c, d)
                except RedisError:
                    if cb is not None:
                        cb(args=[False])
                    self.at_end()
                else:
                    if cb is not None:
                        cb(args=[True])

            asyncio.ensure_future(write_data(channel, message, callback))

    def _call_callback(self, callback_struct, end_connection: bool, args: Optional = None) -> bool:
        channel, channel_obj, callback, reconnect = callback_struct
        if not reconnect and end_connection:
            return False

        try:
            if args is None:
                callback(args=[None, ''], need_report=False)
            else:
                callback(args=args)
        except StopException:
            return False
        else:
            return True

    def _callback_remove(self, connection: Optional[Redis], callback_struct):
        callback_struct[1].close()

    def _at_disconnect(self):
        self.add_callback_event.cancel()
        if self.closed_event_wait is not None and not self.closed_event_wait.done():
            self.closed_event_wait.cancel()
        if self.add_event_wait is not None and not self.add_event_wait.done():
            self.add_event_wait.cancel()
            self.add_event_wait = None

    def on_message(self,
                   channel: Union[str, Pattern],
                   callback: Callable[..., None],
                   reconnect: bool):
        pattern = not isinstance(channel, str)
        channel_obj = aioredis.Channel(channel.pattern.replace('.*', '*').replace('.?', '?') if pattern else channel,
                                       is_pattern=pattern)
        tup = channel, channel_obj, callback, reconnect

        async def message_handling():
            try:
                async for message in channel_obj.iter(decoder=lambda m: m.decode()):
                    if pattern:
                        if not self._call_callback(tup, False, [message[0].decode(), message[1]]):
                            break
                    else:
                        if not self._call_callback(tup, False, [channel, message]):
                            break
            finally:
                channel_obj.close()
                self.at_end()

        asyncio.ensure_future(message_handling())

        if self.add_event_wait is None:
            self.add_callback_event = asyncio.Future()
            self.add_event_wait = asyncio.ensure_future(self.add_callback_event)
        if not self.add_callback_event.done():
            self.add_callback_event.set_result(tup)
        self._add_callback(tup)

    def add_message(self,
                    channel: str,
                    message: str,
                    callback: Optional[Callable[[bool], None]] = None):
        self._add_message((channel, message, callback))

    def get_buffer_wrapper(self, buffer_time):
        return functools.partial(_TimeBuffering,
                                 buffer_time=buffer_time,
                                 is_null=lambda kv: kv[0][0] is None,
                                 merge=lambda l: ([list(map(tuple,
                                                            map(operator.itemgetter(0), l)))], None),
                                 at_exception=self.at_end)


def _get_redis_connection(address: Tuple[str, int]):
    _connection = _redis_connections.get(address)
    if _connection is None:
        with _redis_connections_lock:
            _connection = _redis_connections.get(address)
            if _connection is None:
                _connection = _RedisConnection(address)
                _redis_connections[address] = _connection
    return _connection


def on_message(channel: Union[str, Pattern],
               callback: Callable[[str, str], None],
               key: Optional[str] = None,
               address: Tuple[str, int] = ('localhost', 6379),
               reconnect: bool = True):
    if key is not None:
        key = 'redis.on_message_' + key

    def callback_with_empty(from_channel: Optional[str], message: str):
        if from_channel is not None:
            callback(from_channel, message)

    _get_loop().call_soon_threadsafe(_get_redis_connection(address).on_message,
                                     channel,
                                     _wrapper(callback_with_empty, key,
                                              at_end=_get_redis_connection(address).threadsafe_at_end),
                                     reconnect)


def on_message_buffered(channel: Union[str, Pattern],
                        callback: Callable[[List[Tuple[str, str]]], None],
                        buffer_time: Union[float, int],
                        key: Optional[str] = None,
                        address: Tuple[str, int] = ('localhost', 6379),
                        reconnect: bool = True):
    if key is not None:
        key = 'redis.on_message_buffered_' + key

    def callback_with_empty(from_channel: Optional[List[Tuple[str, str]]], unused_data: Optional[str] = None):
        if from_channel is not None:
            callback(from_channel)

    redis = _get_redis_connection(address)

    wrapped_fun = _wrapper(callback_with_empty, key,
                           out_wrapper=redis.get_buffer_wrapper(buffer_time),
                           at_end=redis.threadsafe_at_end)

    _get_loop().call_soon_threadsafe(redis.on_message, channel, wrapped_fun, reconnect)


def send_message(channel: str,
                 message: str,
                 callback: Optional[Callable[[bool], None]] = None,
                 address: Tuple[str, int] = ('localhost', 6379)):
    if callback is None:
        def cb(*args):
            pass
        callback = cb
    _get_loop().call_soon_threadsafe(_get_redis_connection(address).add_message, channel, message,
                                     _wrapper(callback, delegate_stop=False))
