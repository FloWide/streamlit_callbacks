import asyncio
import functools
import operator
import threading
from asyncio import StreamReader, StreamWriter
from typing import Callable, Union, Optional, List, Awaitable, Tuple
from weakref import WeakValueDictionary

from streamlit import StopException
from .base_connection import _BaseConnection, _TimeBuffering
from .callbacks import _get_loop, _wrapper
from streamlit.script_runner import RerunException

_s_connections = WeakValueDictionary()
_s_connections_lock = threading.Lock()
reconnect_time_seconds = 1.0


class _SocketConnection(_BaseConnection):
    @staticmethod
    def get_reconnect_time_seconds():
        return reconnect_time_seconds

    def _get_event(self, connection: Tuple[StreamReader, StreamWriter]) -> Awaitable:
        return connection[0].read(1024)

    def _handle_event(self, connection: Tuple[StreamReader, StreamWriter], data):
        if data is None:
            raise ConnectionError("Socket sent EOF")

        self._call_all_callback(connection, data)

    def _connect(self) -> Awaitable:
        return asyncio.open_connection(*self.address)

    def _send_messages(self, connection: Optional[Tuple[StreamReader, StreamWriter]], messages):
        if connection is None:
            for data, callback in messages:
                try:
                    callback(args=[False])
                except StopException or RerunException:
                    pass
            return

        for data, callback in messages:
            async def write_data(d, cb):
                try:
                    if not connection[0].at_eof():
                        connection[1].write(d if isinstance(d, bytes) else d.encode())
                        await connection[1].drain()
                        if cb is not None:
                            cb(args=[True])
                    else:
                        self.add_message(d, cb)
                except IOError:
                    if cb is not None:
                        cb(args=[False])
                    self.at_end()

            asyncio.ensure_future(write_data(data, callback))

    def _call_callback(self, callback_struct: Tuple, end_connection: bool, args=None) -> bool:
        callback, reconnect = callback_struct
        if not reconnect and end_connection:
            return False

        try:
            if args is None:
                callback(args=[args], need_report=False)
            else:
                callback(args=[args])
        except StopException:
            return False
        else:
            return True

    def _at_disconnect(self):
        pass

    def _callback_remove(self, connection: Optional, callback_struct):
        pass

    def add_callback(self, callback: Callable[..., None], reconnect: bool):
        self._add_callback((callback, reconnect))

    def add_message(self, data: Union[str, bytes], callback: Optional[Callable[[bool], None]] = None):
        self._add_message((data, callback))

    def get_buffer_wrapper(self, buffer_time):
        return functools.partial(_TimeBuffering,
                                 buffer_time=buffer_time,
                                 is_null=lambda kv: kv[0][0] is None,
                                 merge=lambda l: ([list(map(operator.itemgetter(0),
                                                            map(operator.itemgetter(0), l)))], None),
                                 at_exception=self.at_end)


def _get_s_connection(address: Tuple[str, int]):
    _connection = _s_connections.get(address)
    if _connection is None:
        with _s_connections_lock:
            _connection = _s_connections.get(address)
            if _connection is None:
                _connection = _SocketConnection(address)
                _s_connections[address] = _connection
    return _connection


def is_alive(address: Tuple[str, int]) -> bool:
    return _get_s_connection(address).alive


def on_message(address: Tuple[str, int],
               callback: Callable[[bytes], None],
               key: Optional[str] = None,
               reconnect: bool = True):
    if key is not None:
        key = 's.on_message_' + key

    def callback_with_empty(data: Union[None, bytes]):
        if data is not None:
            callback(data)

    _get_loop().call_soon_threadsafe(_get_s_connection(address).add_callback,
                                     _wrapper(callback_with_empty, key,
                                              at_end=_get_s_connection(address).threadsafe_at_end),
                                     reconnect)


def on_message_buffered(address: Tuple[str, int],
                        callback: Callable[[List[bytes]], None],
                        buffer_time: Union[float, int], key: Optional[str] = None,
                        reconnect: bool = True):
    if key is not None:
        key = 's.on_message_buffered_' + key

    def callback_with_empty(data: Optional[List[bytes]]):
        if data is not None:
            callback(data)

    sconn = _get_s_connection(address)

    wrapped_fun = _wrapper(callback_with_empty, key,
                           out_wrapper=sconn.get_buffer_wrapper(buffer_time),
                           at_end=sconn.threadsafe_at_end)

    _get_loop().call_soon_threadsafe(_get_s_connection(address).add_callback, wrapped_fun, reconnect)


def send_message(address: Tuple[str, int], message: Union[str, bytes], callback: Optional[Callable[[bool], None]] = None):
    if callback is None:
        def cb(*args):
            pass
        callback = cb
    _get_loop().call_soon_threadsafe(_get_s_connection(address).add_message, message, _wrapper(callback,
                                                                                               delegate_stop=False))
