import asyncio
import functools
import operator
import threading
from typing import Callable, Union, Optional, List, Awaitable, Tuple
from weakref import WeakValueDictionary

from streamlit import StopException
from .base_connection import _BaseConnection, _TimeBuffering
from .callbacks import _get_loop, _wrapper
from streamlit.script_runner import RerunException
from tornado.websocket import websocket_connect, WebSocketClosedError, WebSocketClientConnection

_ws_connections = WeakValueDictionary()
_ws_connections_lock = threading.Lock()
reconnect_time_seconds = 1.0


class _WebsocketConnection(_BaseConnection):
    @staticmethod
    def get_reconnect_time_seconds():
        return reconnect_time_seconds

    def _get_event(self, connection) -> Awaitable:
        return connection.read_message()

    def _handle_event(self, connection, data):
        if data is None:
            raise ConnectionError("Websocket sent EOF")

        self._call_all_callback(connection, data)

    def _connect(self) -> Awaitable:
        return websocket_connect(url=self.address)

    def _send_messages(self, connection: WebSocketClientConnection, messages):
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
                    if connection.protocol is not None:
                        await connection.write_message(d, isinstance(d, bytes))
                        if cb is not None:
                            cb(args=[True])
                    else:
                        self.add_message(d, cb)
                except WebSocketClosedError:
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


def _get_ws_connection(url: str):
    _connection = _ws_connections.get(url)
    if _connection is None:
        with _ws_connections_lock:
            _connection = _ws_connections.get(url)
            if _connection is None:
                _connection = _WebsocketConnection(url)
                _ws_connections[url] = _connection
    return _connection


def is_alive(url: str) -> bool:
    return _get_ws_connection(url).alive


def on_message(url: str, callback: Callable[[Union[str, bytes]], None], key: Optional[str] = None,
               reconnect: bool = True):
    if key is not None:
        key = 'ws.on_message_' + key

    def callback_with_empty(data: Union[None, str, bytes]):
        if data is not None:
            callback(data)

    _get_loop().call_soon_threadsafe(_get_ws_connection(url).add_callback,
                                     _wrapper(callback_with_empty, key,
                                              at_end=_get_ws_connection(url).threadsafe_at_end),
                                     reconnect)


def on_message_buffered(url: str, callback: Callable[[List[Union[str, bytes]]], None],
                        buffer_time: Union[float, int], key: Optional[str] = None,
                        reconnect: bool = True):
    if key is not None:
        key = 'ws.on_message_buffered_' + key

    def callback_with_empty(data: Optional[List[Union[str, bytes]]]):
        if data is not None:
            callback(data)

    wsconn = _get_ws_connection(url)

    wrapped_fun = _wrapper(callback_with_empty, key,
                           out_wrapper=wsconn.get_buffer_wrapper(buffer_time),
                           at_end=wsconn.threadsafe_at_end)

    _get_loop().call_soon_threadsafe(_get_ws_connection(url).add_callback, wrapped_fun, reconnect)


def send_message(url: str, message: Union[str, bytes], callback: Optional[Callable[[bool], None]] = None):
    if callback is None:
        def cb(*args):
            pass
        callback = cb
    _get_loop().call_soon_threadsafe(_get_ws_connection(url).add_message, message, _wrapper(callback,
                                                                                            delegate_stop=False))
