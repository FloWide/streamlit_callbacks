import asyncio
import threading
from typing import Callable, Union, Optional, List
from weakref import WeakValueDictionary

from streamlit import StopException
from streamlit.callbacks.callbacks import _get_loop, _wrapper
from tornado.websocket import WebSocketClientConnection, websocket_connect, WebSocketClosedError

_ws_connections = WeakValueDictionary()
_ws_connections_lock = threading.Lock()
reconnect_time_seconds = 1.0


class _WebsocketConnection:
    def __init__(self, url):
        self.url = url
        self.callbacks = []
        self.callbacks_empty = asyncio.Event(loop=_get_loop())
        self.output_msg = []
        self.output_msg_has_element = asyncio.Event(loop=_get_loop())
        self.task = None
        self.alive = False

    async def _async_connect(self):
        empty = asyncio.ensure_future(self.callbacks_empty.wait())
        out_msg = asyncio.ensure_future(self.output_msg_has_element.wait())
        while len(self.callbacks) or len(self.output_msg):
            connection = None
            try:
                connection = await websocket_connect(url=self.url)
                self.alive = True
                read_msg = asyncio.ensure_future(connection.read_message())
                while len(self.callbacks) or len(self.output_msg):
                    done, pend = await asyncio.wait([read_msg, out_msg, empty], return_when=asyncio.FIRST_COMPLETED)
                    for task in done:
                        if task is read_msg:
                            try:
                                data = task.result()
                            except:
                                import traceback
                                traceback.print_exc()
                            else:
                                if data is None:
                                    raise ConnectionError("Websocket sent EOF")

                                self._run_callbacks(data)
                            read_msg = asyncio.ensure_future(connection.read_message())
                        if task is out_msg:
                            self._send_messages(connection)
                            self.output_msg_has_element.clear()
                            out_msg = asyncio.ensure_future(self.output_msg_has_element.wait())
                        if task is empty:
                            if len(self.callbacks) > 0:
                                self.callbacks_empty.clear()
                                empty = asyncio.ensure_future(self.callbacks_empty.wait())
                connection.close()
            except BaseException as e:
                if connection is not None:
                    connection.close()
                print(f"Websocket '{self.url}' ends: {e.__class__.__name__} {e}, try to reconnect")
                self._check_validity()
                if len(self.callbacks) or len(self.output_msg):
                    await asyncio.sleep(reconnect_time_seconds)
            finally:
                self.alive = False

        self.callbacks_empty.clear()
        self.output_msg_has_element.clear()

        if not empty.done():
            empty.cancel()
        if not out_msg.done():
            out_msg.cancel()

        self.task = None

    def _send_messages(self, connection: WebSocketClientConnection):
        for data, callback in self.output_msg:
            async def write_data(d, cb):
                try:
                    await connection.write_message(d, isinstance(d, bytes))
                except WebSocketClosedError:
                    if cb is not None:
                        cb(args=[False])
                    self._check_validity(end_connection=False)
                else:
                    if cb is not None:
                        cb(args=[True])

            asyncio.ensure_future(write_data(data, callback))
        self.output_msg.clear()

    def _check_validity(self, end_connection: bool = True):
        index = 0
        while index < len(self.callbacks):
            (callback, reconnect) = self.callbacks[index]
            if end_connection and not reconnect:
                del self.callbacks[index]
                continue

            try:
                callback(args=[None], need_report=False)
            except StopException:
                del self.callbacks[index]
            else:
                index += 1

        if len(self.callbacks) == 0:
            self.callbacks_empty.set()

    def _run_callbacks(self, data: Union[str, bytes]):
        index = 0
        while index < len(self.callbacks):
            (callback, reconnect) = self.callbacks[index]
            try:
                callback(args=[data])
            except StopException:
                del self.callbacks[index]
            else:
                index += 1

        if len(self.callbacks) == 0:
            self.callbacks_empty.set()

    def add_callback(self, callback: Callable[..., None], reconnect: bool):
        try:
            callback(args=[None], need_report=False)
        except StopException:
            return

        self.callbacks.append((callback, reconnect))
        if self.task is None:
            self.task = asyncio.ensure_future(self._async_connect())

    def send_message(self, data: Union[str, bytes], callback: Optional[Callable[[bool], None]] = None):
        self.output_msg.append((data, callback))
        self.output_msg_has_element.set()
        if self.task is None:
            self.task = asyncio.ensure_future(self._async_connect())


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
    if key is None:
        key = url

    def callback_with_empty(data: Union[None, str, bytes]):
        if data is not None:
            callback(data)

    _get_loop().call_soon_threadsafe(_get_ws_connection(url).add_callback, _wrapper(callback_with_empty, key),
                                     reconnect)


def on_message_buffered(url: str, callback: Callable[[List[Union[str, bytes]]], None],
                        buffer_time: float, key: Optional[str] = None,
                        reconnect: bool = True):
    if key is None:
        key = url

    def callback_with_empty(data: Optional[List[Union[str, bytes]]]):
        if data is not None:
            callback(data)

    wrapped_fun = _wrapper(callback_with_empty, key)

    arg_list = []
    prev_future = None

    def outer_callback(args, *vargs, **kwargs):
        nonlocal arg_list
        nonlocal prev_future
        pf = prev_future
        if pf is not None and pf.done():
            pf.result()

        if args[0] is None:
            wrapped_fun(args=args, *vargs, **kwargs)
        else:
            arg_list.append(args[0])
            if len(arg_list) == 1:
                prev_future = asyncio.Future()

                async def after(fut):
                    nonlocal arg_list
                    await asyncio.sleep(buffer_time)
                    plist, arg_list = arg_list, []
                    try:
                        wrapped_fun(args=[plist])
                        fut.set_result(None)
                    except StopException as e:
                        fut.set_exception(e)

                _get_loop().create_task(after(prev_future))

    _get_loop().call_soon_threadsafe(_get_ws_connection(url).add_callback, outer_callback, reconnect)


def send_message(url: str, message: Union[str, bytes], callback: Optional[Callable[[bool], None]] = None):
    _get_loop().call_soon_threadsafe(_get_ws_connection(url).send_message, message, _wrapper(callback,
                                                                                             delegate_stop=False))
