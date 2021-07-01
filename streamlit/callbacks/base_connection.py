import asyncio
from abc import ABC, abstractmethod
from asyncio import Future
from typing import Awaitable, Tuple, Union, List, Optional, Dict, Any, Callable

from streamlit import StopException
from .callbacks import _get_loop


class _BaseConnection(ABC):
    def __init__(self, address):
        ABC.__init__(self)
        self.address = address
        self.callbacks = []
        self.callbacks_empty = asyncio.Event(loop=_get_loop())
        self.output_msg = []
        self.output_msg_has_element = asyncio.Event(loop=_get_loop())
        self.task = None
        self.alive = False

    @staticmethod
    @abstractmethod
    def get_reconnect_time_seconds() -> Union[float, int]:
        return 1.0

    @abstractmethod
    def _get_event(self, connection) -> Awaitable:
        raise NotImplementedError('get_event')

    @abstractmethod
    def _handle_event(self, connection, event_result):
        raise NotImplementedError('handle_event')

    @abstractmethod
    def _connect(self) -> Awaitable:
        raise NotImplementedError('connect')

    @abstractmethod
    def _send_messages(self, connection: Optional, messages: List):
        raise NotImplementedError('send messages')

    @abstractmethod
    def _call_callback(self, callback_struct, end_connection: bool, args: Optional = None) -> bool:
        raise NotImplementedError('call callback')

    @abstractmethod
    def _at_disconnect(self):
        raise NotImplementedError('disconnect callback')

    @abstractmethod
    def _callback_remove(self, connection: Optional, callback_struct):
        raise NotImplementedError('callback remove')

    def _call_all_callback(self, connection: Optional, args=None):
        index = 0
        while index < len(self.callbacks):
            if not self._call_callback(self.callbacks[index], connection is None, args):
                self._callback_remove(connection, self.callbacks[index])
                del self.callbacks[index]
            else:
                index += 1
        if len(self.callbacks) == 0:
            self.callbacks_empty.set()

    async def _async_connect(self):
        empty = asyncio.ensure_future(self.callbacks_empty.wait())
        out_msg = asyncio.ensure_future(self.output_msg_has_element.wait())
        while len(self.callbacks) or len(self.output_msg):
            connection = None
            try:
                connection = await self._connect()
                self.alive = True
                event = asyncio.ensure_future(self._get_event(connection))
                print(f"Connected to '{self.address}' successfully")
                while len(self.callbacks) or len(self.output_msg):
                    done, pend = await asyncio.wait([event, out_msg, empty], return_when=asyncio.FIRST_COMPLETED)
                    for task in done:
                        if task is event:
                            self._handle_event(connection, task.result())
                            event = asyncio.ensure_future(self._get_event(connection))
                        if task is out_msg:
                            msgs, self.output_msg = self.output_msg, []
                            self._send_messages(connection, msgs)
                            self.output_msg_has_element.clear()
                            out_msg = asyncio.ensure_future(self.output_msg_has_element.wait())
                        if task is empty:
                            if len(self.callbacks) > 0:
                                self.callbacks_empty.clear()
                                empty = asyncio.ensure_future(self.callbacks_empty.wait())
                                self._call_all_callback(connection)
                connection.close()
            except BaseException as e:
                if connection is not None:
                    connection.close()
                if len(self.callbacks) or len(self.output_msg):
                    print(f"Connection '{self.address}' ends: {e.__class__.__name__} {e}, try to reconnect after "
                          f"{self.__class__.get_reconnect_time_seconds()} sec")
            else:
                if len(self.callbacks) or len(self.output_msg):
                    print(f"Connection '{self.address}' ends peacefully, try to reconnect after "
                          f"{self.__class__.get_reconnect_time_seconds()} sec")
            finally:
                self.alive = False
                self._call_all_callback(None)
                msgs, self.output_msg = self.output_msg, []
                self._send_messages(None, msgs)
                self.output_msg_has_element.clear()

                if len(self.callbacks) or len(self.output_msg):
                    await asyncio.sleep(self.__class__.get_reconnect_time_seconds())

        print(f"Connection '{self.address}' ended")
        self.callbacks_empty.clear()
        self.output_msg_has_element.clear()

        if not empty.done():
            empty.cancel()
        if not out_msg.done():
            out_msg.cancel()

        self._at_disconnect()

        self.task = None

    def _add_callback(self, args):
        if not self._call_callback(args, False):
            return

        self.callbacks.append(args)
        if self.task is None:
            self.task = asyncio.ensure_future(self._async_connect())

    def _add_message(self, args):
        self.output_msg.append(args)
        self.output_msg_has_element.set()
        if self.task is None:
            self.task = asyncio.ensure_future(self._async_connect())

    def at_end(self):
        self.callbacks_empty.set()

    def threadsafe_at_end(self):
        _get_loop().call_soon_threadsafe(self.at_end)


class _TimeBuffering:
    def __init__(self,
                 callback: Callable[..., None],
                 buffer_time: Union[float, int],
                 is_null: Callable[[Tuple[Optional[List[Any]], Optional[Dict[str, Any]]]], bool],
                 merge: Callable[[List[Tuple[Optional[List[Any]], Optional[Dict[str, Any]]]]],
                                 Tuple[Optional[List[Any]], Optional[Dict[str, Any]]]],
                 at_exception: Callable[[], None]):
        self.callback = callback
        self.buffer_time = buffer_time
        self.is_null = is_null
        self.merge = merge
        self.at_exception = at_exception
        self.arguments: List[Tuple[Optional[List[Any]], Optional[Dict[str, Any]]]] = []
        self.prev_future: Optional["Future[None]"] = None

    def __call__(self, args=None, kwargs=None, *vargs, **vkwargs):
        if self.prev_future is not None and self.prev_future.done():
            self.prev_future.result()

        if self.is_null((args, kwargs)):
            self.callback(args=args, kwargs=kwargs, *vargs, **vkwargs)
        else:
            self.arguments.append((args, kwargs))
            if len(self.arguments) == 1:
                prev_future = asyncio.Future()

                async def after(fut, v_args, v_kwargs):
                    await asyncio.sleep(self.buffer_time)
                    plist, self.arguments = self.arguments, []
                    try:
                        merged_args, merged_kwargs = self.merge(plist)
                        self.callback(args=merged_args, kwargs=merged_kwargs, *v_args, **v_kwargs)
                        fut.set_result(None)
                    except StopException as e:
                        self.at_exception()

                _get_loop().create_task(after(prev_future, vargs, vkwargs))
