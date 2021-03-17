import asyncio
import functools
import threading
import weakref
from _weakref import ReferenceType
from concurrent.futures import Future
from typing import Optional, Callable, Any, Tuple, List, Union, Dict

from streamlit import StopException
from streamlit.proto.WidgetStates_pb2 import WidgetStates
from streamlit.report_session import ReportSession, ReportSessionState
from streamlit.report_thread import get_report_ctx, add_report_ctx, ReportContext, REPORT_CONTEXT_ATTR_NAME
from streamlit.script_request_queue import RerunData
from streamlit.script_runner import RerunException
from tornado.ioloop import PeriodicCallback, IOLoop

_loop: Optional[asyncio.AbstractEventLoop] = None
_loop_lock = threading.Lock()
_thread: Optional[threading.Thread] = None


class _RerunAndStopException(StopException, RerunException):
    def __init__(self, rerun_data):
        StopException.__init__(self)
        RerunException.__init__(self, rerun_data=rerun_data)


def _run(fut: Future):
    global _loop
    asyncio.set_event_loop(asyncio.new_event_loop())
    try:
        _loop = asyncio.get_event_loop()
        loop = IOLoop.current()
        fut.set_result(_loop)
    except BaseException as e:
        fut.set_exception(e)
    else:
        loop.start()
    finally:
        _loop = None


def _get_loop() -> asyncio.AbstractEventLoop:
    global _loop
    if _loop is None:
        with _loop_lock:
            if _loop is None:
                global _thread
                fut = Future()
                _thread = threading.Thread(daemon=True, target=functools.partial(_run, fut=fut), name="callback thread")
                _thread.start()
                return fut.result()
    return _loop


def _getattrs(obj: object, *attrs: str, default: Optional[Any] = None):
    for attr in attrs:
        obj = getattr(obj, attr, None)
    return default if obj is None else obj


class _SessionState:
    @staticmethod
    def get_report_session_from_ctx(report_ctx: ReportContext):
        return _getattrs(report_ctx, '_enqueue', '__self__')

    @staticmethod
    def get_report_thread(rsession: ReportSession):
        return _getattrs(rsession, '_scriptrunner', '_script_thread')

    @staticmethod
    def set_state(rsession: ReportSession, state: ReportSessionState):
        setattr(rsession, '_state', state)
        getattr(rsession, '_enqueue_session_state_changed_message')()

    def __init__(self, report_session: ReportSession):
        self._session = report_session
        self._ctx: ReportContext = _SessionState.get_report_thread(self._session).streamlit_report_ctx
        self.callbacks = {}
        self.at_end = {}

    def refresh_ctx(self):
        ctx: ReportContext = getattr(_SessionState.get_report_thread(self._session), 'streamlit_report_ctx', None)
        if ctx is not None:
            self._ctx = ctx

    def get_ctx(self):
        self.refresh_ctx()
        return self._ctx

    def get_session_state(self) -> ReportSessionState:
        state: ReportSessionState = getattr(self._session, '_state')
        if state == ReportSessionState.SHUTDOWN_REQUESTED:
            raise StopException("Report session shutdown")
        return state

    def get_session(self) -> ReportSession:
        return self._session

    def regist_at_end(self, at_end: Callable[[], None], session: bool = True):
        self.at_end = {k: v for k, v in self.at_end.items() if v.alive}
        if at_end is None:
            return
        fun_repr = f"{repr(at_end)} - {session}"
        pre_finalize = self.at_end.get(fun_repr)
        if pre_finalize is not None:
            pre_finalize()

        self.at_end[fun_repr] = weakref.finalize(self._session if session else self._ctx, at_end)


def _wrapped(session_state_ref: 'ReferenceType[_SessionState]',
             cb_ref: Union[str, List[Tuple[Callable[..., Any], 'ReferenceType[_SessionState]']]],
             need_report: bool = False,
             delegate_stop: bool = True,
             args: Optional[List[Any]] = None,
             kwargs: Optional[Dict[Any, Any]] = None):
    if args is None:
        args = []
    if kwargs is None:
        kwargs = {}

    session_state = session_state_ref()
    if session_state is None:
        if delegate_stop:
            raise StopException("No session state")
        return

    fun, function_ctx_ref = None, None
    if isinstance(cb_ref, str):
        fun, function_ctx_ref = session_state.callbacks.get(cb_ref, (None, None))
    elif len(cb_ref):
        fun, function_ctx_ref = cb_ref[0] if len(cb_ref) else (None, None)
    if fun is None:
        raise StopException("Deleted function, probably not delegated stop, or not handled")
    function_ctx = function_ctx_ref()

    thread = threading.current_thread()
    orig_ctx = getattr(thread, REPORT_CONTEXT_ATTR_NAME, None)
    set_other_ctx = False
    rsession = session_state.get_session()
    rerun = None
    try:
        if function_ctx is None:
            raise StopException("No function context")

        rstate = session_state.get_session_state()
        current_ctx = session_state.get_ctx()
        if current_ctx != function_ctx:
            raise StopException("Other context")

        add_report_ctx(thread=thread, ctx=current_ctx)
        set_other_ctx = True
        need_report_not_running = False
        try:
            if need_report:
                if rstate == ReportSessionState.REPORT_NOT_RUNNING:
                    need_report_not_running = True
                    _SessionState.set_state(rsession, ReportSessionState.REPORT_IS_RUNNING)
            fun(*args, **kwargs)
        finally:
            if need_report_not_running and _SessionState.get_report_thread(rsession) is None:
                _SessionState.set_state(rsession, ReportSessionState.REPORT_NOT_RUNNING)
    except StopException as e:
        if isinstance(e, _RerunAndStopException):
            rerun = e.rerun_data

        if isinstance(cb_ref, str):
            del session_state.callbacks[cb_ref]
        else:
            del cb_ref[0]
        if delegate_stop:
            raise
    except RerunException as e:
        rerun = e.rerun_data
    except BaseException:
        import traceback
        traceback.print_exc()
        raise
    finally:
        if rerun:
            rsession.request_rerun(rerun)

        if set_other_ctx:
            if orig_ctx is None:
                delattr(thread, REPORT_CONTEXT_ATTR_NAME)
            else:
                add_report_ctx(thread=thread, ctx=orig_ctx)


def _wrapper(callback: Optional[Callable[..., None]],
             uniq_id: Optional[str] = None,
             need_report: bool = True,
             delegate_stop: bool = True,
             at_end: Optional[Callable[[], None]] = None,
             out_wrapper: Callable[[Callable[..., None]], Callable[..., None]] = lambda i: i):
    if callback is None:
        return functools.partial(_wrapper, uniq_id=uniq_id, need_report=need_report, delegate_stop=delegate_stop,
                                 at_end=at_end, out_wrapper=out_wrapper)

    report_ctx = get_report_ctx()
    report_session: ReportSession = _SessionState.get_report_session_from_ctx(report_ctx)
    if report_session is None:
        raise StopException("No report session")

    callback_session_state = getattr(report_session, '_callback_session_state', None)
    if callback_session_state is None:
        callback_session_state = _SessionState(report_session)
        setattr(report_session, '_callback_session_state', callback_session_state)
    else:
        callback_session_state.refresh_ctx()

    if uniq_id is not None:
        pre_callback = callback_session_state.callbacks.get(uniq_id)
        callback_session_state.callbacks[uniq_id] = (callback, weakref.ref(report_ctx))
        if pre_callback is None:
            callback_session_state.regist_at_end(at_end, session=True)
            return out_wrapper(functools.partial(_wrapped,
                                                 session_state_ref=weakref.ref(callback_session_state),
                                                 cb_ref=uniq_id,
                                                 need_report=need_report,
                                                 delegate_stop=delegate_stop))
        else:
            def raise_(*args, **kwargs):
                if delegate_stop:
                    raise StopException("Already running")

            return raise_
    callback_session_state.regist_at_end(at_end, session=False)
    return out_wrapper(functools.partial(_wrapped,
                                         session_state_ref=weakref.ref(callback_session_state),
                                         cb_ref=[(callback, weakref.ref(report_ctx))],
                                         need_report=need_report,
                                         delegate_stop=delegate_stop))


def call(cb: Callable[..., None], *args, key: Optional[str] = None, reinvokable: Optional[bool] = None, **kwargs):
    """
    Schedule the callback to be called with args arguments at the next iteration of the event loop.
    :param cb: callback will be called exactly once.
    :param key: An unique string, which tells that this callable is unique through the whole session
    :param reinvokable: If this parameter is true, when this call ends, the key is released
    :return: None
    """
    if key is not None:
        key = "later_" + key

    cb = functools.partial(cb, *args, **kwargs)
    if reinvokable is True:
        if key is None:
            raise RuntimeError("Reinvokable can be used only with key")
        orig_cb = cb

        def stopped_callback():
            try:
                orig_cb()
            except RerunException as re:
                raise _RerunAndStopException(re.rerun_data)
            else:
                raise StopException("Reinvoke")

        cb = stopped_callback

    _get_loop().call_soon_threadsafe(_wrapper(cb, key, delegate_stop=False))


def later(delay: Union[int, float], cb: Callable[..., None], *args, key: Optional[str] = None,
          reinvokable: Optional[bool] = None, **kwargs):
    """
    Schedule callback to be called after the given delay number of seconds (can be either an int or a float).
    :param delay: seconds
    :param cb: callback will be called exactly once.
    :param key: An unique string, which tells that this callable is unique through the whole session
    :param reinvokable: If this parameter is true, when this call ends, the key is released
    :return: None
    """
    if key is not None:
        key = "later_" + key

    cb = functools.partial(cb, *args, **kwargs)
    if reinvokable is True:
        if key is None:
            raise RuntimeError("Reinvokable can be used only with key")
        orig_cb = cb

        def stopped_callback():
            try:
                orig_cb()
            except RerunException as re:
                raise _RerunAndStopException(re.rerun_data)
            else:
                raise StopException("Reinvoke")

        cb = stopped_callback

    _get_loop().call_soon_threadsafe(functools.partial(_get_loop().call_later, delay, _wrapper(cb, key,
                                                                                               delegate_stop=False)))


def at(when: Union[int, float], cb: Callable[[], None],
       *args, key: Optional[str] = None, reinvokable: Optional[bool] = None, **kwargs):
    """
    Schedule callback to be called at the given absolute timestamp when (an int or a float),
        using the same time reference as callbacks.time().
    :param when: absolute timestamp with, same time reference as callbacks.time()
    :param cb: callback will be called exactly once.
    :param key: An unique string, which tells that this callable is unique through the whole session
    :param reinvokable: If this parameter is true, when this call ends, the key is released
    :return: None
    """
    if key is not None:
        key = "at_" + key

    cb = functools.partial(cb, *args, **kwargs)
    if reinvokable is True:
        if key is None:
            raise RuntimeError("Reinvokable can be used only with key")
        orig_cb = cb

        def stopped_callback():
            try:
                orig_cb()
            except RerunException as re:
                raise _RerunAndStopException(re.rerun_data)
            else:
                raise StopException("Reinvoke")

        cb = stopped_callback

    _get_loop().call_soon_threadsafe(functools.partial(_get_loop().call_at, when, _wrapper(cb, key,
                                                                                           delegate_stop=False)))


def periodic(callback_time: Union[int, float], cb: Callable[..., None], *args, key: Optional[str] = None,
             delay: Union[int, float] = 0, **kwargs):
    """
    Schedules the given callback to be called periodically.

    The callback is called every ``callback_time`` seconds.

    If the callback runs for longer than ``callback_time`` seconds,
    subsequent invocations will be skipped to get back on schedule.

    :param callback_time: seconds
    :param cb: callback will be called periodically
    :param key: An unique string, which tells that this callable is unique through the whole session
    :param delay: seconds which tells that first run need to be delayed with this amount of seconds
    :return: None
    """
    if key is not None:
        key = "periodic_" + key

    class PeriodicCallbackHandler:
        def __init__(self, callback_time_millis: float):
            periodic_cb = PeriodicCallback(self.wrapped(_wrapper(functools.partial(cb, *args, **kwargs), key,
                                                                 at_end=self.stop)), callback_time_millis)
            self._stop = periodic_cb.stop
            self.start = periodic_cb.start

        def stop(self):
            self._stop()

        def wrapped(self, callback: Callable[[], None]):
            def res():
                try:
                    callback()
                except StopException:
                    self._stop()

            return res

    _get_loop().call_soon_threadsafe(functools.partial(_get_loop().call_later, delay,
                                                       PeriodicCallbackHandler(callback_time * 1000).start))


def clear():
    """
    Register this run to the callback thread. This call is important, when it may no other callback
        register function call happens this run
    :return: None
    """
    refresh_ctx = _getattrs(get_report_ctx(), '_enqueue', '__self__', '_callback_session_state', 'refresh_ctx')
    if callable(refresh_ctx):
        refresh_ctx()


def time() -> float:
    """
    Return the current time, as a float value, according to the event loop’s internal monotonic clock.
    :return: the event loop’s internal monotonic clock current time
    """
    return _get_loop().time()


def rerun(*args, **kwargs):
    """
    Throws a RerunException
    :return: None

    Usage:

    ```
    from streamlit.callbacks.callbacks import later, rerun
    later(5.0, rerun)
    ```
    """
    report_ctx = get_report_ctx()
    wstates = WidgetStates()
    wstates.widgets.extend(report_ctx.widgets._state.values())
    raise RerunException(RerunData(report_ctx.query_string, wstates))
