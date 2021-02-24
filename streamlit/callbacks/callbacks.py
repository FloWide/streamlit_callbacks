import asyncio
from concurrent.futures import Future
import functools
import threading
import weakref
from typing import Optional, Callable, Any, Tuple, List, Union, Dict

from streamlit import StopException
from streamlit.script_runner import RerunException
from streamlit.report_session import ReportSession, ReportSessionState
from streamlit.report_thread import get_report_ctx, add_report_ctx, ReportContext, REPORT_CONTEXT_ATTR_NAME
from tornado.ioloop import PeriodicCallback, IOLoop

_loop: Optional[asyncio.AbstractEventLoop] = None
_loop_lock = threading.Lock()
_thread: Optional[threading.Thread] = None


class _RerunAndStopException(StopException):
    pass


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


class _SessionState:
    @staticmethod
    def get_report_thread(rsession: ReportSession):
        return _getattrs(rsession, '_scriptrunner', '_script_thread')

    @staticmethod
    def set_state(rsession: ReportSession, state: ReportSessionState):
        setattr(rsession, '_state', state)
        getattr(rsession, '_enqueue_session_state_changed_message')()

    def __init__(self, report_session: ReportSession):
        self._session = weakref.ref(report_session)
        self._ctx: ReportContext = _getattrs(report_session, '_scriptrunner', '_script_thread', 'streamlit_report_ctx')
        self.callbacks = {}

    def refresh_ctx(self, session: Optional[ReportSession] = None):
        if session is None:
            session = self.get_session()

        ctx: ReportContext = getattr(_SessionState.get_report_thread(session), 'streamlit_report_ctx', None)
        if ctx is not None:
            self._ctx = ctx

    def get_ctx(self, session: Optional[ReportSession] = None):
        self.refresh_ctx(session)
        return self._ctx

    def get_session_with_state(self) -> Tuple[ReportSession, ReportSessionState]:
        session: ReportSession = self._session()
        if session is None:
            raise StopException("No report session")
        state: ReportSessionState = getattr(session, '_state')
        if state == ReportSessionState.SHUTDOWN_REQUESTED:
            raise StopException("Report session shutdown")
        return session, state

    def get_session(self) -> ReportSession:
        return self.get_session_with_state()[0]


def _getattrs(obj: object, *attrs: str, default: Optional[Any] = None):
    for attr in attrs:
        obj = getattr(obj, attr, None)
    return default if obj is None else obj


def _wrapped(session_state: _SessionState,
             cb_ref: Union[str, List[Callable[..., Any]]], need_report: bool = False,
             function_ctx: Optional[ReportContext] = None,
             delegate_stop: bool = True,
             args: Optional[List[Any]] = None,
             kwargs: Optional[Dict[Any, Any]] = None):
    if args is None:
        args = []
    if kwargs is None:
        kwargs = {}

    fun = None
    if isinstance(cb_ref, str):
        fun, function_ctx = session_state.callbacks.get(cb_ref, (None, None))
    elif len(cb_ref):
        fun = cb_ref[0]
    if fun is None:
        raise StopException("Deleted function, probably not delegated stop, or not handled")

    thread = threading.current_thread()
    orig_ctx = getattr(thread, REPORT_CONTEXT_ATTR_NAME, None)
    set_other_ctx = False
    rsession = None
    rerun = False
    try:
        rsession, rstate = session_state.get_session_with_state()
        current_ctx = session_state.get_ctx(rsession)
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
            rerun = True

        if isinstance(cb_ref, str):
            del session_state.callbacks[cb_ref]
        else:
            del cb_ref[0]
        if delegate_stop:
            raise
    except RerunException:
        rerun = True
    except BaseException:
        import traceback
        traceback.print_exc()
        raise
    finally:
        if rerun:
            rsession.request_rerun(getattr(rsession, '_client_state'))

        if set_other_ctx:
            if orig_ctx is None:
                delattr(thread, REPORT_CONTEXT_ATTR_NAME)
            else:
                add_report_ctx(thread=thread, ctx=orig_ctx)


def _wrapper(callback: Optional[Callable[..., None]], uniq_id: Optional[str] = None, need_report: bool = True,
             delegate_stop: bool = True):
    if callback is None:
        return functools.partial(_wrapper, uniq_id=uniq_id, need_report=need_report)

    report_ctx = get_report_ctx()
    report_session: ReportSession = _getattrs(report_ctx, '_enqueue', '__self__')
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
        callback_session_state.callbacks[uniq_id] = (callback, report_ctx)
        if pre_callback is None:
            return functools.partial(_wrapped, session_state=callback_session_state,
                                     cb_ref=uniq_id, need_report=need_report,
                                     delegate_stop=delegate_stop)
        else:
            def raise_(*args, **kwargs):
                if delegate_stop:
                    raise StopException("Already running")
            return raise_
    return functools.partial(_wrapped, session_state=callback_session_state, cb_ref=[callback], need_report=need_report,
                             function_ctx=report_ctx, delegate_stop=delegate_stop)


def call(cb: Callable[[], None], key: Optional[str] = None, reinvokable: Optional[bool] = None):
    """
    Schedule the callback to be called with args arguments at the next iteration of the event loop.
    :param cb: callback will be called exactly once.
    :param key: An unique string, which tells that this callable is unique through the whole session
    :param reinvokable: If this parameter is true, when this call ends, the key is released
    :return: None
    """
    if key is not None:
        key = "later_" + key

    if reinvokable is True:
        if key is None:
            raise RuntimeError("Reinvokable can be used only with key")
        orig_cb = cb

        def stopped_callback():
            try:
                orig_cb()
            except RerunException:
                raise _RerunAndStopException("Reinvoke")
            else:
                raise StopException("Reinvoke")

        cb = stopped_callback

    _get_loop().call_soon_threadsafe(_wrapper(cb, key))


def later(delay: Union[int, float], cb: Callable[[], None], key: Optional[str] = None, reinvokable: Optional[bool] = None):
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

    if reinvokable is True:
        if key is None:
            raise RuntimeError("Reinvokable can be used only with key")
        orig_cb = cb

        def stopped_callback():
            try:
                orig_cb()
            except RerunException:
                raise _RerunAndStopException("Reinvoke")
            else:
                raise StopException("Reinvoke")

        cb = stopped_callback

    _get_loop().call_soon_threadsafe(functools.partial(_get_loop().call_later, delay, _wrapper(cb, key,
                                                                                               delegate_stop=False)))


def at(when: Union[int, float], cb: Callable[[], None], key: Optional[str] = None, reinvokable: Optional[bool] = None):
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

    if reinvokable is True:
        if key is None:
            raise RuntimeError("Reinvokable can be used only with key")
        orig_cb = cb

        def stopped_callback():
            try:
                orig_cb()
            except RerunException:
                raise _RerunAndStopException("Reinvoke")
            else:
                raise StopException("Reinvoke")

        cb = stopped_callback

    _get_loop().call_soon_threadsafe(functools.partial(_get_loop().call_at, when, _wrapper(cb, key,
                                                                                           delegate_stop=False)))


def periodic(callback_time: Union[int, float], cb: Callable[[], None], key: Optional[str] = None,
             delay: Union[int, float] = 0):
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
        def __init__(self, callback: Callable[[], None], callback_time_millis: float):
            periodic_cb = PeriodicCallback(self.wrapped(callback), callback_time_millis)
            self.stop = periodic_cb.stop
            self.start = periodic_cb.start

        def wrapped(self, callback: Callable[[], None]):
            def res():
                try:
                    callback()
                except StopException:
                    self.stop()
            return res

    _get_loop().call_soon_threadsafe(functools.partial(_get_loop().call_later, delay, PeriodicCallbackHandler(
        _wrapper(cb, key), callback_time * 1000).start))


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
