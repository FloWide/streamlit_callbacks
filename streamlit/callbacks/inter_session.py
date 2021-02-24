
from streamlit.report_thread import get_report_ctx
from typing import Callable
from streamlit.callbacks.callbacks import _wrapper
import streamlit

sessions = {}


def _check_sessions():
    for k, v in list(sessions.items()):
        try:
            v(args=[None, None], need_report=False)
        except:
            del sessions[k]


def on_message(callback: Callable[[str, str], None]):
    def cb(session, data):
        if session is not None:
            callback(session, data)

    sessions[get_report_ctx().session_id] = _wrapper(cb)

    _check_sessions()


def is_listening(session_id: str):
    _check_sessions()
    return sessions.get(session_id, None) is not None


def send_message(session_id: str, message: str):
    try:
        sessions[session_id](args=[get_report_ctx().session_id, message])
    except:
        streamlit.error(f"Session id {session_id} is not in listening state")

    _check_sessions()
