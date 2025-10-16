from __future__ import annotations

import asyncio
import ctypes
import ctypes.util
import os
import warnings
from collections.abc import Callable
from typing import Annotated, Literal

import numpy as np
from numpy.typing import NDArray

use_system_lib = os.environ.get("STARWAY_USE_SYSTEM_UCX", "true") == "true"
_system_ucx_available = False
if ctypes.util.find_library("ucp") is not None:
    _system_ucx_available = True

_used_ucx = "system"

if not use_system_lib:
    print("Try to load libucx from wheel package.")
    try:
        import libucx  # type: ignore

        libucx.load_library()
        _used_ucx = "wheel"
    except ImportError:
        if _system_ucx_available:
            warnings.warn(
                "STARWAY_USE_SYSTEM_UCX set to false, but libucx not found in wheel package. Try to load libucx from system."
            )
            _used_ucx = "system"
        else:
            raise ImportError(
                "STARWAY_USE_SYSTEM_UCX is set to false, but cannot find python package libucx, and no fallback system-level libucx installed either! Please install it by: pip install libucx-cu12"
            )
else:
    if not _system_ucx_available:
        warnings.warn(
            "STARWAY_USE_SYSTEM_UCX set to true, but libucx not found in system. Try to load libucx from wheel package."
        )
        try:
            import libucx  # type: ignore

            libucx.load_library()
            _used_ucx = "wheel"
        except ImportError:
            raise ImportError(
                "libucx wheel not installed either. No libucx availalbe. Fatal Error."
            )


from ._bindings import Client as _Client  # type: ignore # noqa: E402
from ._bindings import (  # noqa: E402 # type: ignore
    Context,
    ServerEndpoint,
)
from ._bindings import Server as _Server  # type: ignore # noqa: E402


def check_sys_libs() -> Literal["system"] | Literal["wheel"]:
    assert _used_ucx == "system" or _used_ucx == "wheel"
    return _used_ucx


_ucx_context = Context()


class Server:
    def __init__(self):
        self._server = _Server(_ucx_context)

    def listen(self, addr: str, port: int):
        self._server.listen(addr, port)

    def set_accept_cb(self, on_accept: Callable[[ServerEndpoint], None]):
        self._server.set_accept_callback(on_accept)

    def aclose(self, loop: asyncio.AbstractEventLoop | None = None):
        if loop is None:
            loop = asyncio.get_running_loop()
        ret: asyncio.Future[None] = asyncio.Future(loop=loop)

        def close_cb():
            print("Server closed!")
            loop.call_soon_threadsafe(ret.set_result, None)

        self._server.close(close_cb)
        return ret

    def list_clients(self):
        return self._server.list_clients()

    def send(
        self,
        client_ep: ServerEndpoint,
        buffer: NDArray[np.uint8],
        tag: int,
        done_callback: Callable[[], None],
        fail_callback: Callable[[str], None],
    ):
        return self._server.send(client_ep, buffer, tag, done_callback, fail_callback)

    def asend(
        self,
        client_ep: ServerEndpoint,
        buffer: Annotated[NDArray[np.uint8], dict(shape=(None,), device="cpu")],
        tag: int,
        loop: asyncio.AbstractEventLoop | None = None,
    ):
        if loop is None:
            loop = asyncio.get_running_loop()
        ret: asyncio.Future[None] = asyncio.Future(loop=loop)

        def cur_send():
            ret.get_loop().call_soon_threadsafe(ret.set_result, None)

        def cur_fail(reason: str):
            ret.get_loop().call_soon_threadsafe(ret.set_exception, Exception(reason))

        self._server.send(client_ep, buffer, tag, cur_send, cur_fail)
        return ret

    def recv(
        self,
        buffer: NDArray[np.uint8],
        tag: int,
        tag_mask: int,
        done_callback: Callable[[int, int], None],
        fail_callback: Callable[[str], None],
    ):
        return self._server.recv(buffer, tag, tag_mask, done_callback, fail_callback)

    def arecv(
        self,
        buffer: Annotated[NDArray[np.uint8], dict(shape=(None,), device="cpu")],
        tag: int,
        tag_mask: int,
        loop: asyncio.AbstractEventLoop | None = None,
    ):
        if loop is None:
            loop = asyncio.get_running_loop()
        ret: asyncio.Future[tuple[int, int]] = asyncio.Future(loop=loop)

        def cur_send(sender_tag: int, length: int):
            ret.get_loop().call_soon_threadsafe(ret.set_result, (sender_tag, length))

        def cur_fail(reason: str):
            ret.get_loop().call_soon_threadsafe(ret.set_exception, Exception(reason))

        self._server.recv(buffer, tag, tag_mask, cur_send, cur_fail)
        return ret

    def flush(
        self, done_callback: Callable[[], None], fail_callback: Callable[[str], None]
    ):
        return self._server.flush(done_callback, fail_callback)

    def aflush(self, loop: asyncio.AbstractEventLoop | None = None):
        if loop is None:
            loop = asyncio.get_running_loop()
        ret: asyncio.Future[None] = asyncio.Future(loop=loop)

        def cur_send():
            ret.get_loop().call_soon_threadsafe(ret.set_result, None)

        def cur_fail(reason: str):
            ret.get_loop().call_soon_threadsafe(ret.set_exception, Exception(reason))

        self._server.flush(cur_send, cur_fail)
        return ret

    def flush_ep(
        self,
        client_ep: ServerEndpoint,
        done_callback: Callable[[], None],
        fail_callback: Callable[[str], None],
    ):
        return self._server.flush_ep(client_ep, done_callback, fail_callback)

    def aflush_ep(
        self,
        client_ep: ServerEndpoint,
        loop: asyncio.AbstractEventLoop | None = None,
    ):
        if loop is None:
            loop = asyncio.get_running_loop()
        ret: asyncio.Future[None] = asyncio.Future(loop=loop)

        def cur_send():
            ret.get_loop().call_soon_threadsafe(ret.set_result, None)

        def cur_fail(reason: str):
            ret.get_loop().call_soon_threadsafe(ret.set_exception, Exception(reason))

        self._server.flush_ep(client_ep, cur_send, cur_fail)
        return ret

    def evaluate_perf(self, client_ep: ServerEndpoint, msg_size: int) -> float:
        return self._server.evaluate_perf(client_ep, msg_size)


class Client:
    def __init__(self):
        self._client = _Client(_ucx_context)

    def aconnect(
        self, addr: str, port: int, loop: asyncio.AbstractEventLoop | None = None
    ):
        if loop is None:
            loop = asyncio.get_running_loop()
        ret: asyncio.Future[None] = asyncio.Future(loop=loop)

        def connection_cb(status: str):
            print("Connected!")
            if status == "":
                loop.call_soon_threadsafe(ret.set_result, None)
            else:
                loop.call_soon_threadsafe(ret.set_exception, Exception(status))

        self._client.connect(addr, port, connection_cb)
        return ret

    def aclose(self, loop: asyncio.AbstractEventLoop | None = None):
        if loop is None:
            loop = asyncio.get_running_loop()
        ret: asyncio.Future[None] = asyncio.Future(loop=loop)

        def close_cb():
            print("Client closed!")
            loop.call_soon_threadsafe(ret.set_result, None)

        self._client.close(close_cb)
        return ret

    def recv(
        self,
        buffer: Annotated[NDArray[np.uint8], dict(shape=(None,), device="cpu")],
        tag: int,
        tag_mask: int,
        done_callback: Callable[[int, int], None],
        fail_callback: Callable[[str], None],
    ):
        return self._client.recv(buffer, tag, tag_mask, done_callback, fail_callback)

    def arecv(
        self,
        buffer: Annotated[NDArray[np.uint8], dict(shape=(None,), device="cpu")],
        tag: int,
        tag_mask: int,
        loop: asyncio.AbstractEventLoop | None = None,
    ):
        if loop is None:
            loop = asyncio.get_running_loop()
        ret: asyncio.Future[tuple[int, int]] = asyncio.Future(loop=loop)

        def cur_send(sender_tag: int, length: int):
            ret.get_loop().call_soon_threadsafe(ret.set_result, (sender_tag, length))

        def cur_fail(reason: str):
            ret.get_loop().call_soon_threadsafe(ret.set_exception, Exception(reason))

        self._client.recv(buffer, tag, tag_mask, cur_send, cur_fail)
        return ret

    def send(
        self,
        buffer: Annotated[NDArray[np.uint8], dict(shape=(None,), device="cpu")],
        tag: int,
        done_callback: Callable[[], None],
        fail_callback: Callable[[str], None],
    ):
        return self._client.send(buffer, tag, done_callback, fail_callback)

    def asend(
        self,
        buffer: Annotated[NDArray[np.uint8], dict(shape=(None,), device="cpu")],
        tag: int,
        loop: asyncio.AbstractEventLoop | None = None,
    ):
        if loop is None:
            loop = asyncio.get_running_loop()
        ret: asyncio.Future[None] = asyncio.Future(loop=loop)

        def cur_send():
            ret.get_loop().call_soon_threadsafe(ret.set_result, None)

        def cur_fail(reason: str):
            ret.get_loop().call_soon_threadsafe(ret.set_exception, Exception(reason))

        self._client.send(buffer, tag, cur_send, cur_fail)
        return ret

    def flush(
        self,
        done_callback: Callable[[], None],
        fail_callback: Callable[[str], None],
    ):
        return self._client.flush(done_callback, fail_callback)

    def aflush(self, loop: asyncio.AbstractEventLoop | None = None):
        if loop is None:
            loop = asyncio.get_running_loop()
        ret: asyncio.Future[None] = asyncio.Future(loop=loop)

        def cur_done():
            ret.get_loop().call_soon_threadsafe(ret.set_result, None)

        def cur_fail(reason: str):
            ret.get_loop().call_soon_threadsafe(ret.set_exception, Exception(reason))

        self._client.flush(cur_done, cur_fail)
        return ret

    def evaluate_perf(self, msg_size: int) -> float:
        return self._client.evaluate_perf(msg_size)

    # def evaluate_perf(self, msg_size: int) -> float:
    # return self._client.evaluate_perf(msg_size)


__all__ = [
    "Server",
    "Client",
    "ServerEndpointcheck_sys_libs",
    # "ucp_get_version",
]  # type: ignore
