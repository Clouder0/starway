from __future__ import annotations

import asyncio
import ctypes
import ctypes.util
import os
import warnings
from collections.abc import Callable
from typing import Annotated, Any, Literal, overload

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
from ._bindings import (  # type: ignore # noqa: E402
    ClientRecvFuture as _ClientRecvFuture,
)
from ._bindings import (  # noqa: E402 # type: ignore
    ClientSendFuture as _ClientSendFuture,
)
from ._bindings import Server as _Server  # type: ignore # noqa: E402
from ._bindings import (  # type: ignore # noqa: E402
    ServerRecvFuture as _ServerRecvFuture,
)
from ._bindings import (  # type: ignore # noqa: E402
    ServerSendFuture as _ServerSendFuture,
)


def check_sys_libs() -> Literal["system"] | Literal["wheel"]:
    assert _used_ucx == "system" or _used_ucx == "wheel"
    return _used_ucx


@overload
def wrap_to_asyncio(custom_future: SendFuture) -> asyncio.Future[None]: ...


@overload
def wrap_to_asyncio(custom_future: RecvFuture) -> asyncio.Future[tuple[int, int]]: ...


def wrap_to_asyncio(
    custom_future: SendFuture | RecvFuture,
    loop: asyncio.AbstractEventLoop | None = None,
):
    if loop is None:
        loop = asyncio.get_running_loop()
    future: asyncio.Future[Any] = asyncio.Future(loop=loop)

    def cb(cur: SendFuture | RecvFuture):
        exp = cur.exception()
        # print("Asyncio Future callback", cur, exp)
        if exp is not None:
            future.get_loop().call_soon_threadsafe(future.set_exception, exp)
        else:
            res = cur.result()
            future.get_loop().call_soon_threadsafe(future.set_result, res)

    custom_future.add_done_callback(cb)
    return future


class SendFuture:
    def __init__(self):
        self._cbs: list[Callable[[SendFuture], None]] = []
        self._done = False
        self._exception = None

    def _set_inner(self, inner: _ClientSendFuture | _ServerSendFuture):
        self._inner = inner

    def add_done_callback(self, callback: Callable[[SendFuture], None]):
        self._cbs.append(callback)

    def done(self):
        return self._done

    def exception(self):
        return self._exception

    def result(self):
        return None

    def set_result(self):
        if self._done:
            raise Exception("Duplicate set result on Future.")
        self._done = True
        for cb in self._cbs:
            cb(self)

    def set_exception(self, exp: Any):
        if self._done:
            raise Exception("Duplicate set result on Future.")
        self._exception = exp
        self._done = True
        for cb in self._cbs:
            cb(self)


class RecvFuture:
    def __init__(self):
        self._cbs: list[Callable[[RecvFuture], None]] = []
        self._done = False
        self._exception = None
        self._result: tuple[int, int] | None = None

    def _set_inner(self, inner: _ServerRecvFuture | _ClientRecvFuture):
        self._inner = inner

    def add_done_callback(self, callback: Callable[[RecvFuture], None]):
        self._cbs.append(callback)

    def done(self):
        return self._done

    def exception(self):
        return self._exception

    def result(self):
        return self._result

    def set_result(self, result: tuple[int, int]):
        if self._done:
            raise Exception("Duplicate set result on Future.")
        self._result = result
        self._done = True
        for cb in self._cbs:
            cb(self)

    def set_exception(self, exp: Any):
        if self._done:
            raise Exception("Duplicate set result on Future.")
        self._exception = exp
        self._done = True
        for cb in self._cbs:
            cb(self)


class Server:
    def __init__(self, addr: str, port: int):
        self._server = _Server(addr, port)

    def list_clients(self):
        return self._server.list_clients()

    def send(
        self,
        client_ep: int,
        buffer: Annotated[NDArray[np.uint8], dict(shape=(None,), device="cpu")],
        tag: int,
    ):
        ret = SendFuture()

        def cur_send(future: _ServerSendFuture):
            ret.set_result()

        def cur_fail(future: _ServerSendFuture):
            ret.set_exception(Exception(future.exception()))

        inner = self._server.send(client_ep, buffer, tag, cur_send, cur_fail)
        ret._set_inner(inner)
        return ret

    def asend(
        self,
        client_ep: int,
        buffer: Annotated[NDArray[np.uint8], dict(shape=(None,), device="cpu")],
        tag: int,
        loop: asyncio.AbstractEventLoop | None = None,
    ) -> asyncio.Future[None]:
        if loop is None:
            loop = asyncio.get_running_loop()
        ret = asyncio.Future(loop=loop)

        def cur_send(future: _ServerSendFuture):
            ret.get_loop().call_soon_threadsafe(ret.set_result, None)

        def cur_fail(future: _ServerSendFuture):
            ret.get_loop().call_soon_threadsafe(
                ret.set_exception, Exception(future.exception())
            )

        inner = self._server.send(client_ep, buffer, tag, cur_send, cur_fail)

        def capture_inner(x):
            nonlocal inner
            del inner

        # this callback does nothing, just capture inner future  to keep it alive
        ret.add_done_callback(capture_inner)
        return ret

    def recv(
        self,
        buffer: Annotated[NDArray[np.uint8], dict(shape=(None,), device="cpu")],
        tag: int,
        tag_mask: int,
    ):
        ret = RecvFuture()

        def cur_recv(future: _ServerRecvFuture):
            ret.set_result(future.info())

        def cur_fail(future: _ServerRecvFuture):
            ret.set_exception(Exception(future.exception()))

        inner = self._server.recv(buffer, tag, tag_mask, cur_recv, cur_fail)
        ret._set_inner(inner)
        return ret

    def arecv(
        self,
        buffer: Annotated[NDArray[np.uint8], dict(shape=(None,), device="cpu")],
        tag: int,
        tag_mask: int,
        loop: asyncio.AbstractEventLoop | None = None,
    ) -> asyncio.Future[tuple[int, int]]:
        if loop is None:
            loop = asyncio.get_running_loop()
        ret = asyncio.Future(loop=loop)

        def cur_send(future: _ServerRecvFuture):
            ret.get_loop().call_soon_threadsafe(ret.set_result, future.info())

        def cur_fail(future: _ServerRecvFuture):
            ret.get_loop().call_soon_threadsafe(
                ret.set_exception, Exception(future.exception())
            )

        inner = self._server.recv(buffer, tag, tag_mask, cur_send, cur_fail)

        def capture_inner(x):
            nonlocal inner
            del inner

        # this callback does nothing, just capture inner future  to keep it alive
        ret.add_done_callback(capture_inner)
        return ret


class Client:
    def __init__(self, remote_addr: str, port: int):
        self._client = _Client(remote_addr, port)

    def send(
        self,
        buffer: Annotated[NDArray[np.uint8], dict(shape=(None,), device="cpu")],
        tag: int,
    ):
        ret = SendFuture()

        def cur_send(future: _ClientSendFuture):
            ret.set_result()

        def cur_fail(future: _ClientSendFuture):
            ret.set_exception(Exception(future.exception()))

        inner = self._client.send(buffer, tag, cur_send, cur_fail)
        ret._set_inner(inner)
        return ret

    def recv(
        self,
        buffer: Annotated[NDArray[np.uint8], dict(shape=(None,), device="cpu")],
        tag: int,
        tag_mask: int,
    ):
        ret = RecvFuture()

        def cur_recv(future: _ClientRecvFuture):
            ret.set_result(future.info())

        def cur_fail(future: _ClientRecvFuture):
            ret.set_exception(Exception(future.exception()))

        inner = self._client.recv(buffer, tag, tag_mask, cur_recv, cur_fail)
        ret._set_inner(inner)
        return ret

    def arecv(
        self,
        buffer: Annotated[NDArray[np.uint8], dict(shape=(None,), device="cpu")],
        tag: int,
        tag_mask: int,
        loop: asyncio.AbstractEventLoop | None = None,
    ) -> asyncio.Future[tuple[int, int]]:
        if loop is None:
            loop = asyncio.get_running_loop()
        ret = asyncio.Future(loop=loop)

        def cur_send(future: _ClientRecvFuture):
            ret.get_loop().call_soon_threadsafe(ret.set_result, future.info())

        def cur_fail(future: _ClientRecvFuture):
            ret.get_loop().call_soon_threadsafe(
                ret.set_exception, Exception(future.exception())
            )

        inner = self._client.recv(buffer, tag, tag_mask, cur_send, cur_fail)

        def capture_inner(x):
            nonlocal inner
            del inner

        # this callback does nothing, just capture inner future  to keep it alive
        ret.add_done_callback(capture_inner)
        return ret

    def asend(
        self,
        buffer: Annotated[NDArray[np.uint8], dict(shape=(None,), device="cpu")],
        tag: int,
        loop: asyncio.AbstractEventLoop | None = None,
    ) -> asyncio.Future[None]:
        if loop is None:
            loop = asyncio.get_running_loop()
        ret = asyncio.Future(loop=loop)

        def cur_send(future: _ClientSendFuture):
            ret.get_loop().call_soon_threadsafe(ret.set_result, None)

        def cur_fail(future: _ClientSendFuture):
            ret.get_loop().call_soon_threadsafe(
                ret.set_exception, Exception(future.exception())
            )

        inner = self._client.send(buffer, tag, cur_send, cur_fail)

        def capture_inner(x):
            nonlocal inner
            del inner

        # this callback does nothing, just capture inner future  to keep it alive
        ret.add_done_callback(capture_inner)
        return ret


__all__ = [
    "Server",
    "Client",
    "SendFuture",
    "RecvFuture",
    "wrap_to_asyncio",
    "check_sys_libs",
]
