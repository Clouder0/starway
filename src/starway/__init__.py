from __future__ import annotations

import asyncio
from collections.abc import Callable
import ctypes
import ctypes.util
import os
import warnings
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
from ._bindings import (  # type: ignore # noqa: E402
    ClientRecvFuture as _ClientRecvFuture,
)
from ._bindings import (  # noqa: E402 # type: ignore
    ClientSendFuture as _ClientSendFuture,
)
from ._bindings import Context  # type: ignore  # noqa: E402
from ._bindings import Server as _Server  # type: ignore # noqa: E402
from ._bindings import (  # type: ignore # noqa: E402
    ServerEndpoint as _ServerEndpoint,
)
from ._bindings import (  # type: ignore # noqa: E402
    ServerRecvFuture as _ServerRecvFuture,
)
from ._bindings import (  # type: ignore # noqa: E402
    ServerSendFuture as _ServerSendFuture,
)


def check_sys_libs() -> Literal["system"] | Literal["wheel"]:
    assert _used_ucx == "system" or _used_ucx == "wheel"
    return _used_ucx


_ucx_context = Context()


class Server:
    def __init__(self):
        self._server = _Server(_ucx_context)

    def listen(self, addr: str, port: int):
        self._server.listen(addr, port)

    def set_accept_cb(self, on_accept: Callable[[_ServerEndpoint], None]):
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

    def asend(
        self,
        client_ep: _ServerEndpoint,
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
        ret._some = inner
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
            ret.get_loop().call_soon_threadsafe(ret.set_result, future.result())

        def cur_fail(future: _ServerRecvFuture):
            ret.get_loop().call_soon_threadsafe(
                ret.set_exception, Exception(future.exception())
            )

        ret._some = self._server.recv(buffer, tag, tag_mask, cur_send, cur_fail)
        return ret

    # def evaluate_perf(self, client_ep: _ServerEndpoint, msg_size: int) -> float:
    # return self._server.evaluate_perf(client_ep, msg_size)


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
            ret.get_loop().call_soon_threadsafe(ret.set_result, future.result())

        def cur_fail(future: _ClientRecvFuture):
            ret.get_loop().call_soon_threadsafe(
                ret.set_exception, Exception(future.exception())
            )

        ret._some = self._client.recv(buffer, tag, tag_mask, cur_send, cur_fail)
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

        ret._some = self._client.send(buffer, tag, cur_send, cur_fail)
        return ret

    # def evaluate_perf(self, msg_size: int) -> float:
    # return self._client.evaluate_perf(msg_size)


__all__ = [
    "Server",
    "Client",
    "check_sys_libs",
    # "ucp_get_version",
]
