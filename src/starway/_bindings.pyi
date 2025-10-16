from collections.abc import Callable
from typing import Annotated

import numpy
from numpy.typing import NDArray

class Context:
    def __init__(self) -> None: ...

class ServerEndpoint:
    @property
    def name(self) -> str: ...
    @property
    def local_addr(self) -> str: ...
    @property
    def local_port(self) -> int: ...
    @property
    def remote_addr(self) -> str: ...
    @property
    def remote_port(self) -> int: ...
    def view_transports(self) -> list[tuple[str, str]]: ...

class Server:
    def __init__(self, ctx: Context) -> None: ...
    def set_accept_callback(
        self, callback: Callable[[ServerEndpoint], None]
    ) -> None: ...
    def listen(self, addr: str, port: int) -> None: ...
    def close(self, callback: Callable[[], None]) -> None: ...
    def send(
        self,
        client_ep: ServerEndpoint,
        buffer: Annotated[NDArray[numpy.uint8], dict(shape=(None,), device="cpu")],
        tag: int,
        done_callback: Callable[[], None],
        fail_callback: Callable[[str], None],
    ) -> None: ...
    def recv(
        self,
        buffer: Annotated[NDArray[numpy.uint8], dict(shape=(None,), device="cpu")],
        tag: int,
        tag_mask: int,
        done_callback: Callable[[int, int], None],
        fail_callback: Callable[[str], None],
    ) -> None: ...
    def flush(
        self, done_callback: Callable[[], None], fail_callback: Callable[[str], None]
    ) -> None: ...
    def flush_ep(
        self,
        client_ep: ServerEndpoint,
        done_callback: Callable[[], None],
        fail_callback: Callable[[str], None],
    ) -> None: ...
    def list_clients(self) -> set[ServerEndpoint]: ...
    def evaluate_perf(self, client_ep: ServerEndpoint, msg_size: int) -> float: ...

class Client:
    def __init__(self, ctx: Context) -> None: ...
    def connect(
        self, addr: str, port: int, callback: Callable[[str], None]
    ) -> None: ...
    def close(self, callback: Callable[[], None]) -> None: ...
    def send(
        self,
        buffer: Annotated[NDArray[numpy.uint8], dict(shape=(None,), device="cpu")],
        tag: int,
        done_callback: Callable[[], None],
        fail_callback: Callable[[str], None],
    ) -> None: ...
    def recv(
        self,
        buffer: Annotated[NDArray[numpy.uint8], dict(shape=(None,), device="cpu")],
        tag: int,
        tag_mask: int,
        done_callback: Callable[[int, int], None],
        fail_callback: Callable[[str], None],
    ) -> None: ...
    def flush(
        self, done_callback: Callable[[], None], fail_callback: Callable[[str], None]
    ) -> None: ...
    def evaluate_perf(self, msg_size: int) -> float: ...
