import asyncio as aio
import socket as sc
from types import TracebackType
from typing import Optional, Self, Union

from simio.stream import Stream


class TcpStream(Stream):
    """
    TCP stream.
    """

    def __init__(self, socket: sc.socket):
        assert not socket.getblocking(), "socket must be in non-blocking mode"

        self._socket = socket

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
            self,
            exc_type: Optional[type[Exception]],
            exc_val: Optional[Exception],
            exc_tb: Optional[TracebackType],
    ) -> bool:
        await self.close()
        return False

    @property
    def socket(self) -> sc.socket:
        return self._socket

    async def read(self, max_bytes: int) -> bytes:
        loop = aio.get_running_loop()

        return await loop.sock_recv(self._socket, max_bytes)

    async def write(self, data: Union[bytes, bytearray, memoryview]) -> int:
        loop = aio.get_running_loop()

        await loop.sock_sendall(self._socket, data)
        return len(data)

    async def close_reader(self) -> None:
        self._socket.shutdown(sc.SHUT_RD)

    async def close_writer(self) -> None:
        self._socket.shutdown(sc.SHUT_WR)

    async def close(self) -> None:
        self._socket.close()


async def open_tcp_stream(
        host: str,
        port: int,
        family: sc.AddressFamily = sc.AddressFamily.AF_INET,
        proto: int = -1,
        bind: Optional[tuple[str, int]] = None,
) -> TcpStream:
    """
    Opens a tcp connection.

    :param host: hostname to connect to
    :param port: port to connect to
    :param family: address family
    :param proto: protocol
    :param bind: local host/port pair to bind to
    :return: tcp stream
    """

    loop = aio.get_running_loop()

    socket = sc.socket(family=family, type=sc.SocketKind.SOCK_STREAM, proto=proto)
    try:
        socket.setblocking(False)
        if bind:
            socket.bind(bind)

        await loop.sock_connect(socket, (host, port))

    except BaseException:
        socket.close()
        raise

    return TcpStream(socket)
