import socket as sc
from typing import Any, Optional, Union

from simio.net.address import SocketAddress
from simio.stream import Stream, StreamClosed

from .socket import TcpSocket


class TcpStream[TcpSocketT: TcpSocket[Any]](Stream):
    """
    TCP stream.
    """

    def __init__(self, socket: TcpSocketT):
        self._socket = socket

    @property
    def socket(self) -> TcpSocketT:
        return self._socket

    async def read(self, max_bytes: int) -> bytes:
        try:
            data = await self._socket.recv(max_bytes)
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError) as e:
            raise StreamClosed() from e

        if not data:
            raise StreamClosed()

        return data

    async def write(self, data: Union[bytes, bytearray, memoryview]) -> int:
        try:
            await self._socket.sendall(data)
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError) as e:
            raise StreamClosed() from e

        return len(data)

    async def close_reader(self) -> None:
        self._socket.shutdown(sc.SHUT_RD)

    async def close_writer(self) -> None:
        self._socket.shutdown(sc.SHUT_WR)

    async def close(self) -> None:
        self._socket.close()


async def open_tcp_stream[AddressT: SocketAddress](
        address: AddressT,
        socket:  TcpSocket[AddressT],
        bind: Optional[AddressT] = None,
) -> TcpStream[TcpSocket[AddressT]]:
    """
    Opens a tcp connection.

    :param address: address to connect to
    :param socket: client socket
    :param bind: local host/port pair to bind to
    :return: tcp stream
    """

    try:
        if bind:
            socket.bind(bind)
        await socket.connect(address)
    except BaseException:
        socket.close()
        raise

    return TcpStream(socket)
