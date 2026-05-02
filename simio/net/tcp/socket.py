import asyncio as aio
import socket as sc
from typing import ClassVar, Optional, Self, Union

from simio.net.address import IPv4Address, IPv6Address, SocketAddress
from simio.net.socket import Socket


class TcpSocket[AddressT: SocketAddress](Socket[AddressT]):
    """
    Asynchronous TCP socket.

    :param proto: socket type protocol
    :param raw: standard library socket
    """

    ADDRESS_TYPE: ClassVar[type[AddressT]]

    def __init__(self, proto: int = -1, raw: Optional[sc.socket] = None):
        super().__init__(sc.SocketKind.SOCK_STREAM, proto, raw)

    async def recv(self, max_bytes: int) -> bytes:
        loop = aio.get_running_loop()

        return await loop.sock_recv(self._raw, max_bytes)

    async def recv_into(self, buf: bytearray) -> int:
        loop = aio.get_running_loop()

        return await loop.sock_recv_into(self._raw, buf)

    async def sendall(self, data: Union[bytes, bytearray, memoryview]) -> None:
        loop = aio.get_running_loop()

        return await loop.sock_sendall(self._raw, data)

    def bind(self, address: AddressT) -> None:
        return self._raw.bind(address.raw())

    def listen(self, backlog: int = -1) -> None:
        return self._raw.listen(backlog)

    async def connect(self, address: AddressT) -> None:
        loop = aio.get_running_loop()

        return await loop.sock_connect(self._raw, address.raw())

    async def accept(self) -> tuple[Self, AddressT]:
        loop = aio.get_running_loop()

        socket, address = await loop.sock_accept(self._raw)

        self_cls = type(self)
        return self_cls(raw=socket), address


class TcpSocketInet(TcpSocket[IPv4Address]):
    """
    Asynchronous INET family socket.
    """

    ADDRESS_TYPE = IPv4Address


class TcpSocketInet6(TcpSocket[IPv6Address]):
    """
    Asynchronous INET6 family socket.
    """

    ADDRESS_TYPE = IPv6Address
