import asyncio as aio
import logging
import socket as sc

from simio import net


async def echo(socket: sc.socket) -> None:
    loop = aio.get_running_loop()

    peer_address, peer_port = net.get_socket_peer_address(socket)
    logging.info("client %s:%d connected", peer_address, peer_port)

    data = await loop.sock_recv(socket, 1024)
    await loop.sock_sendall(socket, data)


async def main() -> None:
    logging.basicConfig(level=logging.INFO)

    await net.start_tcp_server("127.0.0.1", 8080, echo, graceful_shutdown=10.0)


aio.run(main())
