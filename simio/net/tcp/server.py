import asyncio as aio
import logging
import socket as sc
from types import TracebackType
from typing import Any, Callable, Coroutine, Optional, Self

from simio.net import utils

logger = logging.getLogger(__package__)


async def start_tcp_server(
        host: str,
        port: int,
        handler: Callable[[sc.socket], Coroutine[None, None, None]],
        *,
        family: sc.AddressFamily = sc.AddressFamily.AF_INET,
        backlog: int = 128,
        reuse_address: bool = True,
        graceful_shutdown: Optional[float] = None,
) -> None:
    """
    Starts TCP socket server.

    :param host: address to bind to
    :param port: port to bind to
    :param handler: client socket handler
    :param family: address family
    :param backlog: number of unaccepted connections that the system will allow before refusing new connections
    :param reuse_address: reuse bind address
    :param graceful_shutdown: period of time during which the server waits for handlers to complete
                              before canceling them
    """

    loop = aio.get_running_loop()

    logger.info("starting server on %s:%d", host, port)

    with sc.socket(family, sc.SOCK_STREAM.SOCK_STREAM) as srv_socket:
        if reuse_address:
            srv_socket.setsockopt(sc.SOL_SOCKET, sc.SO_REUSEADDR, 1)

        srv_socket.setblocking(False)
        srv_socket.bind((host, port))
        srv_socket.listen(backlog)

        async with TaskGuard(graceful_shutdown=graceful_shutdown) as handlers:
            while True:
                try:
                    cli_socket, address = await loop.sock_accept(srv_socket)
                except aio.CancelledError:
                    break

                handlers.create_task(_handle_client_socket(handler, cli_socket))


async def _handle_client_socket(handler: Callable[[sc.socket], Coroutine[None, None, None]], socket: sc.socket) -> None:
    remote_host, remote_port = utils.get_socket_peer_address(socket)
    logger.info("client %s:%d connected", remote_host, remote_port)

    with socket:
        try:
            await handler(socket)
        except Exception as e:
            logger.exception("client %s:%d handling error: %s", remote_host, remote_port, e)

    logger.info("client %s:%d disconnected", remote_host, remote_port)


class TaskGuard:
    def __init__(self, graceful_shutdown: Optional[float] = None):
        self._graceful_shutdown = graceful_shutdown
        self._tasks: set[aio.Task[None]] = set()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
            self,
            exc_type: Optional[type[Exception]],
            exc_val: Optional[Exception],
            exc_tb: Optional[TracebackType],
    ) -> bool:
        if not self._tasks:
            return False

        done, pending = await aio.wait(self._tasks, timeout=self._graceful_shutdown)
        for task in done:
            self._on_task_done(task)
        for task in pending:
            task.cancel()

        for task in pending:
            try:
                await task
            except Exception as exc:
                logger.exception("task %s raised an exception: %s", task.get_name(), exc)
            finally:
                self._on_task_done(task)

        return False

    def create_task(self, coro: Coroutine[Any, Any, None], name: Optional[str] = None) -> aio.Task[None]:
        task = aio.create_task(coro, name=name)
        task.add_done_callback(self._on_task_done)

        self._tasks.add(task)

        return task

    def _on_task_done(self, task: aio.Task[None]) -> None:
        self._tasks.discard(task)
