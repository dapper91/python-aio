import ssl
from types import TracebackType
from typing import Callable, Optional, Self, Union

from simio.stream import Stream, StreamReader, StreamWriter


class AsyncSslObject:
    """
    Asynchronous ssl object.

    :param reader: wrapped stream reader
    :param writer: wrapped stream writer
    :param ssl_context: ssl context
    :param server_side: whether the object is of server or client side
    :param server_hostname: hostname of server
    """

    def __init__(
            self,
            reader: StreamReader,
            writer: StreamWriter,
            ssl_context: ssl.SSLContext,
            server_side: bool,
            server_hostname: Optional[str] = None,
            chunk_size: int = 4096,
    ):
        self._reader = reader
        self._writer = writer

        self._chunk_size = chunk_size
        self._buf_read = bio_read = ssl.MemoryBIO()
        self._buf_write = bio_write = ssl.MemoryBIO()
        self._ssl_object = ssl_context.wrap_bio(
            bio_read,
            bio_write,
            server_side=server_side,
            server_hostname=server_hostname,
        )

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
            self,
            exc_type: Optional[type[Exception]],
            exc_val: Optional[Exception],
            exc_tb: Optional[TracebackType],
    ) -> bool:
        await self.unwrap()

        return False

    def drain_read_buffer(self) -> bytes:
        """
        Drains read buffer and returns content.
        """

        return self._buf_read.read()

    def drain_write_buffer(self) -> bytes:
        """
        Drains write buffer and returns content.
        """

        return self._buf_write.read()

    async def do_handshake(self) -> None:
        """
        Does ssl handshake.
        """

        await self._sync_to_async(self._ssl_object.do_handshake)

    async def read(self, max_bytes: int) -> bytes:
        """
        Reads data from the stream decrypting it using provided ssl context.

        :param max_bytes: max bytes to be read
        """

        return await self._sync_to_async(self._ssl_object.read, len=max_bytes)

    async def write(self, data: Union[bytes, bytearray, memoryview]) -> int:
        """
        Writes data to the stream encrypting it using provided ssl context.

        :param data: data to be written
        :returns: number of bytes written
        """

        return await self._sync_to_async(self._ssl_object.write, data=data)

    async def unwrap(self) -> None:
        """
        Does ssl shutdown.
        """

        await self._sync_to_async(self._ssl_object.unwrap)

        self._buf_read.write_eof()
        self._buf_write.write_eof()

    async def _sync_to_async[**ParamsT, ResT](
            self,
            wrapped: Callable[ParamsT, ResT],
            *args: ParamsT.args,
            **kwargs: ParamsT.kwargs,
    ) -> ResT:
        while True:
            try:
                result = wrapped(*args, **kwargs)
            except ssl.SSLWantReadError:
                if data := await self._reader.read(self._chunk_size):
                    self._buf_read.write(data)
                else:
                    self._buf_read.write_eof()

            except ssl.SSLWantWriteError:
                await self._writer.write(self._buf_write.read())

            else:
                return result


class TlsStream(Stream):
    """
    SSL encrypted stream.

    :param stream: inner stream to be encrypted/decrypted
    :param ssl_context: ssl context
    """

    def __init__(
            self,
            stream: Stream,
            ssl_context: ssl.SSLContext,
            server_side: bool,
            server_hostname: Optional[str] = None,
    ):
        self._ssl_context = ssl_context
        self._ssl_object = AsyncSslObject(
            reader=stream,
            writer=stream,
            ssl_context=ssl_context,
            server_side=server_side,
            server_hostname=server_hostname,
        )

    @property
    def ssl_context(self) -> ssl.SSLContext:
        return self._ssl_context

    async def __aenter__(self) -> Self:
        await self.do_handshake()
        return self

    async def __aexit__(
            self,
            exc_type: Optional[type[Exception]],
            exc_val: Optional[Exception],
            exc_tb: Optional[TracebackType],
    ) -> bool:
        await self.close()

        return False

    async def do_handshake(self) -> None:
        await self._ssl_object.do_handshake()

    async def read(self, max_bytes: int) -> bytes:
        return await self._ssl_object.read(max_bytes)

    async def close_reader(self) -> None:
        pass

    async def write(self, data: Union[bytes, bytearray, memoryview]) -> int:
        return await self._ssl_object.write(data)

    async def close_writer(self) -> None:
        pass

    async def close(self) -> None:
        await self._ssl_object.unwrap()
