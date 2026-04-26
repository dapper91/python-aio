from types import TracebackType
from typing import Optional, Union

from simio.buffer import DequeBuffer
from simio.queue import Queue
from simio.stream import Stream, StreamClosed


class MemoryChannel:
    """
    In-memory data channel (unidirectional data stream).
    """

    def __init__(self, queue: Queue):
        self._queue = queue
        self._closed = False

    async def read(self, max_bytes: int) -> bytes:
        """
        Reads from the channel.
        """

        if self._queue.size == 0 and self._closed:
            raise StreamClosed()

        return await self._queue.pop(max_bytes)

    async def write(self, data: Union[bytes, bytearray, memoryview]) -> int:
        """
        Writes to the channel.
        """

        if self._closed:
            raise StreamClosed()

        await self._queue.push(bytes(data))

        return len(data)

    async def close(self) -> None:
        """
        Closes the channel.
        """

        await self._queue.push(b"")
        self._closed = True


class MemoryStream(Stream):
    """
    In-memory data stream.
    """

    def __init__(self, in_channel: MemoryChannel, out_channel: MemoryChannel):
        self._in_channel = in_channel
        self._out_channel = out_channel

    async def read(self, max_bytes: int) -> bytes:
        return await self._in_channel.read(max_bytes)

    async def close_reader(self) -> None:
        await self._in_channel.close()

    async def write(self, data: Union[bytes, bytearray, memoryview]) -> int:
        await self._out_channel.write(bytes(data))

        return len(data)

    async def close_writer(self) -> None:
        await self._out_channel.close()

    async def close(self) -> None:
        await self.close_reader()
        await self.close_writer()


class MemoryPipe:
    """
    In-memory bidirectional channel between two endpoints.

    :param buf_size: buffer size
    """

    def __init__(self, buf_size: int = 4096):
        self._l2r_channel = MemoryChannel(Queue(DequeBuffer(max_size=buf_size)))
        self._r2l_channel = MemoryChannel(Queue(DequeBuffer(max_size=buf_size)))

        self._left_stream = MemoryStream(in_channel=self._r2l_channel, out_channel=self._l2r_channel)
        self._right_stream = MemoryStream(in_channel=self._l2r_channel, out_channel=self._r2l_channel)

    async def __aenter__(self) -> tuple[Stream, Stream]:
        return self.open()

    async def __aexit__(
            self,
            exc_type: Optional[type[Exception]],
            exc_val: Optional[Exception],
            exc_tb: Optional[TracebackType],
    ) -> bool:
        await self.close()
        return False

    async def close(self) -> None:
        """
        Closes the pipe.
        """

        await self._l2r_channel.close()
        await self._r2l_channel.close()

    def open(self) -> tuple[Stream, Stream]:
        """
        Opens the pipe.
        """

        return self._left_stream, self._right_stream
