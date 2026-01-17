import abc
from types import TracebackType
from typing import Optional, Self, Union

from simio.buffer import Buffer, BufferOverflowError, DequeBuffer


class IncompleteError(Exception):
    """
    Raised when the underlying buffer is closed before requested number of bytes received
    or provided delimiter is found.
    """


class StreamReader(abc.ABC):
    """
    Asynchronous stream reader.
    """

    @abc.abstractmethod
    async def read(self, max_bytes: int) -> bytes:
        """
        Reads max bytes from the stream.

        :param max_bytes: max bytes to read
        :return: read bytes
        """

    @abc.abstractmethod
    async def close_reader(self) -> None:
        """
        Closes reader end of the stream.
        """


class BufferedStreamReader(StreamReader):
    """
    Asynchronous buffered stream reader.

    :param reader: inner unbuffered reader
    :param buffer: buffer instance
    """

    def __init__(self, reader: StreamReader, buffer: Optional[Buffer] = None):
        self._reader = reader
        self._read_buffer: Buffer = buffer if buffer is not None else DequeBuffer()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
            self,
            exc_type: Optional[type[Exception]],
            exc_val: Optional[Exception],
            exc_tb: Optional[TracebackType],
    ) -> bool:
        await self.close_reader()
        return False

    async def read(self, max_bytes: int) -> bytes:
        assert max_bytes > 0, "max_bytes must be positive"

        if len(self._read_buffer) != 0:
            data = self._read_buffer.pop(max_bytes)
        else:
            data = await self._reader.read(max_bytes)

        return data

    async def read_exactly(self, size: int) -> bytes:
        """
        Reads the exact number of bytes from the stream.

        :param size: bytes to read
        :return: read bytes
        """

        assert size > 0, "size must be positive"

        while (remain := size - len(self._read_buffer)) > 0:
            # check if the buffer is full before reading the stream to prevent data loss
            if self._read_buffer.is_full():
                raise BufferOverflowError()

            if self._read_buffer.max_size is not None:
                remain = min(remain, self._read_buffer.max_size - self._read_buffer.size)

            if not (chunk := await self._reader.read(remain)):
                raise IncompleteError()

            self._read_buffer.append(chunk)

        return self._read_buffer.pop(size)

    async def read_until(self, sep: bytes, chunk_size: int = 1024) -> bytes:
        """
        Reads until the separator is found in the stream.

        :param sep: separator to be found
        :param chunk_size: the size of a chunk by which the data will be read from the stream

        :return: read bytes including separator
        """

        if self._read_buffer:
            data = self._read_buffer.pop()
            before, found_sep, after = data.partition(sep)
            if found_sep:
                self._read_buffer.append(after)
                return before + found_sep
            else:
                self._read_buffer.append(before)

        while True:
            # check if the buffer is full before reading the stream to prevent data loss
            if self._read_buffer.is_full():
                raise BufferOverflowError()

            if self._read_buffer.max_size is not None:
                max_bytes = min(chunk_size, self._read_buffer.max_size - self._read_buffer.size)
            else:
                max_bytes = chunk_size

            if not (data := await self._reader.read(max_bytes)):
                raise IncompleteError()

            self._read_buffer.append(data)

            data = self._read_buffer.pop()
            before, found_sep, after = data.partition(sep)
            if found_sep:
                self._read_buffer.append(after)
                return before + found_sep
            else:
                self._read_buffer.append(before)

    async def peek(self, size: int) -> bytes:
        """
        Reads `size` bytes from the stream keeping them in the stream for the consequent reads.

        :param size: bytes to be peeked
        :return: peeked bytes
        """

        assert size > 0, "size must be positive"

        while len(self._read_buffer) < size:
            if not (chunk := await self._reader.read(size - len(self._read_buffer))):
                raise IncompleteError()

            self._read_buffer.append(chunk)

        return self._read_buffer.copy(size)

    def get_buffer(self) -> bytes:
        """
        Drains internal buffer and returns its data.

        :return: data kept in the buffer
        """

        return self._read_buffer.pop()

    async def close_reader(self) -> None:
        """
        Closes reader end of the stream.
        """

        await self._reader.close_reader()


class StreamWriter(abc.ABC):
    """
    Asynchronous stream writer.
    """

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
            self,
            exc_type: Optional[type[Exception]],
            exc_val: Optional[Exception],
            exc_tb: Optional[TracebackType],
    ) -> bool:
        await self.close_writer()
        return False

    @abc.abstractmethod
    async def write(self, data: Union[bytes, bytearray, memoryview]) -> int:
        """
        Writes data to the stream.

        :param data: data to be written
        :return: number of written bytes
        """

    @abc.abstractmethod
    async def close_writer(self) -> None:
        """
        Closes writer end of the stream.
        """

    @abc.abstractmethod
    async def close(self) -> None:
        """
        Closes the stream.
        """

    async def write_all(self, data: Union[bytes, bytearray, memoryview]) -> None:
        """
        Writes all the data to the steam.
        :param data: data to be written
        """

        if not isinstance(data, memoryview):
            data = memoryview(data)

        while (written := await self.write(data)) != len(data):
            data = data[written:]


class BufferedStreamWriter(StreamWriter):
    """
    Asynchronous buffered stream writer.

    :param writer: inner unbuffered writer
    :param buffer: buffer instance
    """

    def __init__(self, writer: StreamWriter, buffer: Optional[Buffer] = None):
        self._writer = writer
        self._write_buffer: Buffer = buffer if buffer is not None else DequeBuffer()

    async def write(self, data: Union[bytes, bytearray, memoryview]) -> int:
        """
        Writes data to the stream

        :param data: data to be written
        :return: number of written bytes
        """

        await self.drain()
        return await self._writer.write(data)

    async def close_writer(self) -> None:
        """
        Closes writer end of the stream.
        """

        await self.drain()
        return await self._writer.close_writer()

    async def close(self) -> None:
        """
        Closes the stream.
        """

        await self.drain()
        return await self._writer.close()

    def feed(self, data: Union[bytes, bytearray, memoryview]) -> None:
        """
        Writes data to the internal buffer without sending it to the stream

        :param data: data to be written
        """

        self._write_buffer.append(data)

    async def drain(self) -> None:
        """
        Sends the data from the internal buffer to the stream.
        """

        data = self._write_buffer.pop()
        while (written := await self._writer.write(data)) != len(data):
            data = data[written:]


class Stream(StreamReader, StreamWriter, abc.ABC):
    """
    Asynchronous stream.
    """

    async def __aexit__(
            self,
            exc_type: Optional[type[Exception]],
            exc_val: Optional[Exception],
            exc_tb: Optional[TracebackType],
    ) -> bool:
        await self.close()
        return False


class BufferedStream(Stream, BufferedStreamReader, BufferedStreamWriter):
    """
    Asynchronous buffered stream.
    """

    def __init__(self, stream: Stream, read_buffer: Optional[Buffer] = None, write_buffer: Optional[Buffer] = None):
        BufferedStreamReader.__init__(self, stream, buffer=read_buffer)
        BufferedStreamWriter.__init__(self, stream, buffer=write_buffer)
