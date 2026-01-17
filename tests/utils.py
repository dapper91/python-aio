from typing import Optional, Union

from simio.buffer import DequeBuffer
from simio.stream import StreamReader, StreamWriter


class StreamClosedError(Exception):
    pass


class MemoryStream(StreamReader, StreamWriter):
    def __init__(self, read_chunk_size: Optional[int] = None, write_chunk_size: Optional[int] = None):
        self._read_chunk_size = read_chunk_size
        self._write_chunk_size = write_chunk_size
        self._buffer = DequeBuffer()
        self._reader_closed = False
        self._writer_closed = False

    async def read(self, max_bytes: int) -> bytes:
        if self._reader_closed:
            raise StreamClosedError()

        return self._buffer.pop(self._read_chunk_size or max_bytes)

    async def write(self, data: Union[bytes, bytearray, memoryview]) -> int:
        if self._writer_closed:
            raise StreamClosedError()

        if self._write_chunk_size:
            data = memoryview(data)[0:self._write_chunk_size]

        self._buffer.append(data)

        return len(data)

    async def close_reader(self) -> None:
        self._reader_closed = True

    async def close_writer(self) -> None:
        self._writer_closed = True

    async def close(self) -> None:
        self._reader_closed = True
        self._writer_closed = True
