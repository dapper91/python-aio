import pytest

from simio.buffer import BufferOverflowError, CircularBuffer
from simio.stream import BufferedStreamReader, BufferedStreamWriter, IncompleteError
from tests import utils


async def test_stream_writer_write_all():
    stream = utils.MemoryStream(write_chunk_size=3)

    writer = stream
    await writer.write_all(b"0123456789")

    assert await stream.read(10) == b"0123456789"


async def test_stream_buffered_stream_reader_read_exactly():
    stream = utils.MemoryStream(read_chunk_size=3)
    await stream.write(b"0123456789")

    reader = BufferedStreamReader(reader=stream)
    assert await reader.read_exactly(5) == b"01234"
    assert await reader.read_exactly(3) == b"567"
    assert await reader.read_exactly(2) == b"89"


async def test_stream_buffered_stream_reader_read_exactly_incomplete():
    stream = utils.MemoryStream(read_chunk_size=3)
    await stream.write(b"01234")

    reader = BufferedStreamReader(reader=stream)
    with pytest.raises(IncompleteError):
        assert await reader.read_exactly(6)

    assert reader.get_buffer() == b"01234"


async def test_stream_buffered_stream_reader_peek():
    stream = utils.MemoryStream()
    await stream.write(b"012345")

    reader = BufferedStreamReader(reader=stream)
    assert await reader.peek(size=6) == b"012345"
    assert await reader.peek(size=3) == b"012"
    assert await reader.peek(size=1) == b"0"


async def test_stream_buffered_stream_reader_read_until():
    stream = utils.MemoryStream(read_chunk_size=3)
    await stream.write(b"0000100010010110")

    reader = BufferedStreamReader(reader=stream)
    assert await reader.read_until(b"1") == b"00001"
    assert await reader.read_until(b"1") == b"0001"
    assert await reader.read_until(b"1") == b"001"
    assert await reader.read_until(b"1") == b"01"
    assert await reader.read_until(b"1") == b"1"

    with pytest.raises(IncompleteError):
        await reader.read_until(b"1")

    assert reader.get_buffer() == b"0"


async def test_stream_buffered_stream_read_until_buffer_overflow():
    stream = utils.MemoryStream(read_chunk_size=4)
    await stream.write(b"00001")

    reader = BufferedStreamReader(reader=stream, buffer=CircularBuffer(4))

    with pytest.raises(BufferOverflowError):
        await reader.read_until(b"1")

    assert await reader.read_exactly(4) == b"0000"
    assert await reader.read(1024) == b"1"


async def test_stream_buffered_stream_read_exactly_buffer_overflow():
    stream = utils.MemoryStream(read_chunk_size=4)
    await stream.write(b"00001")

    reader = BufferedStreamReader(reader=stream, buffer=CircularBuffer(4))

    with pytest.raises(BufferOverflowError):
        await reader.read_exactly(5)

    assert await reader.read_exactly(4) == b"0000"
    assert await reader.read(1024) == b"1"


async def test_stream_buffered_writer_write_all():
    stream = utils.MemoryStream(write_chunk_size=3)

    writer = BufferedStreamWriter(writer=stream)
    await writer.write_all(b"0123456789")

    assert await stream.read(10) == b"0123456789"
