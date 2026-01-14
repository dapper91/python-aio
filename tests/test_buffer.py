from aio.buffer import CircularBuffer, DequeBuffer


def test_circular_buffer_capacity():
    buffer = CircularBuffer(capacity=5)
    assert buffer.capacity == 5


def test_circular_buffer_size():
    buffer = CircularBuffer(capacity=5)
    assert buffer.size == 0

    buffer.append(b'12345')
    assert buffer.size == 5

    buffer.pop(2)
    assert buffer.size == 3

    buffer.pop(3)
    assert buffer.size == 0

    buffer.pop(1)
    assert buffer.size == 0

    buffer.append(b'123')
    assert buffer.size == 3


def test_circular_buffer_copy():
    buffer = CircularBuffer(capacity=5)
    buffer.append(b'12345')

    assert buffer.copy(max_bytes=1) == b'1'
    assert buffer.copy(max_bytes=3) == b'123'
    assert buffer.copy(max_bytes=5) == b'12345'
    assert buffer.copy(max_bytes=6) == b'12345'


def test_circular_buffer_append_pop():
    buffer = CircularBuffer(capacity=5)
    buffer.append(b'12345')
    assert buffer.pop() == b'12345'

    buffer.append(b'12345')
    assert buffer.pop(max_bytes=2) == b'12'
    assert buffer.pop(max_bytes=3) == b'345'

    buffer.append(b'12345')
    assert buffer.pop(max_bytes=3) == b'123'
    buffer.append(b'678')
    assert buffer.pop(max_bytes=3) == b'456'
    buffer.append(b'9ab')
    assert buffer.pop(max_bytes=1) == b'7'
    buffer.append(b'c')
    assert buffer.pop(max_bytes=5) == b'89abc'


def test_deque_buffer_pop_all():
    buffer = DequeBuffer()

    buffer.append(b"0")
    buffer.append(b"12")
    buffer.append(b"345")
    buffer.append(b"6789")
    assert len(buffer) == 10

    assert buffer.pop() == b"0123456789"


def test_deque_buffer_pop_max_bytes():
    buffer = DequeBuffer()

    buffer.append(b"0")
    buffer.append(b"12")
    buffer.append(b"345")
    buffer.append(b"6789")
    assert len(buffer) == 10

    assert buffer.pop(max_bytes=4) == b"0123"
    assert len(buffer) == 6

    assert buffer.pop(max_bytes=3) == b"456"
    assert len(buffer) == 3

    assert buffer.pop(max_bytes=2) == b"78"
    assert len(buffer) == 1

    assert buffer.pop(max_bytes=1) == b"9"
    assert len(buffer) == 0

    assert buffer.pop(max_bytes=1) == b""
    assert len(buffer) == 0


def test_deque_buffer_copy():
    buffer = DequeBuffer()

    buffer.append(b"0")
    buffer.append(b"12")
    buffer.append(b"345")
    buffer.append(b"6789")
    assert len(buffer) == 10

    assert buffer.copy(max_bytes=1) == b"0"
    assert len(buffer) == 10

    assert buffer.copy(max_bytes=2) == b"01"
    assert len(buffer) == 10

    assert buffer.copy(max_bytes=3) == b"012"
    assert len(buffer) == 10

    assert buffer.copy(max_bytes=6) == b"012345"
    assert len(buffer) == 10

    assert buffer.copy(max_bytes=10) == b"0123456789"
    assert len(buffer) == 10
