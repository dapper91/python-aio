import abc
from collections import deque
from collections.abc import Sized
from typing import Optional


class BufferOverflowError(Exception):
    """
    Buffer max size exceeded.
    """


class Buffer(abc.ABC, Sized):
    """
    Buffer interface.
    """

    def __bool__(self) -> bool:
        return len(self) > 0

    def __len__(self) -> int:
        return self.size

    def is_full(self) -> bool:
        """
        Returns True is the buffer is full.
        """

        return self.max_size is not None and self.max_size == self.size

    @property
    @abc.abstractmethod
    def size(self) -> int:
        """
        Returns buffer size.
        """

    @property
    @abc.abstractmethod
    def max_size(self) -> Optional[int]:
        """
        Returns buffer max size.
        """

    @abc.abstractmethod
    def append(self, data: bytes) -> None:
        """
        Appends data to the buffer.

        :param data: data to be added
        """

    @abc.abstractmethod
    def pop(self, max_bytes: Optional[int] = None) -> bytes:
        """
        Pops data from the buffer.

        :param max_bytes: max bytes to be popped
        """

    @abc.abstractmethod
    def copy(self, max_bytes: Optional[int] = None) -> bytes:
        """
        Returns `max_bytes` bytes from the buffer keeping them in the buffer.

        :param max_bytes: max bytes to be copied
        :return: copied bytes
        """


class DequeBuffer(Buffer):
    """
    Dequeue buffer implementation.

    :param max_size: buffer max size
    """

    def __init__(self, max_size: Optional[int] = None) -> None:
        self._buffer: deque[bytes] = deque()
        self._buffer_size = 0
        self._max_size = max_size

    @property
    def size(self) -> int:
        return self._buffer_size

    @property
    def max_size(self) -> Optional[int]:
        return self._max_size

    def append(self, data: bytes) -> None:
        if self._max_size is not None and self._buffer_size + len(data) > self._max_size:
            raise BufferOverflowError()

        self._buffer.append(data)
        self._buffer_size += len(data)

    def pop(self, max_bytes: Optional[int] = None) -> bytes:
        if max_bytes is None:
            data = b"".join(self._buffer)
            self._buffer.clear()
            self._buffer_size = 0
            return data
        else:
            stash: list[bytes] = []
            stash_size: int = 0
            while self._buffer_size and stash_size < max_bytes:
                data = self._buffer.popleft()
                self._buffer_size -= len(data)
                stash.append(data)
                stash_size += len(data)

            data = b"".join(stash)
            if len(data) > max_bytes:
                data, tail = data[0:max_bytes], data[max_bytes:]
                self._buffer.appendleft(tail)
                self._buffer_size += len(tail)

            return data

    def copy(self, max_bytes: Optional[int] = None) -> bytes:
        data = self.pop(max_bytes)
        self._buffer.appendleft(data)
        self._buffer_size += len(data)

        return data


class CircularBuffer(Buffer):
    """
    Circular (ring) buffer implementation.

    :param capacity: buffer capacity
    """

    def __init__(self, capacity: int) -> None:
        self._buffer = bytearray(capacity)
        self._begin_idx = 0
        self._size = 0

    @property
    def size(self) -> int:
        return self._size

    @property
    def capacity(self) -> int:
        return len(self._buffer)

    @property
    def max_size(self) -> int:
        return self.capacity

    def append(self, data: bytes) -> None:
        if self.size + len(data) > self.capacity:
            raise BufferOverflowError()

        end_idx = (self._begin_idx + self._size) % len(self._buffer)
        head_len = min(len(data), len(self._buffer) - end_idx)
        tail_len = len(data) - head_len

        self._buffer[end_idx:end_idx + head_len] = data[0:head_len]
        self._buffer[0:tail_len] = data[head_len:len(data)]
        self._size += len(data)

    def pop(self, max_bytes: Optional[int] = None) -> bytes:
        data = self.copy(max_bytes)
        self._begin_idx = (self._begin_idx + len(data)) % len(self._buffer)
        self._size -= len(data)

        return data

    def copy(self, max_bytes: Optional[int] = None) -> bytes:
        buffer_view = memoryview(self._buffer)
        max_bytes = min(max_bytes or self._size, self._size)
        begin_idx, end_idx = self._begin_idx, (self._begin_idx + self._size) % len(self._buffer)

        data = bytearray()
        head = buffer_view[begin_idx:min(begin_idx + max_bytes, len(self._buffer))]
        data.extend(head)
        tail = buffer_view[0:min(max_bytes - len(head), end_idx)]
        data.extend(tail)

        return bytes(data)
