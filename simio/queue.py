import asyncio as aio
from typing import Optional

from simio.buffer import Buffer


class QueueFull(Exception):
    """
    Raised when queue is full.
    """


class QueueEmpty(Exception):
    """
    Raised when queue is empty.
    """


class DataTooLarge(Exception):
    """
    Raised when data size is too large for the queue.
    """


class Queue:
    """
    Asynchronous bytes queue.

    :param buffer: internal buffer
    """

    def __init__(self, buffer: Buffer):
        self._buffer = buffer
        self._buffer_changed = aio.Event()

    def __bool__(self) -> bool:
        return len(self) > 0

    def __len__(self) -> int:
        return self._buffer.size

    def is_full(self) -> bool:
        """
        Returns true if queue is full.
        """

        return self._buffer.is_full()

    @property
    def size(self) -> int:
        """
        Returns queue size.
        """

        return self._buffer.size

    @property
    def max_size(self) -> Optional[int]:
        """
        Returns queue max size if provided.
        """

        return self._buffer.max_size

    async def push(self, data: bytes) -> None:
        """
        Push data to the queue. If the queue is full waits until the required space is available.
        """

        if self._buffer.max_size is not None:
            if len(data) > self._buffer.max_size:
                raise DataTooLarge()

            while self._buffer.size + len(data) > self._buffer.max_size:
                await self._buffer_changed.wait()
                self._buffer_changed.clear()

        self._buffer.append(data)
        self._buffer_changed.set()

    def push_nowait(self, data: bytes) -> None:
        """
        Push data to the queue. If the queue is full raises `QueueFull` exception.
        """

        if self._buffer.max_size is not None:
            if len(data) > self._buffer.max_size:
                raise DataTooLarge()

            if self._buffer.size + len(data) > self._buffer.max_size:
                raise QueueFull()

        self._buffer.append(data)
        self._buffer_changed.set()

    async def pop(self, max_bytes: Optional[int] = None) -> bytes:
        """
        Pop data from the queue. If the queue is empty waits until any data is available.
        """

        while self.size == 0:
            await self._buffer_changed.wait()
            self._buffer_changed.clear()

        data = self._buffer.pop(max_bytes)
        self._buffer_changed.set()

        return data

    def pop_nowait(self, max_bytes: Optional[int] = None) -> bytes:
        """
        Pop data from the queue. If the queue is empty raises `QueueEmpty` exception.
        """

        if self.size == 0:
            raise QueueEmpty()

        data = self._buffer.pop(max_bytes)
        self._buffer_changed.set()

        return data
