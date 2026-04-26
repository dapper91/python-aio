import asyncio as aio

import pytest

from simio.buffer import CircularBuffer
from simio.queue import Queue, QueueEmpty, QueueFull


async def test_queue_push_no_wait():
    queue = Queue(buffer=CircularBuffer(capacity=3))

    queue.push_nowait(b"123")

    assert queue.size == 3
    assert queue.max_size == 3
    assert queue.is_full() is True

    with pytest.raises(QueueFull):
        queue.push_nowait(b"4")


async def test_queue_pop_no_wait():
    queue = Queue(buffer=CircularBuffer(capacity=3))

    queue.push_nowait(b"123")

    assert queue.pop_nowait(3) == b"123"
    assert queue.max_size == 3
    assert queue.size == 0
    assert queue.is_full() is False

    with pytest.raises(QueueEmpty):
        queue.pop_nowait()


async def test_queue_push_pop():
    queue = Queue(buffer=CircularBuffer(capacity=5))
    done = aio.Event()

    async def sender() -> bytes:
        data = [
            b"hello",
            b" ",
            b"world",
            b"!!!",
        ]
        for item in data:
            await queue.push(item)

        done.set()

        return b"".join(data)

    async def receiver() -> bytes:
        data = []
        while not done.is_set() or queue.size != 0:
            data.append(await queue.pop())

        return b"".join(data)

    sender = aio.create_task(sender())
    receiver = aio.create_task(receiver())
    await aio.wait((sender, receiver), return_when=aio.FIRST_EXCEPTION)

    sent_data, recv_data = await sender, await receiver
    assert sent_data == recv_data
