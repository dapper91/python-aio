import asyncio as aio
import logging

from . import Stream, StreamReader, StreamWriter

logger = logging.getLogger(__package__)


async def pipe_streams(src: StreamReader, dst: StreamWriter, buffer_size: int = 1024) -> None:
    """
    Direct data stream from one stream to another.

    :param src: source stream to read data from
    :param dst: destination stream to write data to
    :param buffer_size: buffer size
    """

    try:
        while data := await src.read(max_bytes=buffer_size):
            await dst.write_all(data)
    except (BrokenPipeError, ConnectionResetError):
        logger.debug("connection reset by peer")


async def pipe_streams_bidirectional(str1: Stream, str2: Stream, buffer_size: int = 1024) -> None:
    """
    Binds to streams bidirectionally (reads data from str1 and writes it to str2 and vice versa).

    :param str1: first stream
    :param str2: second stream
    :param buffer_size: buffer size
    """

    try:
        async with aio.TaskGroup() as binders:
            binders.create_task(pipe_streams(str1, str2, buffer_size))
            binders.create_task(pipe_streams(str2, str1, buffer_size))
    except aio.CancelledError:
        pass
    except ExceptionGroup as exc:
        # make verbose exception group message
        raise ExceptionGroup(",".join(str(e) for e in exc.exceptions), exc.exceptions)
