import asyncio as aio

from simio.memory import MemoryPipe, Stream, StreamClosed


async def test_memory_pipe():
    async def client(stream: Stream) -> tuple[str, str]:
        out_data = ["hello", " ", "world", "!!!"]
        for chunk in out_data:
            await stream.write(chunk.encode())
        await stream.close_writer()

        in_data = []
        while True:
            try:
                chunk = await stream.read(1024)
                in_data.append(chunk.decode())
            except StreamClosed:
                break

        return "".join(out_data), "".join(in_data)

    async def echo_server(stream: Stream) -> None:
        in_data = []
        while True:
            try:
                in_data.append(await stream.read(max_bytes=1024))
            except StreamClosed:
                break

        for chunk in in_data:
            await stream.write(chunk)
        await stream.close_writer()

    pipe = MemoryPipe(buf_size=5)
    async with pipe as (cli_stream, srv_stream):
        cli_task = aio.create_task(client(cli_stream))
        srv_task = aio.create_task(echo_server(srv_stream))
        await aio.wait((cli_task, srv_task), return_when=aio.FIRST_EXCEPTION)

    sent_data, recv_data = await cli_task
    assert sent_data == recv_data
