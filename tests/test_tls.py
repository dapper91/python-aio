import asyncio as aio
import pathlib as pl
import ssl

from simio import memory as mem
from simio import tls
from simio.stream import Stream


async def test_ssl_stream(resource_dir: pl.Path):
    async def client(stream: Stream, server_cert: pl.Path) -> tuple[bytes, bytes]:
        ssl_context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=server_cert)
        async with tls.TlsStream(
                stream=stream,
                ssl_context=ssl_context,
                server_side=False,
                server_hostname='test.org',
        ) as tls_stream:
            out_data = b"hello world!!!\0"
            await tls_stream.write(out_data)

            in_data = []
            while True:
                chunk = await tls_stream.read(1024)
                in_data.append(chunk)
                if b'\0' in chunk:
                    break

        return out_data, b"".join(in_data)

    async def echo_server(stream: Stream, server_cert: pl.Path, server_key: pl.Path) -> None:
        ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        ssl_context.load_cert_chain(certfile=server_cert, keyfile=server_key)

        async with tls.TlsStream(
                stream=stream,
                ssl_context=ssl_context,
                server_side=True,
        ) as tls_stream:
            in_data = []
            while True:
                data = await tls_stream.read(max_bytes=1024)
                in_data.append(data)
                if b'\0' in data:
                    break

            for chunk in in_data:
                await tls_stream.write(chunk)

    server_cert_file = resource_dir / 'test.org.crt'
    server_key_file = resource_dir / 'test.org.key'

    async with mem.MemoryPipe() as (cli_stream, srv_stream):
        async with aio.TaskGroup() as tasks:
            tasks.create_task(echo_server(srv_stream, server_cert_file, server_key_file))

            cli_task = tasks.create_task(client(cli_stream, server_cert_file))

    sent_data, recv_data = await cli_task
    assert sent_data == recv_data
