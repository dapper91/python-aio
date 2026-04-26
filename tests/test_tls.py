import ssl

from simio import memory, tls


async def test_ssl_stream():
    pipe = memory.MemoryPipe()

    async with pipe as (cli_stream, srv_stream):
        srv_stream = tls.TlsStream(
            stream=srv_stream,
            ssl_context=ssl.create_default_context(ssl.Purpose.CLIENT_AUTH),
            server_side=True,
        )
        cli_stream = tls.TlsStream(
            stream=cli_stream,
            ssl_context=ssl.create_default_context(ssl.Purpose.SERVER_AUTH),
            server_side=False,
        )

        await cli_stream.write_all(b"0123456789")
        assert await srv_stream.read(10) == b"0123456789"

        await srv_stream.write_all(b"abcdefg")
        assert await cli_stream.read(7) == b"abcdefg"
