import asyncio

from aio import net, stream


async def main() -> None:
    async with await net.open_tcp_stream("httpforever.com", 80) as http_stream:
        buffered_stream = stream.BufferedStream(http_stream)
        await buffered_stream.write_all(
            b'GET / HTTP/1.1\r\n'
            b'Host: httpforever.com\r\n'
            b'\r\n',
        )
        status = await buffered_stream.read_until(b'\r\n')
        print(status.decode())

        headers = await buffered_stream.read_until(b'\r\n\r\n')
        headers = dict(line.split(b':', maxsplit=1) for line in headers.removesuffix(b'\r\n\r\n').split(b'\r\n'))

        body = await buffered_stream.read_exactly(size=int(headers[b'Content-Length']))
        print(body.decode())


asyncio.run(main())
