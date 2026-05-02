import asyncio as aio

from simio import net, stream, tls


async def main() -> None:
    hostname = "www.wikipedia.org"

    async with await net.open_tcp_stream(net.IPv4Address(hostname, 443), socket=net.TcpSocketInet()) as tcp_stream:
        async with tls.open_tls_stream(tcp_stream, server_side=False, server_hostname=hostname) as tls_stream:
            http_stream = stream.BufferedStream(tls_stream)
            await http_stream.write_all(
                b'GET / HTTP/1.0\r\n'
                b'Host: www.wikipedia.org\r\n'
                b'User-Agent: simio\r\n'
                b'\r\n',
            )
            status = await http_stream.read_until(b'\r\n')
            print(status.decode())

            raw_headers = await http_stream.read_until(b'\r\n\r\n')
            headers: dict[bytes, bytes] = {}
            for line in raw_headers.removesuffix(b'\r\n\r\n').split(b'\r\n'):
                key, value = line.split(b':', maxsplit=1)
                headers[key.lower()] = value.lstrip()

            body = await http_stream.read_exactly(size=int(headers[b'content-length']))
            print(body.decode())


aio.run(main())
