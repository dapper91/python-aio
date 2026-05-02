[![Downloads][download-badge]][download-url]
[![License][licence-badge]][licence-url]
[![Python Versions][python-version-badge]][python-version-url]
[![Build status][build-badge]][build-url]

[download-badge]: https://static.pepy.tech/personalized-badge/simio?period=month&units=international_system&left_color=grey&right_color=orange&left_text=Downloads/month
[download-url]: https://pepy.tech/project/simio
[licence-badge]: https://img.shields.io/badge/license-Unlicense-blue.svg
[licence-url]: https://github.com/dapper91/simio/blob/master/LICENSE
[python-version-badge]: https://img.shields.io/pypi/pyversions/simio.svg
[python-version-url]: https://pypi.org/project/simio
[build-badge]: https://github.com/dapper91/simio/actions/workflows/test.yml/badge.svg?branch=master
[build-url]: https://github.com/dapper91/simio/actions/workflows/test.yml

# simio

Python simple lightweight zero-dependency asynchronous IO library.

## Motivation

Python 3.4 introduced native support for asynchronous code and announced
[asyncio](https://docs.python.org/3/library/asyncio.html) standard library. async/await syntax provided a very
convenient way to write single-threaded concurrent code but asyncio library itself caused a lot of pain to developers
since it was ugly designed and provides very inconvenient api and asynchronous primitives. To fix that problem
developers implemented some third-party libraries to replace the standard one like [trio](https://trio.readthedocs.io).

You may use [trio](https://trio.readthedocs.io) or
[anyio](https://anyio.readthedocs.io), but trio implements its own runtime, anyio although work on top
of asyncio but provides too high level interface which may be undesirable for small projects.

This library is intended to solve some of that tensions.

## Features

* Buffered streams
* TCP stream
* TCP server

## Installation

You can install simio with pip:

```shell
pip install simio
```

## Quickstart

### Buffered stream:

```python
import asyncio as aio

from simio import net, stream


async def main() -> None:
    async with await net.open_tcp_stream(
            address=net.IPv4Address("httpforever.com", 80),
            socket=net.TcpSocketInet(),
    ) as http_stream:
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


aio.run(main())

```

### Echo TCP server:

```python
import asyncio as aio
import logging

from simio import net


async def echo(socket: net.TcpSocket[net.IPv4Address]) -> None:
    data = await socket.recv(1024)
    await socket.sendall(data)


async def main() -> None:
    logging.basicConfig(level=logging.INFO)

    await net.start_tcp_server(
        net.IPv4Address("127.0.0.1", 8080),
        srv_socket=net.TcpSocketInet(),
        handler=echo,
        graceful_shutdown=10.0,
    )


aio.run(main())

```

### TCP client stream:

```python
import asyncio as aio

from simio import net


async def main() -> None:
    async with await net.open_tcp_stream(
            address=net.IPv4Address("127.0.0.1", 8080),
            socket=net.TcpSocketInet(),
    ) as tcp_stream:
        await tcp_stream.write(b"Hello World!!!")

        while data := await tcp_stream.read(1024):
            print(data)


aio.run(main())

```


### TLS client stream:

```python
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

```
