[![Downloads][download-badge]][download-url]
[![License][licence-badge]][licence-url]
[![Python Versions][python-version-badge]][python-version-url]
[![Build status][build-badge]][build-url]

[download-badge]: https://static.pepy.tech/personalized-badge/aio?period=month&units=international_system&left_color=grey&right_color=orange&left_text=Downloads/month
[download-url]: https://pepy.tech/project/aio
[licence-badge]: https://img.shields.io/badge/license-Unlicense-blue.svg
[licence-url]: https://github.com/dapper91/aio/blob/master/LICENSE
[python-version-badge]: https://img.shields.io/pypi/pyversions/aio.svg
[python-version-url]: https://pypi.org/project/aio
[build-badge]: https://github.com/dapper91/aio/actions/workflows/test.yml/badge.svg?branch=master
[build-url]: https://github.com/dapper91/aio/actions/workflows/test.yml

# aio

Python low-level zero-dependency asynchronous IO.

## Motivation

Python 3.4 introduced native support for asynchronous code and announced
[asyncio](https://docs.python.org/3/library/asyncio.html) standard library. async/await syntax provided a very
convenient way to write single-threaded concurrent code but asyncio library itself caused a lot of pain to developers
since it was ugly designed and provides very inconvenient api and asynchronous primitives. To fix that problem
developers implemented some libraries to replace standard one like [trio](https://trio.readthedocs.io),

You may use [trio](https://trio.readthedocs.io) or
[anyio](https://anyio.readthedocs.io), but trio implements its own runtime, anyio although work on top
of asyncio but provides too high level interface which may be undesirable for small projects.
This library is intended to solve some of that tensions.

## Features

* Buffered streams
* TCP stream
* TCP server

## Installation

You can install aio with pip:

```shell
pip install aio
```

## Quickstart

### Buffered stream:

```python
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

```

### Echo TCP server:

```python
import asyncio
import logging
import socket as sc

from aio import net


async def echo(socket: sc.socket) -> None:
    loop = asyncio.get_running_loop()

    peer_address, peer_port = net.get_socket_peer_address(socket)
    logging.info("client %s:%d connected", peer_address, peer_port)

    data = await loop.sock_recv(socket, 1024)
    await loop.sock_sendall(socket, data)


async def main() -> None:
    logging.basicConfig(level=logging.INFO)

    await net.start_tcp_server("127.0.0.1", 8080, echo, graceful_shutdown=10.0)


asyncio.run(main())

```

### TCP client stream:

```python
import asyncio

from aio import net


async def main() -> None:
    async with await net.open_tcp_stream("127.0.0.1", 8080) as tcp_stream:
        await tcp_stream.write(b"Hello World!!!")

        while data := await tcp_stream.read(1024):
            print(data)


asyncio.run(main())

```
