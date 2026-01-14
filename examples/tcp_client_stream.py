import asyncio

from aio import net


async def main() -> None:
    async with await net.open_tcp_stream("127.0.0.1", 8080) as tcp_stream:
        await tcp_stream.write(b"Hello World!!!")

        while data := await tcp_stream.read(1024):
            print(data)


asyncio.run(main())
