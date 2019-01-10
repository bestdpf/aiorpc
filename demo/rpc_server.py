from aiorpc import register, serve

import asyncio


async def echo(msg):
    return msg


async def echo_stream(msg):
    for i in range(10):
        await asyncio.sleep(0)
        yield f'{msg}-{i}'


def run_server():
    try:
        import uvloop
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    except ModuleNotFoundError:
        pass
    # asyncio.set_event_loop(asyncio.ProactorEventLoop())
    register("echo", echo)
    register('echo_stream', echo_stream)
    coro = asyncio.start_server(serve, '127.0.0.1', 6000)
    server = asyncio.get_event_loop().run_until_complete(coro)

    try:
        asyncio.get_event_loop().run_forever()
    except KeyboardInterrupt:
        server.close()
        asyncio.get_event_loop().run_until_complete(server.wait_closed())


if __name__ == '__main__':
    run_server()
