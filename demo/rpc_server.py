from aiorpc import register, serve

import asyncio
import sys


async def echo(msg):
    return msg


async def echo_stream(msg):
    for i in range(10):
        await asyncio.sleep(0)
        yield f'{msg}-{i}'


def run_server():
    # asyncio.set_event_loop(asyncio.ProactorEventLoop())
    register("echo", echo)
    register('echo_stream', echo_stream)
    coro = asyncio.start_server(serve, '127.0.0.1', 6000)
    server = asyncio.get_event_loop().run_until_complete(coro)

    old_close = server.close

    def new_close():
        import traceback
        traceback.print_stack()
        old_close()
    server.close = new_close
    old_wait_closed = server.wait_closed

    async def new_wait_closed():
        import traceback
        traceback.print_stack()
        await old_wait_closed()

    server.wait_closed = new_wait_closed

    try:
        while True:
            asyncio.get_event_loop().run_until_complete(asyncio.sleep(10))
            print(server)
        # asyncio.get_event_loop().run_forever()
    except KeyboardInterrupt:
        import objgraph
        objgraph.show_most_common_types(limit=50)
        import gc
        gc.set_debug(gc.DEBUG_LEAK)
        print('='*10)
        print(gc.garbage)
        server.close()
        asyncio.get_event_loop().run_until_complete(server.wait_closed())


if __name__ == '__main__':
    try:
        pass
        import uvloop
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    except ModuleNotFoundError:
        pass
    if sys.platform == 'win32':
        asyncio.set_event_loop(asyncio.ProactorEventLoop())
    run_server()
