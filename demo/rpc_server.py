from aiorpc import register, serve

import asyncio


def echo(msg):
    return msg


def run_server():

    register("echo", echo)
    coro = asyncio.start_server(serve, '127.0.0.1', 6000)
    server = asyncio.get_event_loop().run_until_complete(coro)

    try:
        asyncio.get_event_loop().run_forever()
    except KeyboardInterrupt:
        server.close()
        asyncio.get_event_loop().run_until_complete(server.wait_closed())


if __name__ == '__main__':
    run_server()
