from aiorpc import RPCClient

import asyncio


async def do(cli):
    ret = await cli.call_once('echo', 'messagexxx')
    print(f'xxxxx {ret}')


def run_client():
    loop = asyncio.get_event_loop()
    client = RPCClient('127.0.0.1', 6000)
    loop.run_until_complete(do(client))
    client.close()


if __name__ == '__main__':
    run_client()

