from aiorpc import RPCClient

import asyncio
import time


async def do(cli):
    start_time = time.time()
    cnt = 100000
    for i in range(cnt):
        ret = await cli.call_once('echo', 'messagexxx')
        # print(f'xxxxx {ret}')
    end_time = time.time()
    time_diff = end_time - start_time
    print(f'speed is {time_diff/cnt}, total cost time is {time_diff}')


def run_client():
    loop = asyncio.get_event_loop()
    client = RPCClient('127.0.0.1', 6000)
    loop.run_until_complete(do(client))
    client.close()


if __name__ == '__main__':
    run_client()

