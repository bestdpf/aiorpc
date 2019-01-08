from aiorpc import RPCClient

import asyncio
import time


async def do(cli, cnt=1000):
    start_time = time.time()
    for i in range(cnt):
        ret = await cli.call_once('echo', 'messagexxx', timeout=10)
        # print(f'xxxxx {ret}')
    end_time = time.time()
    time_diff = end_time - start_time
    print(f'speed is {time_diff/cnt}, total cost time is {time_diff}')


async def mulit_do(cli, num=10, cnt=1000):
    jobs = []
    for i in range(num):
        jobs.append(do(cli, cnt))
    await asyncio.gather(*jobs, return_exceptions=True)


def run_client():
    try:
        import uvloop
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    except ModuleNotFoundError:
        pass
    loop = asyncio.get_event_loop()
    client = RPCClient('127.0.0.1', 6000)
    loop.run_until_complete(client._open_connection())
    loop.run_until_complete(mulit_do(client, num=5, cnt=1))
    client.close()


if __name__ == '__main__':
    run_client()

