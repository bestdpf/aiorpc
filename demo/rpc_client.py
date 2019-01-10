from aiorpc import RPCClient

import asyncio
import time


async def do(cli, cnt=1000):
    start_time = time.time()
    for i in range(cnt):
        ret = await cli.call_once('echo', 'messagexxx', timeout=3)
        # print(f'xxxxx {ret}')
    end_time = time.time()
    time_diff = end_time - start_time
    print(f'speed is {time_diff/cnt}, total cost time is {time_diff}')


async def do_stream(cli):
    start_time = time.time()
    cnt = 0
    async for ret in cli.call_stream('echo_stream', 'stream message'):
        # print(f'stream ret is {ret}')
        cnt += 1
    end_time = time.time()
    time_diff = end_time - start_time
    print(f'{cli._conn.writer._transport._extra} {cnt} speed is {time_diff/cnt}, total cost time is {time_diff}')


async def multi_do(cli, num=10, cnt=1000):
    jobs = []
    for i in range(num):
        jobs.append(do(cli, cnt))
    await asyncio.gather(*jobs, return_exceptions=True)


async def multi_do_stream(cli, num=10):
    jobs = []
    for i in range(num):
        jobs.append(do_stream(cli))
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
    loop.run_until_complete(multi_do_stream(client, 8000))
    client.close()


if __name__ == '__main__':
    run_client()

