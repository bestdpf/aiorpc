from aiorpc import RPCClient

import asyncio
import time
import sys


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
    try:
        cnt = 0
        async for ret in cli.call_stream('echo_stream', 'stream message'):
            # print(f'stream ret is {ret}')
            cnt += 1
    except:
        import traceback
        traceback.print_exc()
    end_time = time.time()
    time_diff = end_time - start_time
    if cnt == 10:
        print(f'{cnt} speed is {time_diff/cnt}, total cost time is {time_diff}')
    else:
        print(f'error cnt is {cnt}')


async def multi_do(cli, num=10, cnt=1000):
    jobs = []
    for i in range(num):
        jobs.append(do(cli, cnt))
    await asyncio.gather(*jobs, return_exceptions=True)


async def multi_do_stream(cli, num=10):
    jobs = []
    for i in range(num):
        jobs.append(do_stream(cli))
    ret = await asyncio.gather(*jobs, return_exceptions=True)
    print(f'multi do stream ret is {ret}')
    

async def run_client():

    try:
        loop = asyncio.get_event_loop()
        client = RPCClient('127.0.0.1', 6000)
        # await client._open_connection()
        await multi_do_stream(client, 5)
        client.close()
    except:
        import traceback
        traceback.print_exc()


async def multi_run_client(num=100):
    loop = asyncio.get_event_loop()
    job = []
    for i in range(num):
        job.append(run_client())
    ret = await asyncio.gather(*job, return_exceptions=True)
    print(f'multi run client ret is {ret}')

if __name__ == '__main__':
    try:
        pass
        import uvloop
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    except ModuleNotFoundError:
        pass
    if sys.platform == 'win32':
        asyncio.set_event_loop(asyncio.ProactorEventLoop())
    asyncio.get_event_loop().run_until_complete(multi_run_client(500))

