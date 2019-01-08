# -*- coding: utf-8 -*-

import asyncio
from aiorpc.log import rootLogger
from aiorpc.constants import SOCKET_RECV_SIZE

__all__ = ['Connection']
_logger = rootLogger.getChild(__name__)


class Connection:
    def __init__(self, reader, writer, unpacker):
        self.reader = reader
        self.writer = writer
        self.unpacker = unpacker
        self._is_closed = False
        self.peer = self.writer.get_extra_info('peername')

    async def sendall(self, raw_req, timeout):
        _logger.debug('sending raw_req {} to {}'.format(
            str(raw_req), self.peer))

        self.writer.write(raw_req)
        await self.writer.drain()
        # await asyncio.wait_for(self.writer.drain(), timeout)
        _logger.debug('sending {} completed'.format(str(raw_req)))

    async def recvall(self):
        _logger.debug('entered recvall from {}'.format(self.peer))
        req = None
        while True:
            # 可能有老的数据没有get
            try:
                req = next(self.unpacker)
                return req
            except StopIteration:
                pass
            data = await self.reader.read(SOCKET_RECV_SIZE)
            _logger.debug('receiving data {} from {}'.format(data, self.peer))
            # print(f'recv data {data}')
            if not data:
                # print('error')
                raise IOError('Connection to {} closed'.format(self.peer))
            self.unpacker.feed(data)
            try:
                req = next(self.unpacker)
                break
            except StopIteration:
                continue
        _logger.debug('received req from {} : {}'.format(self.peer, req))
        _logger.debug('exiting recvall from {}'.format(self.peer))
        # print(f'get req {req}')
        return req

    def close(self):
        self.reader.feed_eof()
        self.writer.close()
        self._is_closed = True

    def is_closed(self):
        return self._is_closed
