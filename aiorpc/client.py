# -*- coding: utf-8 -*-
import asyncio
import msgpack
import logging

from aiorpc.connection import Connection
from aiorpc.log import rootLogger
from aiorpc.constants import MSGPACKRPC_RESPONSE, MSGPACKRPC_REQUEST, BACKGROUND_RECV_INTERVAL, TRY_CONNECT_TIME
from aiorpc.exceptions import RPCProtocolError, RPCError, EnhancedRPCError, CtrlRPCError, RPCIOError
from concurrent.futures._base import CancelledError

__all__ = ['RPCClient']

_logger = rootLogger.getChild(__name__)


class RPCClient:
    """RPC client.

    Usage:
        >>> from aiorpc.client import RPCClient
        >>> client = RPCClient('127.0.0.1', 6000)
        >>> import asyncio
        >>> loop = asyncio.get_event_loop()
        >>> loop.run_until_complete(client.call('sum', 1, 2))

    :param str host: Hostname.
    :param int port: Port number.
    :param int timeout: (optional) Socket timeout.
    :param str pack_encoding: (optional) Character encoding used to pack data
        using Messagepack.
    :param str unpack_encoding: (optional) Character encoding used to unpack
        data using Messagepack.
    :param dict pack_params: (optional) Parameters to pass to Messagepack Packer
    :param dict unpack_params: (optional) Parameters to pass to Messagepack
        Unpacker.
    """

    def __init__(self, host, port, *, comm_timeout=3, loop=None,
                 pack_encoding='utf-8', unpack_encoding='utf-8',
                 pack_params=None, unpack_params=None):
        self._host = host
        self._port = port
        # channel的timeout，而非call的
        self._timeout = comm_timeout
        self._background_recv_task = None

        self._loop = loop
        self._conn = None
        self._msg_id = 0
        self._pack_encoding = pack_encoding
        self._pack_params = pack_params or dict()
        self._unpack_encoding = unpack_encoding
        self._unpack_params = unpack_params or dict(use_list=False)
        # msg id到fut的映射
        self._id2fut = {}

    def getpeername(self):
        """Return the address of the remote endpoint."""
        return self._host, self._port

    def close(self):
        try:
            if self._conn:
                self._conn.close()
            if self._background_recv_task:
                self._background_recv_task.cancel()
                self._background_recv_task = None
            for _, fut in self._id2fut.items():
                if not fut.done():
                    fut.set_result(None)
            self._id2fut.clear()
            # print('close connection')
        except AttributeError:
            import traceback
            traceback.print_exc()
            pass

    async def _open_connection(self):
        _logger.debug("connect to {}:{}...".format(self._host, self._port))
        try_cnt = 0
        while try_cnt < TRY_CONNECT_TIME:
            try:
                try_cnt += 1
                reader, writer = await asyncio.open_connection(self._host, self._port, loop=self._loop)
                break
            except ConnectionError as e:
                print(f'connection error when open_connection try cnt is {try_cnt}')
                if try_cnt >= TRY_CONNECT_TIME:
                    raise e

        # 这里做的检查才是有效的，不然await之后会有问题
        if self._conn and not self._conn.is_closed():
            return

        self._conn = Connection(reader, writer,
                                msgpack.Unpacker(encoding=self._unpack_encoding, **self._unpack_params))
        # 加入background recv任务
        if self._background_recv_task is None:
            self._background_recv_task = asyncio.create_task(self._recv_on_background())
        # print(f'reopen connection ...')
        _logger.debug("Connection to {}:{} established".format(self._host, self._port))

    async def call_once(self, method, *args, timeout=3):
        """Calls a RPC method.

        :param str method: Method name.
        :param args: Method arguments.
        :param timeout: the call's max return time
        """

        _logger.debug('creating request')
        req, msg_id = self._create_request(method, args, timeout, False)

        if self._conn is None or self._conn.is_closed():
            await self._open_connection()

        try:
            _logger.debug('Sending req: {0} {1}'.format(req, msg_id))
            await self._conn.sendall(req, self._timeout)
            _logger.debug('Sending complete')
        except asyncio.TimeoutError as te:
            _logger.error("Write request to {}:{} timeout".format(self._host, self._port))
            raise te
        except Exception as e:
            raise e
        try:
            response_fut = asyncio.Future()
            # TODO 可能有点泄漏？
            self._id2fut.update({msg_id: response_fut})
            if timeout:
                response = await asyncio.wait_for(response_fut, timeout=timeout)
            else:
                response = await response_fut
        except asyncio.TimeoutError:
            _logger.error('rpc {0} {1} timeout'.format(method, args))
            return None
        finally:
            self._id2fut.pop(msg_id, None)

        return response

    async def call_stream(self, method, *args):
        """Calls a RPC method.

        :param str method: Method name.
        :param args: Method arguments.
        :param timeout: the call's max return time
        """

        _logger.debug('creating request')
        req, msg_id = self._create_request(method, args, 0, True)

        if self._conn is None or self._conn.is_closed():
            await self._open_connection()

        try:
            _logger.debug('Sending req: {}'.format(req))
            await self._conn.sendall(req, self._timeout)
            _logger.debug('Sending complete')
        except asyncio.TimeoutError as te:
            _logger.error("Write request to {}:{} timeout".format(self._host, self._port))
            raise te
        except Exception as e:
            raise e

        try:
            while True:
                response_fut = asyncio.Future()
                self._id2fut.update({msg_id: response_fut})
                response = await response_fut
                # 对于流指令来讲，None就是结束
                if response is None:
                    return
                yield response
        finally:
            self._id2fut.pop(msg_id, None)

    def _create_request(self, method, args, timeout=0, streamed=False):
        self._msg_id += 1

        req = (MSGPACKRPC_REQUEST, self._msg_id, method, args, timeout, streamed)

        return msgpack.packb(req, encoding=self._pack_encoding, **self._pack_params), self._msg_id

    def _parse_response(self, response):
        if (len(response) != 5 or response[0] != MSGPACKRPC_RESPONSE):
            raise RPCProtocolError('Invalid protocol')

        (_, msg_id, error, result, ctrl) = response

        if ctrl and len(ctrl) == 2:
            # 控制包，暂时只有流的停止
            # raise CtrlRPCError(*ctrl)
            return result, msg_id, ctrl

        if error and len(error) == 2:
            raise EnhancedRPCError(*error)
        elif error:
            raise RPCError(error)

        return result, msg_id, ctrl

    async def _process_one_request(self):
        while True:
            try:
                _logger.debug('receiving result from server')
                # TODO 只是为了服务器不卡死吧
                await asyncio.sleep(BACKGROUND_RECV_INTERVAL)
                if not self._conn or self._conn.is_closed():
                    return
                response = await self._conn.recvall()
                _logger.debug('receiving result completed')
            # TODO 这里需要改一下，范围太广
            except Exception as e:
                self._conn.reader.set_exception(e)
                import traceback
                # traceback.print_exc()
                raise e

            if response is None:
                raise RPCIOError("Connection closed")

            if type(response) != tuple:
                logging.debug('Protocol error, received unexpected data: {}'.format(response))
                raise RPCProtocolError('Invalid protocol')

            response, msg_id, ctrl = self._parse_response(response)
            # print(f'get resp {response} {msg_id} {self._id2fut}')
            if msg_id in self._id2fut:
                self._id2fut[msg_id].set_result(response)
            else:
                logging.debug(f'Recv unknow msg {response} for {msg_id}')

    async def _recv_on_background(self):
        try:
            await asyncio.shield(self._process_one_request())
        except ConnectionError:
            print('connection error')
            pass
            self.close()
            # await self._open_connection()
        except CancelledError:
            print(f'process task is cancelled')
            self.close()
            pass
        except RPCIOError:
            print(f'connection lost')
            self.close()
            pass
        else:
            if self._conn and not self._conn.is_closed():
                self._background_recv_task = None
                self._background_recv_task = asyncio.create_task(self._recv_on_background())
            else:
                # self.close()
                print(f'stop recv')

    async def __aenter__(self):
        await self._open_connection()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._conn and not self._conn.is_closed():
            logging.debug('Closing connection from context manager')
            self.close()
