# -*- coding: utf-8 -*-
import asyncio
import msgpack
import datetime
import inspect

from aiorpc.constants import MSGPACKRPC_REQUEST, MSGPACKRPC_RESPONSE
from aiorpc.exceptions import MethodNotFoundError, RPCProtocolError, MethodRegisteredError
from aiorpc.connection import Connection
from aiorpc.log import rootLogger

__all__ = ['register', 'msgpack_init', 'set_timeout', 'serve', 'register_class']

_logger = rootLogger.getChild(__name__)
_methods = dict()
_class_methods = dict()
_pack_encoding = 'utf-8'
_pack_params = dict()
_unpack_encoding = 'utf-8'
_unpack_params = dict(use_list=False)
_timeout = 3


def register(name, f):
    """Register a function on the RPC server.
    Usage:
        >>> def sum(x, y):
        >>>     return x + y
        >>> register('sum', sum)

    :param name: The remote name of the function, can be different with the f.__name__.
    :param f: Function object. Must be a callable object or a coroutine object.
    :return: None
    """
    global _methods
    if not hasattr(f, "__call__"):
        raise MethodRegisteredError("{} is not a callable object".format(f.__name__))
    if name in _methods:
        raise MethodRegisteredError("Name {} has already been used".format(name))
    _methods[name] = f


def register_class(cls):
    """
    Registers a class on the RPC server. Methods can be accessed by ClassName.Method
    :param cls: class to load
    :return:
    """
    name = cls.__name__
    _logger.info("Loaded class `{0}`".format(name))
    if name in _class_methods:
        raise MethodRegisteredError("Class {} has already been loaded".format(name))
    _class_methods[name] = cls()


def msgpack_init(**kwargs):
    """Init parameters of msgpack packer and unpacker.
    Usage:
        >>> msgpack_init(pack_encoding='utf-8')
    :param kwargs: See http://pythonhosted.org/msgpack-python/api.html
            default:
            pack_encoding='utf-8'
            pack_params=dict()
            unpack_encoding='utf-8'
            unpack_params=dict(use_list=False)
    :return: None
    """
    global _pack_encoding, _pack_params, _unpack_encoding, _unpack_params
    _pack_encoding = kwargs.pop('pack_encoding', 'utf-8')
    _pack_params = kwargs.pop('pack_params', dict())

    _unpack_encoding = kwargs.pop('unpack_encoding', 'utf-8')
    _unpack_params = kwargs.pop('unpack_params', dict(use_list=False))


def set_timeout(timeout):
    """Set the IO timeout
    Usage:
        >>> set_timeout(1)

    :param timeout: Timeout. Seconds.
    :return: None
    """
    global _timeout
    _timeout = timeout
    
    
async def _send_error(conn, exception, error, msg_id):
    response = (MSGPACKRPC_RESPONSE, msg_id, (exception, error), None, None)
    try:
        await conn.sendall(msgpack.packb(response, encoding=_pack_encoding, **_pack_params),
                           _timeout)
    except asyncio.TimeoutError as te:
        _logger.error("Timeout when _send_error {} to {}".format(
            error, conn.writer.get_extra_info('peername')))
    except Exception as e:
        _logger.error("Exception {} raised when _send_error {} to {}".format(
            str(e), error, conn.writer.get_extra_info("peername")
        ))


async def _send_ctrl(conn, ctrl, ctrl_detail, msg_id):
    response = (MSGPACKRPC_RESPONSE, msg_id, None, None, (ctrl, ctrl_detail))
    try:
        await conn.sendall(msgpack.packb(response, encoding=_pack_encoding, **_pack_params),
                           _timeout)
    except asyncio.TimeoutError as te:
        _logger.error("Timeout when _send_ctrl {} to {}".format(
            ctrl, conn.writer.get_extra_info('peername')))
    except Exception as e:
        _logger.error("Exception {} raised when _send_error {} to {}".format(
            str(e), ctrl, conn.writer.get_extra_info("peername")
        ))


async def _send_result(conn, result, msg_id):
    _logger.debug('entering _send_result')
    response = (MSGPACKRPC_RESPONSE, msg_id, None, result, None)
    try:
        _logger.debug('begin to sendall')
        ret = msgpack.packb(response, encoding=_pack_encoding, **_pack_params)
        await conn.sendall(ret, _timeout)
        _logger.debug('sendall completed')
    except asyncio.TimeoutError as te:
        _logger.error("Timeout when _send_result {} to {}".format(
            str(result), conn.writer.get_extra_info('peername')))
    except Exception as e:
        import traceback
        traceback.print_exc()
        # here is dangerous, as upper application transaction may rely on the result.
        # This should never happen if we want something like distributed transaction!!!!
        _logger.error("Exception {} raised when _send_result {} to {}".format(
            str(e), str(result), conn.writer.get_extra_info("peername")
        ))


def _parse_request(req):
    if len(req) != 6 or req[0] != MSGPACKRPC_REQUEST:
        raise RPCProtocolError('Invalid protocol')

    _, msg_id, method_name, args, timeout, streamed = req
    
    _method_soup = method_name.split('.')
    if len(_method_soup) == 1:
        method = _methods.get(method_name)
    else:
        method = getattr(_class_methods.get(_method_soup[0]), _method_soup[1])

    if not method:
        raise MethodNotFoundError("No such method {}".format(method_name))

    return msg_id, method, args, method_name, timeout, streamed


async def handle_request(conn, req):
    method = None
    msg_id = None
    args = None
    timeout = 0
    try:
        _logger.debug('parsing req: {}'.format(str(req)))
        msg_id, method, args, method_name, timeout, streamed = _parse_request(req)
        _logger.debug('parsing completed: {0} {1}'.format(str(req), msg_id))
    except Exception as e:
        _logger.error("Exception {} raised when _parse_request {}".format(str(e), req))
        return
    else:
        if streamed:
            # stream指令暂时不用timeout
            return await handle_stream_request(conn, msg_id, method, args, method_name)
        else:
            return await handle_once_request(conn, msg_id, method, args, method_name, timeout)


async def handle_once_request(conn, msg_id, method, args, method_name, timeout):
    req_start = datetime.datetime.now()
    # Execute the parsed request
    try:
        _logger.debug('calling method: {}'.format(str(method)))
        ret = method.__call__(*args)
        if asyncio.iscoroutine(ret):
            _logger.debug("start to wait_for")
            if timeout > 0:
                ret = await asyncio.wait_for(ret, timeout)
            else:
                ret = await ret
        _logger.debug('calling {} completed. result: {}'.format(str(method), str(ret)))
    except Exception as e:
        import traceback
        traceback.print_exc()
        _logger.error("Caught Exception in `{0}`. {1}: {2}".format(method_name, type(e).__name__, str(e)))
        await _send_error(conn, type(e).__name__, str(e), msg_id)
        _logger.debug('sending exception {} completed'.format(str(e)))
    else:
        _logger.debug('sending result: {}'.format(str(ret)))
        await _send_result(conn, ret, msg_id)
        _logger.debug('sending result {} completed'.format(str(ret)))

    req_end = datetime.datetime.now()
    _logger.info("Method `{0}` took {1}ms".format(method_name, (req_end - req_start).microseconds / 1000))


async def handle_stream_request(conn, msg_id, method, args, method_name):
    req_start = datetime.datetime.now()

    # Execute the parsed request
    try:
        _logger.debug('calling method: {}'.format(str(method)))
        if not inspect.isasyncgenfunction(method):
            await _send_error(conn, 'not stream coroutine', 'not stream coroutine', msg_id)
            _logger.error(f'the stream method {method} must be coroutine function!!!')
        else:
            async for ret in method.__call__(*args):
                await _send_result(conn, ret, msg_id)
            # 暂时可以用None result替代控制包
            await _send_result(conn, None, msg_id)
            # await _send_ctrl(conn, 'ctrl', 'stop_stream', msg_id)
        _logger.debug('calling stream {} completed'.format(str(method)))
    except Exception as e:
        import traceback
        traceback.print_exc()
        _logger.error("Caught Exception in `{0}`. {1}: {2}".format(method_name, type(e).__name__, str(e)))
        await _send_error(conn, type(e).__name__, str(e), msg_id)
        _logger.debug('sending exception {} completed'.format(str(e)))
    finally:
        pass

    req_end = datetime.datetime.now()
    _logger.info("Method `{0}` took {1}ms".format(method_name, (req_end - req_start).microseconds / 1000))


async def serve(reader, writer):
    """Serve function.
    Don't use this outside asyncio.start_server.
    """
    global _unpack_encoding, _unpack_params
    _logger.debug('enter serve: {}'.format(writer.get_extra_info('peername')))

    conn = Connection(reader, writer,
                      msgpack.Unpacker(encoding=_unpack_encoding, **_unpack_params))

    while not conn.is_closed():
        req = None
        try:
            req = await conn.recvall()
        except IOError as ie:
            break
        except Exception as e:
            conn.reader.set_exception(e)
            raise e

        if not isinstance(req, tuple):
            try:
                await _send_error(conn, "Invalid protocol", -1)
                # skip the rest of iteration code after sending error
                continue

            except Exception as e:
                _logger.error("Error when receiving req: {}".format(str(e)))
        asyncio.create_task(handle_request(conn, req))



