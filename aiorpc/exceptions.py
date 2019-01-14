# -*- coding: utf-8 -*-


class RPCError(Exception):
    pass


class RPCIOError(RPCError):
    pass


class RPCProtocolError(RPCError):
    pass


class MethodNotFoundError(RPCError):
    pass


class EnhancedRPCError(RPCError):
    def __init__(self, parent, message):
        self.parent = parent
        self.message = message

        Exception.__init__(self, "{0}: {1}".format(parent, message))


class CtrlRPCError(RPCError):
    def __init__(self, parent, message):
        self.parent = parent
        self.message = message

        Exception.__init__(self, "{0}: {1}".format(parent, message))


class MethodRegisteredError(RPCError):
    pass
