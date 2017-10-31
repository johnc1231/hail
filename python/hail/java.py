import SocketServer
import socket
import sys
from threading import Thread

import py4j
from decorator import decorator


class FatalError(Exception):
    """:class:`.FatalError` is an error thrown by Hail method failures"""


class Env:
    _jvm = None
    _gateway = None
    _hail_package = None
    _jutils = None
    _hc = None

    @staticmethod
    def jvm():
        if not Env._jvm:
            raise EnvironmentError('no Hail context initialized, create one first')
        return Env._jvm

    @staticmethod
    def hail():
        if not Env._hail_package:
            Env._hail_package = getattr(Env.jvm(), 'is').hail
        return Env._hail_package

    @staticmethod
    def gateway():
        if not Env._gateway:
            raise EnvironmentError('no Hail context initialized, create one first')
        return Env._gateway

    @staticmethod
    def jutils():
        if not Env._jutils:
            Env._jutils = scala_package_object(Env.hail().utils)
        return Env._jutils

    @staticmethod
    def hc():
        if not Env._hc:
            raise EnvironmentError('no Hail context initialized, create one first')
        return Env._hc


def jarray(jtype, lst):
    jarr = Env.gateway().new_array(jtype, len(lst))
    for i, s in enumerate(lst):
        jarr[i] = s
    return jarr


def scala_object(jpackage, name):
    return getattr(getattr(jpackage, name + '$'), 'MODULE$')


def scala_package_object(jpackage):
    return scala_object(jpackage, 'package')


def jnone():
    return scala_object(Env.jvm().scala, 'None')


def jsome(x):
    return Env.jvm().scala.Some(x)


def joption(x):
    return jsome(x) if x else jnone()


def from_option(x):
    return x.get() if x.isDefined() else None


def jindexed_seq(x):
    return Env.jutils().arrayListToISeq(x)


def jset(x):
    return Env.jutils().arrayListToSet(x)


def jindexed_seq_args(x):
    args = [x] if isinstance(x, str) or isinstance(x, unicode) else x
    return jindexed_seq(args)


def jset_args(x):
    args = [x] if isinstance(x, str) or isinstance(x, unicode) else x
    return jset(args)


def jiterable_to_list(it):
    if it:
        return list(Env.jutils().iterableToArrayList(it))
    else:
        return None


def jarray_to_list(a):
    return list(a) if a else None


@decorator
def handle_py4j(func, *args, **kwargs):
    try:
        r = func(*args, **kwargs)
    except py4j.protocol.Py4JJavaError as e:
        tpl = Env.jutils().handleForPython(e.java_exception)
        deepest, full = tpl._1(), tpl._2()
        raise FatalError('%s\n\nJava stack trace:\n%s\n'
                         'Hail version: %s\n'
                         'Error summary: %s' % (deepest, full, Env.hc().version, deepest))
    except py4j.protocol.Py4JError as e:
        if e.args[0].startswith('An error occurred while calling'):
            msg = 'An error occurred while calling into JVM, probably due to invalid parameter types.'
            raise FatalError('%s\n\nJava stack trace:\n%s\n'
                             'Hail version: %s\n'
                             'Error summary: %s' % (msg, e.message, Env.hc().version, msg))
        else:
            raise e
    return r


class LoggingTCPHandler(SocketServer.StreamRequestHandler):
    def handle(self):
        for line in self.rfile:
            sys.stderr.write(line)


class SimpleServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, server_address, handler_class):
        SocketServer.TCPServer.__init__(self, server_address, handler_class)


def connect_logger(host, port):
    """
    This method starts a simple server which listens on a port for a
    client to connect and start writing messages. Whenever a message
    is received, it is written to sys.stderr. The server is run in
    a daemon thread from the caller, which is killed when the caller
    thread dies.

    If the socket is in use, then the server tries to listen on the
    next port (port + 1). After 25 tries, it gives up.

    :param str host: Hostname for server.
    :param int port: Port to listen on.
    """
    server = None
    tries = 0
    max_tries = 25
    while not server:
        try:
            server = SimpleServer((host, port), LoggingTCPHandler)
        except socket.error:
            port += 1
            tries += 1

            if tries >= max_tries:
                sys.stderr.write(
                    'WARNING: Could not find a free port for logger, maximum retries {} exceeded.'.format(max_tries))
                return

    t = Thread(target=server.serve_forever, args=())

    # The thread should be a daemon so that it shuts down when the parent thread is killed
    t.daemon = True

    t.start()
    Env.jutils().addSocketAppender(host, port)
