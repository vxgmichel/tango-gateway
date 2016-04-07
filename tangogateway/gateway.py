"""Provide a Tango gateway server."""

# Imports
import socket
import asyncio
import argparse
from enum import Enum
from functools import partial
from contextlib import closing

# Local imports
from . import giop
from . import zmqforward

# Optional imports
try:
    import PyTango
except ImportError:
    PyTango = None


# Tokens

IMPORT_DEVICE = b'DbImportDevice'
GET_CSDB_SERVER = b'DbGetCSDbServerList'
ZMQ_SUBSCRIPTION_CHANGE = b'ZmqEventSubscriptionChange'


# Enumerations

class Patch(Enum):
    NONE = 0
    IOR = 1
    CSD = 2
    ZMQ = 3
    SUB = 4


class HandlerType(Enum):
    DB = 1
    DS = 2
    ZMQ = 3


# Function helpers

def find_all(string, sub):
    start = 0
    while True:
        start = string.find(sub, start)
        if start == -1:
            return
        yield start
        start += len(sub)


def make_translater(sub, pub):
    sub = ':'.join(map(str, sub)).encode()
    pub = ':'.join(map(str, pub)).encode()
    args = (sub, pub), (pub, sub)
    return lambda value, reverse=False: value.replace(*args[reverse])


# Coroutine helpers

@asyncio.coroutine
def get_connection(key, loop, only_check=False):
    host, port, _ = key
    # Try to connect
    try:
        reader, writer = yield from asyncio.open_connection(
            host, port, loop=loop)
    # Connection broken
    except ConnectionRefusedError:
        print("Could not connect to {} port {}".format(host, port))
        yield from stop_forwarding(key, loop)
        return False
    # Connection OK
    if not only_check:
        return reader, writer
    writer.close()
    return True


@asyncio.coroutine
def get_host_name(stream, resolve=True):
    loop = stream._loop
    sock = stream._transport._sock
    if not resolve:
        return sock.getsockname()[0]
    name_info = yield from loop.getnameinfo(sock.getsockname())
    return name_info[0]


@asyncio.coroutine
def check_servers(loop, period=10.):
    while True:
        yield from asyncio.sleep(period)
        for key in list(loop.forward_dict):
            yield from get_connection(key, loop, only_check=True)


# Forwarding helpers

@asyncio.coroutine
def get_forwarding(host, port, handler_type,
                   bind_address='0.0.0.0', server_port=0, loop=None):
    if loop is None:
        loop = asyncio.get_event_loop()
    # Check connection
    key = host, port, bind_address
    if not (yield from get_connection(key, loop, only_check=False)):
        return None, bind_address, loop.bound_port
    # Check cache
    if key in loop.forward_dict:
        return (yield from loop.forward_dict[key])
    loop.forward_dict[key] = asyncio.Future(loop=loop)
    # Start forwarding
    value = yield from start_forwarding(
        host, port, handler_type, bind_address, server_port, loop)
    # Set cache
    loop.forward_dict[key].set_result(value)
    return value


@asyncio.coroutine
def start_forwarding(host, port, handler_type,
                     bind_address='0.0.0.0', server_port=0, loop=None):
    if loop is None:
        loop = asyncio.get_event_loop()
    # GIOP handler
    if handler_type != HandlerType.ZMQ:
        # Make handler
        key = host, port, bind_address
        handler_dict = {
            HandlerType.DB: handle_db_client,
            HandlerType.DS: handle_ds_client}
        handler = partial(handler_dict[handler_type], key=key)
        # Start server
        server = yield from asyncio.start_server(
            handler, bind_address, server_port, loop=loop)
        bind_address, server_port = server.sockets[0].getsockname()
    # ZMQ handler
    else:
        # Make translater
        address = bind_address, loop.server_port
        translater = make_translater(address, loop.tango_host)
        # Start server
        coro = zmqforward.pubsub_forwarding(
            host, port, translater, bind_address, server_port, loop=loop)
        server, bind_address, server_port = yield from coro
    # Print and return
    msg = "Forwarding {} traffic on {} port {} to {} port {}"
    print(msg.format(handler_type.name, bind_address, server_port, host, port))
    return server, bind_address, server_port


@asyncio.coroutine
def stop_forwarding(key, loop):
    # Get server
    if key not in loop.forward_dict or \
       not loop.forward_dict[key].done() or \
       loop.forward_dict[key].exception():
        return
    server, bind_address, server_port = loop.forward_dict.pop(key).result()
    # Close server
    server.close()
    yield from server.wait_closed()
    # Print
    host, port, _ = key
    msg = "Stopped forwarding traffic on {} port {} to {} port {}"
    print(msg.format(bind_address, server_port, host, port))


# Frame helper

@asyncio.coroutine
def forward_giop_frame(reader, writer, bind_address, patch=Patch.NONE):
    last = False
    while not last:
        last, fragment = yield from read_giop_fragment(
            reader, bind_address, patch)
        if fragment:
            writer.write(fragment)
    return fragment


@asyncio.coroutine
def read_giop_fragment(reader, bind_address, patch=Patch.NONE):
    # Read header
    loop = reader._loop
    try:
        raw_header = yield from reader.readexactly(12)
    except asyncio.IncompleteReadError:
        return True, b''
    header = giop.unpack_giop_header(raw_header)
    last = giop.is_last_fragment(header)
    # Read data
    raw_data = yield from reader.readexactly(header.size)
    raw_frame = raw_header + raw_data
    if header.message_type != giop.MessageType.Reply or patch == Patch.NONE:
        return last, raw_frame
    # Unpack reply
    raw_reply_header, raw_body = raw_data[:12], raw_data[12:]
    reply_header = giop.unpack_reply_header(raw_reply_header)
    if reply_header.reply_status != giop.ReplyStatus.NoException:
        return last, raw_frame
    assert giop.is_little_endian(header)
    # Patch body
    if patch == Patch.IOR:
        new_body = yield from check_ior(raw_body, bind_address, loop)
    elif patch == Patch.ZMQ:
        new_body = yield from check_zmq(raw_body, bind_address, loop)
    elif patch == Patch.CSD:
        new_body = yield from check_csd(raw_body, bind_address, loop)
    # Ignore
    if not new_body:
        return last, raw_frame
    # Repack frame
    raw_data = raw_reply_header + new_body
    return last, giop.pack_giop(header, raw_data)


# Inspect DB traffic

@asyncio.coroutine
def handle_db_client(reader, writer, key):
    with closing(writer):
        loop = reader._loop
        bind_address = yield from get_host_name(writer)
        # Connect to client
        connection = yield from get_connection(key, loop)
        if not connection:
            return
        db_reader, db_writer = connection
        # Loop over reply/requests
        with closing(db_writer):
            while not reader.at_eof() and not db_reader.at_eof():
                # Read request
                request = yield from forward_giop_frame(
                    reader, db_writer, bind_address)
                if not request:
                    break
                # Choose patch
                if IMPORT_DEVICE in request:
                    patch = Patch.IOR
                elif GET_CSDB_SERVER in request:
                    patch = Patch.CSD
                else:
                    patch = Patch.NONE
                # Read reply_header
                reply = yield from forward_giop_frame(
                    db_reader, writer, bind_address, patch=patch)


@asyncio.coroutine
def check_ior(raw_body, bind_address, loop):
    # Find IOR, host and port
    ior = giop.find_ior(raw_body)
    if not ior:
        return False
    ior, start, stop = ior
    host = giop.from_byte_string(ior.host)
    key = host, ior.port, bind_address
    # Start port forwarding
    server, _, server_port = yield from get_forwarding(
        host, ior.port, HandlerType.DS, bind_address, loop=loop)
    # Patch IOR
    ior = ior._replace(host=giop.to_byte_string(bind_address),
                       port=server_port)
    # Repack body
    return giop.repack_ior(raw_body, ior, start, stop)


@asyncio.coroutine
def check_csd(raw_body, bind_address, loop):
    csd = giop.find_csd(raw_body)
    if not csd:
        return False
    csd, start = csd
    new_csd = ':'.join((bind_address, str(loop.server_port)))
    new_csd = giop.to_byte_string(new_csd)
    return giop.repack_csd(raw_body, new_csd, start)


# Inspect DS traffic

@asyncio.coroutine
def handle_ds_client(reader, writer, key):
    with closing(writer):
        loop = reader._loop
        bind_address = yield from get_host_name(writer)
        # Connect to client
        connection = yield from get_connection(key, loop)
        if not connection:
            return
        ds_reader, ds_writer = connection
        # Loop over reply/requests
        with closing(ds_writer):
            while not reader.at_eof() and not ds_reader.at_eof():
                # Read request
                request = yield from forward_giop_frame(
                    reader, ds_writer, bind_address)
                if not request:
                    break
                # Choose patch
                if ZMQ_SUBSCRIPTION_CHANGE in request:
                    patch = Patch.ZMQ
                else:
                    patch = Patch.NONE
                # Read reply_header
                reply = yield from forward_giop_frame(
                    ds_reader, writer, bind_address, patch=patch)


@asyncio.coroutine
def check_zmq(raw_body, bind_address, loop):
    # Find zmq token
    zmq = giop.find_zmq_endpoints(raw_body)
    if not zmq:
        return False
    # Exctract endpoints
    new_endpoints = []
    zmq1, zmq2, start = zmq
    for zmq in (zmq1, zmq2):
        host, port = giop.decode_zmq_endpoint(zmq)
        key = host, port, bind_address
        # Start port forwarding
        server, _, server_port = yield from get_forwarding(
            host, port, HandlerType.ZMQ, bind_address, loop=loop)
        # Make new endpoints
        endpoint = giop.encode_zmq_endpoint(bind_address, server_port)
        new_endpoints.append(endpoint)
    # Repack body
    zmq1, zmq2 = new_endpoints
    return giop.repack_zmq_endpoints(raw_body, zmq1, zmq2, start)


# Run server

def run_gateway_server(bind_address, server_port, tango_host):
    """Run a Tango gateway server."""
    # Initialize loop
    loop = asyncio.get_event_loop()
    loop.bind_address = bind_address
    loop.server_port = server_port
    loop.tango_host = tango_host
    loop.forward_dict = {}
    # Create locked
    loop.bound_socket = socket.socket()
    loop.bound_socket.bind((bind_address, 0))
    loop.bound_port = loop.bound_socket.getsockname()[1]
    # Create server
    host, port = tango_host
    coro = get_forwarding(
        host, port, HandlerType.DB, bind_address, server_port, loop=loop)
    server, _, _ = loop.run_until_complete(coro)
    # Serve requests until Ctrl+C is pressed
    try:
        check_task = loop.create_task(check_servers(loop))
        loop.run_forever()
    except KeyboardInterrupt:
        check_task.cancel()
    # Close all the servers
    servers = [fut.result()[0]
               for fut in loop.forward_dict.values()
               if fut.done() and not fut.exception()]
    servers.append(server)
    for server in servers:
        server.close()
    # Wait for the servers to close
    wait_servers = asyncio.wait([server.wait_closed() for server in servers])
    loop.run_until_complete(wait_servers)
    # Cancel all the tasks
    tasks = asyncio.Task.all_tasks()
    for task in tasks:
        task.cancel()
    # Wait for all the tasks to finish
    if tasks:
        loop.run_until_complete(asyncio.wait(tasks))
    loop.close()


def main(*args):
    """Run a Tango gateway server from CLI arguments."""
    # Create parser
    parser = argparse.ArgumentParser(
        prog='tangogateway',
        description='Run a Tango gateway server')
    parser.add_argument(
        '--bind', '-b', metavar='ADDRESS', default='0.0.0.0',
        help='Specify the bind address (default is all interfaces)')
    parser.add_argument(
        '--port', '-p', metavar='PORT', default=8000, type=int,
        help='Port for the server (default is 8000)')
    parser.add_argument(
        '--tango', '-t', metavar='HOST',
        help='Tango host (default is given by PyTango)')
    # Parse arguments
    namespace = parser.parse_args(*args)
    # Check Tango database
    if PyTango is None:
        if namespace.tango:
            print("Warning: PyTango not available, cannot check database")
            namespace.tango = namespace.tango.split(":")
        else:
            parser.error("PyTango not available, "
                         "the tango host has to be defined explicitely")
    else:
        if namespace.tango:
            db = PyTango.Database(namespace.tango)
        else:
            db = PyTango.Database()
        namespace.tango = db.get_db_host(), int(db.get_db_port())
    # Run the server
    return run_gateway_server(namespace.bind, namespace.port, namespace.tango)


if __name__ == '__main__':
    main()
