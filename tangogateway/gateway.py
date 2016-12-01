"""Provide a Tango gateway server."""

# Imports
import socket
import asyncio
from enum import Enum
from functools import partial
from contextlib import closing

# Local imports
from . import giop
from . import zmqforward

# Logging import
import logging
from logging import getLogger, Formatter, StreamHandler


# Create logger
logger = getLogger("Tango gateway")
# Create console handler
log_handler = StreamHandler()
# Create formater
log_format = Formatter('%(levelname)s - %(message)s')
log_handler.setFormatter(log_format)
logger.addHandler(log_handler)
logger.setLevel(logging.INFO)

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
    except (ConnectionRefusedError, OSError):
        logger.warn("Could not connect to {} port {}".format(host, port))
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
    if not (yield from get_connection(key, loop, only_check=True)):
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
            handler, bind_address, server_port,
            family=socket.AF_INET, loop=loop)
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
    msg = msg.format(handler_type.name, bind_address, server_port, host, port)
    logger.info(msg)
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
    logger.info(msg.format(bind_address, server_port, host, port))


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
    # Start port forwarding
    server, _, server_port = yield from get_forwarding(
        host, ior.port, HandlerType.DS, bind_address, loop=loop)
    # Patch IOR
    ior = ior._replace(host=giop.to_byte_string(bind_address),
                       port=server_port)
    # Log tango device name
    try:
        device_name = giop.find_device_name(raw_body, start-4)
        logger.info("Providing access to device {}".format(device_name))
    except ValueError:
        msg = "Could not get device name in {} reply"
        logger.warn(msg.format(IMPORT_DEVICE))
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
    result = giop.find_zmq_endpoints(raw_body)
    if not result:
        return False
    # Filter endpoints
    endpoints, start = result
    nb = len(endpoints)
    if nb > 2:
        logger.info('Discarding {}/{} endpoints'.format(nb-2, nb))
        endpoints = endpoints[:2]
    # Exctract endpoints
    new_endpoints = []
    for endpoint in endpoints:
        host, port = giop.decode_zmq_endpoint(endpoint)
        # Start port forwarding
        _, _, server_port = yield from get_forwarding(
            host, port, HandlerType.ZMQ, bind_address, loop=loop)
        # Make new endpoints
        new_endpoint = giop.encode_zmq_endpoint(bind_address, server_port)
        new_endpoints.append(new_endpoint)
    # Repack body
    return giop.repack_zmq_endpoints(raw_body, new_endpoints, start)


# Run server

def run_gateway_server(bind_address, server_port, tango_host, debug=False):
    """Run a Tango gateway server."""
    # Configure logger
    if debug:
        logger.setLevel(logging.DEBUG)
    # Initialize loop
    loop = asyncio.get_event_loop()
    loop.bind_address = bind_address
    loop.server_port = server_port
    loop.tango_host = tango_host
    loop.forward_dict = {}
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
