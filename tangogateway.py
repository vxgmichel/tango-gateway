"""Provide a Tango gateway server."""

import giop
import PyTango
import asyncio
import argparse
from enum import Enum
from functools import partial
from contextlib import closing


class Patch(Enum):
    NONE = 0
    IOR = 1
    ZMQ = 2


@asyncio.coroutine
def forward(client_reader, client_writer, host, port, patch=Patch.NONE):
    debug = patch == Patch.NONE
    ds_reader, ds_writer = yield from asyncio.open_connection(host, port)
    task1 = inspect_pipe(client_reader, ds_writer, Patch.NONE, debug=debug)
    task2 = inspect_pipe(ds_reader, client_writer, patch, debug=debug)
    yield from asyncio.gather(task1, task2)


@asyncio.coroutine
def inspect_pipe(reader, writer, patch=Patch.NONE, debug=False):
    bind_address = writer._transport._sock.getsockname()[0]
    with closing(writer):
        while not reader.at_eof():
            data = yield from read_frame(reader, bind_address, patch, debug)
            if debug and data:
                rhost, rport = reader._transport._sock.getsockname()
                whost, wport = writer._transport._sock.getsockname()
                origin = ':'.join((rhost, str(rport)))
                origin += ' -> ' + ':'.join((whost, str(wport)))
                print(origin.center(len(origin) + 2).center(60, '#'))
                giop.print_bytes(data)
                data = data.replace(b'8\x00tango://10.0.3.1:10000', b'<\x00tango://194.47.253.49:8888')
                data = data.replace(b'8\x01tango://10.0.3.1:10000', b'<\x01tango://194.47.253.49:8888')
                data = data.replace(b'2\x00tango://10.0.3.1:10000', b'6\x00tango://194.47.253.49:8888')
                data = data.replace(b'2\x01tango://10.0.3.1:10000', b'6\x01tango://194.47.253.49:8888')
                print(data)
            if b'10.0.3.1' in data:
                print('!'*20)
                print(data)
                print('!'*20)
            writer.write(data)


@asyncio.coroutine
def read_frame(reader, bind_address, patch=Patch.NONE, debug=False):
    # No patch
    if patch == Patch.NONE:
        return (yield from reader.read(4096))
    # Read header
    loop = reader._loop
    raw_header = yield from reader.read(12)
    if not raw_header:
        return raw_header
    header = giop.unpack_giop_header(raw_header)
    if debug:
        print(header)
    # Read data
    raw_data = yield from reader.read(header.size)
    raw_frame = raw_header + raw_data
    if header.message_type != giop.MessageType.Reply:
        return raw_frame
    # Unpack reply
    raw_reply_header, raw_body = raw_data[:12], raw_data[12:]
    reply_header = giop.unpack_reply_header(raw_reply_header)
    if debug:
        print(reply_header)
    if reply_header.reply_status != giop.ReplyStatus.NoException or \
       header.order != giop.LITTLE_ENDIAN:
        return raw_frame
    if debug:
        giop.print_bytes(raw_body)
    # Patch body
    if patch == Patch.IOR:
        new_body = yield from check_ior(raw_body, bind_address, loop)
    elif patch == Patch.ZMQ:
        new_body = yield from check_zmq(raw_body, bind_address, loop)
    # Ignore
    if not new_body:
        return raw_frame
    # Repack frame
    raw_data = raw_reply_header + new_body
    return giop.pack_giop(header, raw_data)


@asyncio.coroutine
def check_ior(raw_body, bind_address, loop):
    # Find IOR, host and port
    ior = giop.find_ior(raw_body)
    if not ior:
        return False
    ior, start, stop = ior
    host = ior.host[:-1].decode()
    key = host, ior.port, bind_address
    # Start port forwarding
    if key not in loop.forward_dict:
        value = yield from start_forward(
            host, ior.port, bind_address, loop, Patch.ZMQ)
        loop.forward_dict[key] = value
    # Patch IOR
    server, host, port = loop.forward_dict[key]
    ior = ior._replace(host=host.encode() + giop.STRING_TERM, port=port)
    if b'10.0.3.1' in ior.body:
        print('!'*20)
        print(ior.body)
        print('!'*20)
    # Repack body
    return giop.repack_ior(raw_body, ior, start, stop)


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
        if key not in loop.forward_dict:
            value = yield from start_forward(
                host, port, bind_address, loop, Patch.NONE)
            loop.forward_dict[key] = value
        # Make new endpoints
        server, host, port = loop.forward_dict[key]
        new_endpoints.append(giop.encode_zmq_endpoint(host, port))
    # Repack body
    zmq1, zmq2 = new_endpoints
    return giop.repack_zmq_endpoints(raw_body, zmq1, zmq2, start)


@asyncio.coroutine
def start_forward(host, port, bind_address, loop, patch=Patch.NONE):
    # Start port forwarding
    handler = partial(forward, host=host, port=port, patch=patch)
    server = yield from asyncio.start_server(
        handler, bind_address, 0, loop=loop)
    value = (
        server,
        server.sockets[0].getsockname()[0],
        server.sockets[0].getsockname()[1],)
    msg = "Forwarding {0[0]} port {0[1]} to {1[0]} port {1[1]}..."
    print(msg.format(value[1:], (host, port)))
    return value


def run_server(bind_address, server_port, tango_host):
    """Run a Tango gateway server."""
    # Initialize loop
    loop = asyncio.get_event_loop()
    loop.bind_address = bind_address
    loop.server_port = server_port
    loop.tango_host = tango_host
    loop.forward_dict = {}
    # Create server
    host, port = tango_host
    handler = partial(forward, host=host, port=port, patch=Patch.IOR)
    coro = asyncio.start_server(handler, bind_address, server_port)
    server = loop.run_until_complete(coro)
    # Serve requests until Ctrl+C is pressed
    msg = ('Serving a Tango gateway to {0[0]} port {0[1]} '
           'on {1[0]} port {1[1]} ...')
    print(msg.format(loop.tango_host, server.sockets[0].getsockname()))
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    # Close all the servers
    servers = [server for server, host, port in loop.forward_dict.values()]
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
    parser = argparse.ArgumentParser(description='Run a Tango gateway server.')
    parser.add_argument('--bind', '-b', metavar='ADDRESS', default='',
                        help='Specify the bind address '
                        '(default is all interfaces)')
    parser.add_argument('--port', '-p', metavar='PORT', default=8000,
                        help='Port for the server (default is 8000)')
    parser.add_argument('--tango', '-t', metavar='HOST',
                        help='Tango host (default is $TANGO_HOST)')
    # Parse arguments
    namespace = parser.parse_args(*args)
    # Check Tango database
    if namespace.tango:
        db = PyTango.Database(namespace.tango)
    else:
        db = PyTango.Database()
    namespace.tango = db.get_db_host(), int(db.get_db_port())
    # Run the server
    return run_server(namespace.bind, namespace.port, namespace.tango)


if __name__ == '__main__':
    main()
