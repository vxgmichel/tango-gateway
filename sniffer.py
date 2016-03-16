"""Provide a Tango gateway server."""

import giop
import asyncio
from functools import partial
from contextlib import closing


@asyncio.coroutine
def forward_pipe(reader, writer):
    with closing(writer):
        while not reader.at_eof():
            data = yield from reader.read(4096)
            writer.write(data)


@asyncio.coroutine
def forward(s_reader, s_writer, host, port):
    c_reader, c_writer = yield from asyncio.open_connection(host, port)
    task1 = forward_pipe(s_reader, c_writer)
    task2 = forward_pipe(c_reader, s_writer)
    yield from asyncio.gather(task1, task2)


@asyncio.coroutine
def inspect_pipe(reader, writer):
    with closing(writer):
        while not reader.at_eof():
            data = yield from read_frame(reader)
            if not data:
                break
            writer.write(data)


@asyncio.coroutine
def read_frame(reader):
    # Read header
    loop = reader._loop
    raw_header = yield from reader.read(12)
    if not raw_header:
        return raw_header
    header = giop.unpack_giop_header(raw_header)
    # Read data
    raw_data = yield from reader.read(header.size)
    raw_frame = raw_header + raw_data
    if message_type != giop.MessageType.Reply:
        return raw_frame
    # Unpack reply
    raw_reply_header, raw_body = raw_data[:12], raw_data[12:]
    repy_header = giop.unpack_reply_header(raw_reply_header)
    if reply_header.reply_status != giop.ReplyStatus.NoException:
        return raw_frame
    # Find IOR, host and port
    ior = giop.find_ior(raw_body)
    if not ior:
        return raw_frame
    ior, start, stop = ior
    host = ior.host[:-1].decode()
    key = host, ior.port
    # Start port forwarding
    if key not in loop.forward_dict:
        args = partial(forward, host=host, port=ior.port), loop.bind_address, 0
        server = yield from asyncio.start_server(*args, loop=loop)
        value = (server.sockets[0].getsockname()[1],
                 server.sockets[0].getsockname()[0].encode() + b'\x00')
        loop.forward_dict[key] = value
        print("Forwarding {} to {}...".format(value, key))
    # Patch IOR
    host, port = loop.forward_dict[key]
    ior = ior._replace(host=host, port=port)
    # Repack body
    raw_body = giop.repack_ior(raw_body, ior, start, stop)
    raw_data = raw_reply_header + raw_body
    header = header._replace(size=len(raw_data))
    return giop.pack_giop(header, raw_body)


@asyncio.coroutine
def inspect(server_reader, server_writer):
    client_reader, client_writer = yield from asyncio.open_connection(
        *TANGO_HOST.split(":"), loop=loop)
    task1 = inspect_pipe(server_reader, client_writer)
    task2 = inspect_pipe(client_reader, server_writer)
    yield from asyncio.gather(task1, task2)


def run_server(bind_address, server_port):
    # Initialize loop
    loop = asyncio.get_event_loop()
    loop.bind_address = bind_address
    loop.server_port = server_port
    loop.forward_dict = {}
    # Create server
    coro = asyncio.start_server(inspect, bind_address, server_port)
    server = loop.run_until_complete(coro)
    # Serve requests until Ctrl+C is pressed
    print('Serving on {}'.format(server.sockets[0].getsockname()))
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    # Close the server
    server.close()
    loop.run_until_complete(server.wait_closed())
    loop.close()
