import asyncio
import enum
import struct
import ior_decoder
from functools import partial

TANGO_HOST = "localhost:10000"

MESSAGE_TYPE = ["Request", "Reply", "CancelRequest", "LocateRequest",
                "LocateReply", "CloseConnection", "MessageError", "Fragment"]

REPLY_STATUS_TYPE = ["NO_EXCEPTION,"
                     "USER_EXCEPTION",
                     "SYSTEM_EXCEPTION",
                     "LOCATION_FORWARD"]

IOR_TOKEN = b'IOR'



NETWORK_HOST = "194.47.253.112"


def ascii_to_octet(s):
    return bytes(int(s[i:i + 2], 16) for i in range(0, len(s), 2))


@asyncio.coroutine
def basic_pipe(reader, writer, message):
    while not reader.at_eof():
        data = yield from reader.read(4)
        print(message, ":", data)
        writer.write(data)
    writer.close()

@asyncio.coroutine
def subscribed_handler(hots, port , server_reader, server_writer):
    client_reader, client_writer =yield from asyncio.open_connection(hots, port, loop=loop)
    task1 = basic_pipe(server_reader, client_writer, "--------client to db")
    task2 = basic_pipe(client_reader, server_writer, "---------db to client")
    yield from asyncio.gather(task1, task2)
    print("Close ok")

@asyncio.coroutine
def handle_echo(hots, port , reader, writer):
    data = yield from reader.read(100)
    message = data.decode()
    addr = writer.get_extra_info('peername')
    print("Received %r from %r" % (message, addr))

    print("Send: %r" % message)
    writer.write(data)
    yield from writer.drain()

    print("Close the client socket")
    writer.close()


@asyncio.coroutine
def pipe(reader, writer, message):
    while not reader.at_eof():
        header = yield from reader.read(12)
        if not header:
            break
        print(message, ": Row header:", header)
        assert len(header) == 12
        magic = header[:4].decode()
        print(message, ": magic :", magic)
        assert magic == "GIOP"
        version = header[4], header[5]
        print(message, ": version :", version)
        assert version in [(1, 0), (1, 1)]
        order = "<" if header[6] else ">"
        print(message, ": order :", order)
        message_type = MESSAGE_TYPE[header[7]]
        print(message, ": message_type :", message_type)
        size = struct.unpack(order + "L", header[8:12])[0]
        print(message, ": size :", size)

        data = yield from reader.read(size)
        if message_type == "Reply":
            body_header = data[0:12]
            service_context = struct.unpack(order + "L", body_header[0:4])[0]
            request_id = struct.unpack(order + "L", body_header[4:8])[0]
            reply_status = struct.unpack(order + "L", body_header[8:12])[0]
            print(message, ": body header : service_context :", service_context)
            print(message, ": body header : request_id :", request_id)
            print(message, ": body header : reply_status :", reply_status)
            body = data[12:]
            for i in range(0, len(body), 8):
                print(message, ": body :", body[i:i+4], body[i+4:i+8])
            if IOR_TOKEN in body:
                index = [i for i in range(len(body)) if
                         body[i:i + len(IOR_TOKEN)] == IOR_TOKEN][0]
                ior_size = struct.unpack(order + "L", body[index - 4:index])[0]
                ior = body[index:index + ior_size - 1]
                res = ior_decoder.unmarshal(ior)
                #host, port = res


                host = res.host[:-1].decode()
                port = int(res.port)

                server = yield from  asyncio.start_server(partial(subscribed_handler, host , port ), '0.0.0.0', 0, loop=loop)
                socket_port = server.sockets[0].getsockname()[1]

                d = res._asdict()
                d["port"] = socket_port
                d["host"] = NETWORK_HOST.encode() + b'\x00'
                ior = ior_decoder.IOR(**d)
                ior = ior_decoder.marshal(ior) + b"\x00"
                patch = struct.pack("I{}s0I".format(len(ior)), len(ior), ior)
                stop = index + struct.calcsize("{}s0I".format(ior_size))
                start = index-4
                new_body = body[:start] + patch + body[stop:]
                new_size = size - len(body) + len(new_body)
                data = data[:12] + new_body
                header = header[:8] + struct.pack("I", new_size)
                foo = header + data
                #for i in range(0, len(foo), 8):
                #    print(message, ":", i, "--", foo[i:i + 4], foo[i + 4:i + 8])
                for i in range(0, len(body), 4):
                    print(message, ":", i, "--", body[i:i + 4], new_body[i:i +4], body[i:i + 4] ==  new_body[i:i +4])

        else:
            print(message, ": body :", data)
        writer.write(header + data)
    writer.close()


@asyncio.coroutine
def handle_request(server_reader, server_writer):
    client_reader, client_writer = yield from asyncio.open_connection(
        *TANGO_HOST.split(":"), loop=loop)
    task1 = pipe(server_reader, client_writer, "client to db")
    task2 = pipe(client_reader, server_writer, "db to client")
    yield from asyncio.gather(task1, task2)
    print("Close ok")


loop = asyncio.get_event_loop()
loop.set_debug(True)
coro = asyncio.start_server(handle_request, '0.0.0.0', 8888, loop=loop)
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
