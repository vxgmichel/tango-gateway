"""Provide helpers to pack and unpack GIOP message."""

import struct
from enum import IntEnum
from collections import namedtuple

# Constants

MAGIC_GIOP = b'GIOP'
MAGIC_IOR = b'IOR:'
GIOP_HEADER_STRUCT = '4sBBBBI'
REPLY_HEADER_STRUCT = 'III'
IOR_STRUCT_1 = 'II'
IOR_STRUCT_2 = IOR_STRUCT_1 + '{:d}sIIIBBHI'
IOR_STRUCT_3 = IOR_STRUCT_2 + '{:d}sH0I'
IOR_LENGTH_STRUCT = 'BBHI{}sH0I'
MIN_IOR_LENGTH = 76
HEXA_DIGIT_SET = set(b'0123456789abcdef')
ZMQ_STRUCT = 'I{:d}sI{:d}s'
ZMQ_TOKEN = b'tcp://'
STRING_TERM = b'\x00'
DEVVARSTRINGARRAY_TOKEN = b'DevVarStringArray\x00'
CSD_OFFSET = 48
IMPORT_DEVICE_ARGOUTS = 6


# Enumerations

class MessageType(IntEnum):
    Request = 0
    Reply = 1
    CancelRequest = 2
    LocateRequest = 3
    LocateReply = 4
    CloseConnection = 5
    MessageError = 6
    Fragment = 7


class ReplyStatus(IntEnum):
    NoException = 0
    UserException = 1
    SystemException = 2
    LocationForward = 3


class Endian(IntEnum):
    Big = 0
    Little = 1


# Structures

GiopHeader = namedtuple(
    'GiopHeader',
    'giop major minor flags message_type size')

ReplyHeader = namedtuple(
    'ReplyHeader',
    'service_context request_id reply_status')

IOR = namedtuple(
    'IOR',
    'first dtype_length dtype nb_profile tag '
    'length major minor wtf host_length host port body')


# Helpers

def print_bytes(string):
    print('Bytes (len={:d}):'.format(len(string)))
    for x in range(0, len(string), 8):
        a = str(string[x:x+4])[2:-1]
        b = str(string[x+4:x+8])[2:-1]
        print('... {:<4d}: {:16s} {:16s}'.format(x, a, b))


def to_byte_string(string):
    return string.encode() + STRING_TERM


def from_byte_string(string):
    assert string[-1:] == STRING_TERM
    return string[:-1].decode()


# ASCII/bytes helpers

def ascii_to_bytes(s):
    return bytes(int(s[i:i+2], 16) for i in range(0, len(s), 2))


def bytes_to_ascii(s):
    return b''.join(format(x, '02x').encode() for x in s)


# GIOP helpers

def unpack_giop_header(bytes_header):
    values = struct.unpack(GIOP_HEADER_STRUCT, bytes_header)
    header = GiopHeader(*values)
    assert header.giop == MAGIC_GIOP
    assert header.major == 1
    assert header.minor in range(3)
    MessageType(header.message_type)
    order = '<' if is_little_endian(header) else '>'
    values = struct.unpack(order + GIOP_HEADER_STRUCT, bytes_header)
    return GiopHeader(*values)


def pack_giop(header, body):
    values = (MAGIC_GIOP,) + header[1:-1] + (len(body),)
    bytes_header = struct.pack(GIOP_HEADER_STRUCT, *values)
    return bytes_header + body


def is_little_endian(header):
    return header.flags % 2 == Endian.Little


def is_last_fragment(header):
    return not (header.flags >> 1) % 2


# Reply helpers

def unpack_reply_header(bytes_header):
    values = struct.unpack(REPLY_HEADER_STRUCT, bytes_header)
    header = ReplyHeader(*values)
    ReplyStatus(header.reply_status)
    return header


def pack_reply(header, body):
    bytes_header = struct.pack(REPLY_HEADER_STRUCT, *header)
    return bytes_header + body


# IOR helpers

def valid_ior(string):
    if len(string) < MIN_IOR_LENGTH or not string.startswith(MAGIC_IOR):
        return False
    return not set(string[4:]) - HEXA_DIGIT_SET


def find_device_name(body, ior_start):
    # Find the the end of the device name lenght frame
    start = body.rfind(b"\x00\x00", None, ior_start)
    # Shift to the previous frame (argouts count of IMPORT_DEVICE command)
    start = start - 6
    argout_size, device_name_len = struct.unpack_from("II", body, start)
    if not argout_size == IMPORT_DEVICE_ARGOUTS:
        raise ValueError("Not a valid body")
    frame_struct = "II{}s".format(device_name_len)
    _, _, device_name = struct.unpack_from(frame_struct, body, start)
    # Remove last \x00
    return device_name[:-1].decode()


def find_ior(body, index=4):
    while True:
        index = body.find(MAGIC_IOR, index)
        if index < 0:
            return False
        size = struct.unpack('I', body[index-4:index])[0]
        ior = body[index:index+size-1]
        if len(ior) == size-1 and valid_ior(ior):
            start = index - 4
            stop = index + struct.calcsize("{:d}s0I".format(size))
            return unmarshal_ior(ior), start, stop
        index += 1


def repack_ior(body, ior, start, stop):
    string = marshal_ior(ior) + b'\x00'
    form = 'I{:d}s0I'.format(len(string))
    ior_struct = struct.pack(form, len(string), string)
    return body[:start] + ior_struct + body[stop:]


def unmarshal_ior(encoded_ior):
    assert encoded_ior[:4] == MAGIC_IOR
    ior = ascii_to_bytes(encoded_ior[4:])
    dtype_length = struct.unpack_from(IOR_STRUCT_1, ior)[-1]
    form = IOR_STRUCT_2.format(dtype_length)
    host_length = struct.unpack_from(form, ior)[-1]
    form = IOR_STRUCT_3.format(dtype_length, host_length)
    values = struct.unpack_from(form, ior)
    values += (ior[struct.calcsize(form):],)
    return IOR(*values)


def marshal_ior(ior):
    ior = update_ior_length(ior)
    form = IOR_STRUCT_3.format(ior.dtype_length, ior.host_length)
    string = struct.pack(form, *ior[:-1])
    string += ior.body
    return MAGIC_IOR + bytes_to_ascii(string)


def update_ior_length(ior):
    d = ior._asdict()
    d['host_length'] = len(ior.host)
    d['dtype_length'] = len(ior.dtype)
    form = IOR_LENGTH_STRUCT.format(len(ior.host))
    d['length'] = struct.calcsize(form) + len(ior.body)
    return IOR(**d)


# CSD Helpers

def find_csd(body):
    index = body.rfind(DEVVARSTRINGARRAY_TOKEN)
    if index < 0:
        return False
    index += CSD_OFFSET
    subbody = body[index:]
    size = struct.unpack_from('I', subbody)[0]
    if len(subbody) != size + 4:
        return False
    return subbody[4:], index


def repack_csd(body, csd, start):
    l = len(csd)
    string = struct.pack('I{:d}s'.format(l), l, csd)
    return body[:start] + string


# ZMQ Helpers

def find_zmq_endpoints(body):
    if body.count(ZMQ_TOKEN) < 2:
        return False
    strings = []
    pattern = 'I'
    index = body.find(ZMQ_TOKEN, 4) - 8
    sub_body = body[index:]
    length, = struct.unpack_from(pattern, sub_body)
    for i in range(length):
        pattern += 'I'
        size = struct.unpack_from(pattern, sub_body)[-1]
        pattern += '{:d}s'.format(size)
        string = struct.unpack_from(pattern, sub_body)[-1]
        strings.append(string)
    return strings, index


def decode_zmq_endpoint(encoded):
    host, port = encoded[:-1].lstrip(ZMQ_TOKEN).decode().split(':')
    port = int(port)
    return host, port


def encode_zmq_endpoint(host, port):
    encoded = ':'.join((host, str(port))).encode()
    return ZMQ_TOKEN + encoded + STRING_TERM


def repack_zmq_endpoints(body, zmqs, start):
    form = 'I' + 'I{:d}s' * len(zmqs)
    pattern = form.format(*map(len, zmqs))
    values = [x for zmq in zmqs for x in (len(zmq), zmq)]
    string = struct.pack(pattern, len(zmqs), *values)
    return body[:start] + string
