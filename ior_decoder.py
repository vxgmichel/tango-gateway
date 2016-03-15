import struct
from collections import namedtuple


IOR = namedtuple('IOR',
                  'first dtype_length dtype nb_profile tag '
                  'length major minor host_length host port body')


def ascii_to_bytes(s):
     return bytearray(int(s[i:i+2], 16) for i in range(0, len(s), 2))


def bytes_to_ascii(s):
     return b''.join(format(x, '02x').encode() for x in s)


def unmarshal(encoded_ior):
     assert encoded_ior[:4] == b'IOR:'
     ior = ascii_to_bytes(encoded_ior[4:])
     dtype_length = struct.unpack_from('II', ior)[-1]
     form = 'II{}sIIIBBI'.format(dtype_length)
     host_length = struct.unpack_from(form, ior)[-1]
     form = 'II{}sIIIBBI{}sH0I'.format(dtype_length, host_length)
     values = struct.unpack_from(form, ior)
     values += (ior[struct.calcsize(form):],)
     return IOR(*values)


def marshal(ior):
     ior = update_length(ior)
     form = 'II{}sIIIBBI{}sH0I'.format(ior.dtype_length, ior.host_length)
     string = struct.pack(form, *ior[:-1])
     string += ior.body
     return b'IOR:' + bytes_to_ascii(string)

def update_length(ior):
     d = ior._asdict()
     d['host_length'] = len(ior.host)
     d['dtype_length'] = len(ior.dtype)
     form = 'BBI{}sH0I'.format(len(ior.host))
     d['length'] = struct.calcsize(form) + len(ior.body)
     return IOR(**d)
