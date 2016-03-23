python-tangogateway
===================
***

A Tango gateaway server

Requirements
------------

- python >= 3.4
- zmq
- aiozmq
- pytango (optional)

Usage
-----

```bash
 $ python3 -m tangogateway --help
usage: tangogateway [-h] [--bind ADDRESS] [--port PORT] [--tango HOST]

Run a Tango gateway server

optional arguments:
  -h, --help            show this help message and exit
  --bind ADDRESS, -b ADDRESS
                        Specify the bind address (default is all interfaces)
  --port PORT, -p PORT  Port for the server (default is 8000)
  --tango HOST, -t HOST
                        Tango host (default is given by PyTango)

```

Contact
-------

Vincent Michel: vincent.michel@maxlab.lu.se
