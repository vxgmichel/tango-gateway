tango-gateway
=============

A Tango gateway server

Clients from other networks can connect to the gateway to access the tango
database transparently. It opens ports dynamically when an access to a device
is required and redirects the traffic to the corresponding device. The ZMQ
tango events are also supported.


Requirements
------------

- python >= 3.4
- zmq
- aiozmq
- pytango (optional)


Usage
-----

```
$ tango-gateway -h
usage: tango-gateway [-h] [--bind ADDRESS] [--port PORT] [--tango HOST]

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
