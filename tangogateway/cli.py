"""Command line interface for the gateway server."""

# Imports
import argparse
from .gateway import run_gateway_server

# Optional imports
try:
    import PyTango
except ImportError:
    PyTango = None


def main(*args):
    """Run a Tango gateway server from CLI arguments."""
    # Create parser
    parser = argparse.ArgumentParser(
        prog='tango-gateway',
        description='Run a Tango gateway server')
    parser.add_argument(
        '--bind', '-b', metavar='ADDRESS', default='0.0.0.0',
        help='Specify the bind address (default is all interfaces)')
    parser.add_argument(
        '--port', '-p', metavar='PORT', default=10000, type=int,
        help='Port for the server (default is 10000)')
    parser.add_argument(
        '--tango', '-t', metavar='HOST',
        help='Tango host (default is given by PyTango)')
    parser.add_argument('--verbose', '-v', action='store_true')
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
    return run_gateway_server(namespace.bind,
                              namespace.port,
                              namespace.tango,
                              namespace.verbose)


if __name__ == '__main__':
    main()
