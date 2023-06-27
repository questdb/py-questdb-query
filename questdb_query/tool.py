"""
Benchmarking tool

From the command line, run as::

    python3 -m questdb_query.tool --help

"""

from .endpoint import Endpoint
from .synchronous import numpy_query


def _parse_args():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', type=str, default='localhost')
    parser.add_argument('--port', type=int)
    parser.add_argument('--https', action='store_true')
    parser.add_argument('--username', type=str)
    parser.add_argument('--password', type=str)
    parser.add_argument('--chunks', type=int, default=1)
    parser.add_argument('query', type=str)
    return parser.parse_args()


def main(args):
    import time
    endpoint = Endpoint(
        host=args.host,
        port=args.port,
        https=args.https,
        username=args.username,
        password=args.password)
    start_time = time.perf_counter()
    np_arrs, total_downloaded = numpy_query(endpoint, args.query, args.chunks, stats=True)
    elapsed = time.perf_counter() - start_time
    print(f'Elapsed: {elapsed}')
    bytes_throughput = total_downloaded / 1024.0 / 1024.0 / elapsed
    print(
        f'Data throughput: {bytes_throughput:.2f} MiB/sec (of downloaded CSV data)')


if __name__ == "__main__":
    args = _parse_args()
    main(args)
