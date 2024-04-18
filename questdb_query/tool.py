"""
Benchmarking tool

From the command line, run as::

    python3 -m questdb_query.tool --help

"""

from .endpoint import Endpoint
from .synchronous import pandas_query


def _parse_args():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', type=str, default='localhost')
    parser.add_argument('--port', type=int)
    parser.add_argument('--https', action='store_true')
    parser.add_argument('--username', type=str)
    parser.add_argument('--password', type=str)
    parser.add_argument('--token', type=str)
    parser.add_argument('--chunks', type=int, default=1)
    parser.add_argument('query', type=str)
    return parser.parse_args()


def main(args):
    endpoint = Endpoint(
        host=args.host,
        port=args.port,
        https=args.https,
        username=args.username,
        password=args.password,
        token=args.token)
    df = pandas_query(args.query, endpoint, args.chunks)
    print(df)
    print()
    print(df.query_stats)


if __name__ == "__main__":
    args = _parse_args()
    main(args)
