"""
Async functions to query QuestDB over HTTP(S) via CSV into Pandas or Numpy.
"""

__all__ = ['pandas_query', 'numpy_query']

import asyncio
from collections import namedtuple
import time
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
import re
from typing import Optional

import aiohttp
import numpy as np
import pandas as pd

from .endpoint import Endpoint
from .errors import QueryError
from .pandas_util import pandas_to_numpy
from .stats import Stats


class TokenAuth(namedtuple("TokenAuth", ["token"])):
    """Http token (bearer) authentication helper."""

    def __new__(
        cls, token: str, encoding: str = "utf-8"
    ) -> "TokenAuth":
        if token is None:
            raise ValueError("None is not allowed as token value")

        # https://datatracker.ietf.org/doc/html/rfc6750#section-2.1
        if not re.match(r'^[A-Za-z0-9-._~+/]+=*$', token):
            raise ValueError("Invalid characters in token")

        return super().__new__(cls, token)

    @classmethod
    def decode(cls, auth_header: str) -> "TokenAuth":
        """Create a TokenAuth object from an Authorization HTTP header."""
        raise RuntimeError("Not yet implemented: TokenAuth does not support decoding from header")

    @classmethod
    def from_url(cls, url, *, encoding: str = "latin1") -> Optional["TokenAuth"]:
        """Create BasicAuth from url."""
        raise RuntimeError("Not yet implemented: TokenAuth does not support creation from URL") 

    def encode(self) -> str:
        """Encode credentials."""
        return "Bearer " + self.token


def _new_session(endpoint, timeout: int = None):
    auth = None
    if endpoint.username:
        auth = aiohttp.BasicAuth(endpoint.username, endpoint.password)
    elif endpoint.token:
        auth = TokenAuth(endpoint.token)
    timeout = aiohttp.ClientTimeout(total=timeout) \
        or aiohttp.ClientTimeout(total=300)
    return aiohttp.ClientSession(
        auth=auth,
        read_bufsize=4 * 1024 * 1024,
        timeout=timeout)


async def _pre_query(session: aiohttp.ClientSession, endpoint: Endpoint, query: str) -> tuple[
    list[tuple[str, (str, object)]], int]:
    url = f'{endpoint.url}/exec'
    params = [('query', query), ('count', 'true'), ('limit', '0')]
    dtypes_map = {
        'STRING': ('STRING', 'string'),
        'SYMBOL': ('SYMBOL', 'string'),
        'SHORT': ('SHORT', 'int16'),
        'BOOLEAN': ('BOOLEAN', 'bool'),
        'INT': ('INT', 'Int32'),
        'LONG': ('LONG', 'Int64'),
        'DOUBLE': ('DOUBLE', 'float64'),
        'FLOAT': ('FLOAT', 'float32'),
        'CHAR': ('CHAR', 'string'),
        'TIMESTAMP': ('TIMESTAMP', None),
        'IPV4': ('IPV4', 'string'),
        'BYTE': ('BYTE', 'int8'),
        'DATE': ('DATE', None),
        'UUID': ('UUID', 'string'),
        'BINARY': ('BINARY', 'string'),
        'LONG256': ('LONG256', 'string'),
    }

    def get_dtype(col):
        ty = col['type'].upper()
        if ty.startswith('GEOHASH'):
            return (ty, 'string')
        return dtypes_map[ty]

    async with session.get(url=url, params=params) as resp:
        result = await resp.json()
        if resp.status != 200:
            raise QueryError.from_json(result)
        columns = [
            (col['name'], get_dtype(col))
            for col in result['columns']]
        count = result['count']
        return columns, count


async def _query_pandas(
        session: aiohttp.ClientSession,
        executor: ThreadPoolExecutor,
        endpoint: Endpoint,
        query: str,
        result_schema: list[tuple[str, tuple[str, object]]],
        limit_range: tuple[int, int]) -> pd.DataFrame:
    url = f'{endpoint.url}/exp'
    params = [
        ('query', query),
        ('limit', f'{limit_range[0]},{limit_range[1]}')]
    async with session.get(url=url, params=params) as resp:
        if resp.status != 200:
            raise QueryError.from_json(await resp.json())
        buf = await resp.content.read()
        download_bytes = len(buf)
        buf_reader = BytesIO(buf)
        dtypes = {
            col[0]: col[1][1]
            for col in result_schema
            if col[1][1] is not None}

        def _read_csv():
            df = pd.read_csv(buf_reader, dtype=dtypes, engine='pyarrow')
            # Patch up the column types.
            for col_schema in result_schema:
                col_name = col_schema[0]
                col_type = col_schema[1][0]
                try:
                    if col_type in ('TIMESTAMP', 'DATE'):
                        series = df[col_name]
                        # Drop the UTC timezone during conversion.
                        # This allows `.to_numpy()` on the series to
                        # yield a `datetime64` dtype column.
                        series = pd.to_datetime(series).dt.tz_convert(None)
                        df[col_name] = series
                except Exception as e:
                    print(df[col_name])
                    raise ValueError(
                        f'Failed to convert column {col_name} to type {col_type}: {e}\n{series}')
            return df

        loop = asyncio.get_running_loop()
        df = await loop.run_in_executor(executor, _read_csv)
        return df, download_bytes


async def pandas_query(
        query: str,
        endpoint: Endpoint = None,
        chunks: int = 1,
        timeout: int = None) -> pd.DataFrame:
    """
    Query QuestDB via CSV to a Pandas DataFrame.

    :param timeout: The timeout in seconds for the query, defaults to None (300 seconds).
    """
    endpoint = endpoint or Endpoint()
    start_ts = time.perf_counter_ns()
    with ThreadPoolExecutor(max_workers=chunks) as executor:
        async with _new_session(endpoint, timeout) as session:
            result_schema, row_count = await _pre_query(session, endpoint, query)
            chunks = max(min(chunks, row_count), 1)
            rows_per_spawn = row_count // chunks
            limit_ranges = [
                (
                    i * rows_per_spawn,
                    ((i + 1) * rows_per_spawn) if i < chunks - 1 else row_count
                )
                for i in range(chunks)]
            tasks = [
                asyncio.ensure_future(_query_pandas(
                    session, executor, endpoint, query, result_schema, limit_range))
                for limit_range in limit_ranges]
            results = await asyncio.gather(*tasks)
            sub_dataframes = [result[0] for result in results]
            df = pd.concat(sub_dataframes)
            if chunks > 1:
                df.reset_index(drop=True, inplace=True)
            end_ts = time.perf_counter_ns()
            total_downloaded = sum(result[1] for result in results)
            df.query_stats = Stats(end_ts - start_ts, row_count, total_downloaded)
            return df


async def numpy_query(
        query: str,
        endpoint: Endpoint = None,
        chunks: int = 1,
        timeout: int = None
        ) -> dict[str, np.array]:
    """
    Query and obtain the result as a dict of columns.
    Each column is a numpy array.

    :param timeout: The timeout in seconds for the query, defaults to None (300 seconds).
    """
    df = await pandas_query(query, endpoint, chunks, timeout)
    return pandas_to_numpy(df)
