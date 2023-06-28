"""
Async functions to query QuestDB over HTTP(S) via CSV into Pandas or Numpy.
"""

__all__ = ['pandas_query', 'numpy_query']

import asyncio
import time
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO

import aiohttp
import numpy as np
import pandas as pd

from .endpoint import Endpoint
from .errors import QueryError
from .pandas_util import pandas_to_numpy
from .stats import Stats


def _new_session(endpoint):
    auth = None
    if endpoint.username is not None:
        if endpoint.password is not None:
            raise ValueError('Password specified without username')
        auth = aiohttp.BasicAuth(endpoint.username, endpoint.password)
    return aiohttp.ClientSession(auth=auth)


async def _pre_query(session: aiohttp.ClientSession, endpoint: Endpoint, query: str) -> tuple[
    list[tuple[str, (str, object)]], int]:
    url = f'{endpoint.url}/exec'
    params = [('query', query), ('count', 'true'), ('limit', '0')]
    dtypes_map = {
        'STRING': ('STRING', None),
        'SYMBOL': ('SYMBOL', None),
        'DOUBLE': ('DOUBLE', 'float64'),
        'FLOAT': ('FLOAT', 'float32'),
        'CHAR': ('CHAR', None),
        'TIMESTAMP': ('TIMESTAMP', None)
    }
    async with session.get(url=url, params=params) as resp:
        result = await resp.json()
        if resp.status != 200:
            raise QueryError.from_json(result)
        columns = [
            (col['name'], dtypes_map[col['type'].upper()])
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
                    if col_type == 'TIMESTAMP':
                        series = df[col_name]
                        # Drop the UTC timezone during conversion.
                        # This allows `.to_numpy()` on the series to
                        # yield a `datetime64` dtype column.
                        series = pd.to_datetime(series).dt.tz_convert(None)
                        df[col_name] = series
                except Exception as e:
                    raise ValueError(
                        f'Failed to convert column {col_name} to type {col_type}: {e}\n{series}')
            return df

        loop = asyncio.get_running_loop()
        df = await loop.run_in_executor(executor, _read_csv)
        return df, download_bytes


async def pandas_query(query: str, endpoint: Endpoint = None, chunks: int = 1) -> pd.DataFrame:
    """
    Query QuestDB via CSV to a Pandas DataFrame.
    """
    endpoint = endpoint or Endpoint()
    start_ts = time.perf_counter_ns()
    with ThreadPoolExecutor(max_workers=chunks) as executor:
        async with _new_session(endpoint) as session:
            result_schema, row_count = await _pre_query(session, endpoint, query)
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
            end_ts = time.perf_counter_ns()
            total_downloaded = sum(result[1] for result in results)
            df.query_stats = Stats(end_ts - start_ts, row_count, total_downloaded)
            return df


async def numpy_query(query: str, endpoint: Endpoint = None, chunks: int = 1) -> dict[str, np.array]:
    """
    Query and obtain the result as a dict of columns.
    Each column is a numpy array.
    """
    df = await pandas_query(query, endpoint, chunks)
    return pandas_to_numpy(df)
