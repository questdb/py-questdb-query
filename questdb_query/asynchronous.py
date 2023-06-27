"""
Async functions to query QuestDB over HTTP(S) via CSV into Pandas or Numpy.
"""

__all__ = ['pandas_query', 'numpy_query']

import asyncio
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO

import aiohttp
import numpy as np
import pandas as pd

from .endpoint import Endpoint
from .errors import QueryError


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
                        series = pd.to_datetime(series)
                        df[col_name] = series
                except Exception as e:
                    raise ValueError(
                        f'Failed to convert column {col_name} to type {col_type}: {e}\n{series}')
            return df

        loop = asyncio.get_running_loop()
        df = await loop.run_in_executor(executor, _read_csv)
        return df, download_bytes


async def pandas_query(query: str, endpoint: Endpoint = None, chunks: int = 1, *, stats: bool = False) -> pd.DataFrame:
    """
    Query QuestDB via CSV to a Pandas DataFrame.
    """
    endpoint = endpoint or Endpoint()
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
            if stats:
                total_downloaded = sum(result[1] for result in results)
                return df, total_downloaded
            else:
                return df


async def numpy_query(query: str, endpoint: Endpoint = None, chunks: int = 1, *, stats: bool = False) -> dict[str, np.array]:
    """
    Query and obtain the result as a dict of columns.
    Each column is a numpy array.
    """
    res = await pandas_query(query, endpoint, chunks, stats=stats)
    df, stats_res = res if stats else (res, None)
    # Calling `.to_numpy()` for each column is quite efficient and generally avoids copies.
    # Pandas already stores columns as numpy.
    # We go through Pandas as this allows us to get fast CSV parsing.
    np_arrays = {col_name: df[col_name].to_numpy() for col_name in df}
    return (np_arrays, stats_res) if stats else np_arrays
