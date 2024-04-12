"""
A sync shim around the `asynchronous` module.
"""

__all__ = ['pandas_query', 'numpy_query']

import asyncio

import numpy as np
import pandas as pd

from . import asynchronous as a
from .endpoint import Endpoint
from .pandas_util import pandas_to_numpy


def pandas_query(
        query: str,
        endpoint: Endpoint = None,
        chunks: int = 1,
        timeout: int = None
        ) -> pd.DataFrame:
    """
    Query QuestDB via CSV to a Pandas DataFrame.

    :param timeout: The timeout in seconds for the query, defaults to None (300 seconds).
    """
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None
    if loop is None:
        return asyncio.run(a.pandas_query(query, endpoint, chunks, timeout))
    else:
        return loop.run_until_complete(a.pandas_query(query, endpoint, chunks, timeout))


def numpy_query(
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
    df = pandas_query(query, endpoint, chunks, timeout)
    return pandas_to_numpy(df)
