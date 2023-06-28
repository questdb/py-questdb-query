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


def pandas_query(query: str, endpoint: Endpoint = None, chunks: int = 1) -> pd.DataFrame:
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None
    if loop is None:
        return asyncio.run(a.pandas_query(query, endpoint, chunks))
    else:
        return loop.run_until_complete(a.pandas_query(query, endpoint, chunks))


def numpy_query(query: str, endpoint: Endpoint = None, chunks: int = 1) -> dict[str, np.array]:
    df = pandas_query(query, endpoint, chunks)
    return pandas_to_numpy(df)
