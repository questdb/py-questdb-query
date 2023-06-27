"""
A sync shim around the `asynchronous` module.
"""

__all__ = ['pandas_query', 'numpy_query']

import asyncio

import numpy as np
import pandas as pd

from . import asynchronous as a
from .endpoint import Endpoint


def _syncify(async_fn, *args, **kwargs):
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None
    if loop is None:
        return asyncio.run(async_fn(*args, **kwargs))
    else:
        return loop.run_until_complete(async_fn(*args, **kwargs))


def pandas_query(query: str, endpoint: Endpoint = None, chunks: int = 1, *, stats: bool = False) -> pd.DataFrame:
    return _syncify(a.pandas_query(query, endpoint, chunks, stats=stats))


def numpy_query(query: str, endpoint: Endpoint = None, chunks: int = 1, *, stats: bool = False) -> dict[str, np.array]:
    return _syncify(a.numpy_query(query, endpoint, chunks, stats=stats))
