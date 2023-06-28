"""
Query QuestDB over HTTP into Pandas or Numpy arrays.

The primary implementation is in the `asynchronous` module, with a wrapper

"""

__version__ = '0.1.0'

from .endpoint import Endpoint
from .errors import QueryError
from .synchronous import pandas_query, numpy_query
from .pandas_util import pandas_to_numpy
