__all__ = ['pandas_to_numpy']

import numpy as np
import pandas as pd

from .stats import StatsDict


def pandas_to_numpy(df: pd.DataFrame) -> dict[str, np.array]:
    """
    Convert a pandas dataframe into a dict containing numpy arrays, keyed by column name.

    If the index is named, then convert that too.
    """
    # Calling `.to_numpy()` for each column is quite efficient and generally avoids copies.
    # This is because Pandas internally already usually stores columns as numpy.
    np_arrs = {col_name: df[col_name].to_numpy() for col_name in df}

    # If the index is named, then convert that too.
    if df.index.name:
        np_arrs[df.index.name] = df.index.to_numpy()

    # Carry across stats, if these are present.
    if hasattr(df, 'query_stats'):
        np_arrs = StatsDict(np_arrs, df.query_stats)

    return np_arrs
