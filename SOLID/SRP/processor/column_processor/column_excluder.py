from typing import List

from pyspark.sql import DataFrame, Column
from pyspark.sql.functions import col


def drop_cols(df: DataFrame, cols: List[str]) -> List[Column]:
    existing_cols = set(df.columns)
    new_cols = list(existing_cols - set(cols))

    return [col(c) for c in new_cols]
