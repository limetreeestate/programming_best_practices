from typing import Any

from pyspark.sql import Column
from pyspark.sql.functions import when, col


def fill_missing_values(col_name: str, replacement: Any) -> Column:
        return (when(col(col_name).isNull(), replacement)
                .otherwise(col(col_name))
                .alias(col_name + "_filled"))
