from pyspark.sql import Column
from pyspark.sql.functions import col


def rename_col(old_name: str, new_name: str) -> Column:
    return col(old_name).alias(new_name)
