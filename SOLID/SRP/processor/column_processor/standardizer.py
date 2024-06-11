from pyspark.sql import DataFrame, Column
from pyspark.sql.functions import stddev, mean, col, lit


def standardize_feature(df: DataFrame, column: str, new_name: str) -> Column:
    stddev_feature = df.select(stddev(column)).first()[0]
    mean_feature = df.select(mean(column)).first()[0]

    return ((col(column).na - lit(mean_feature)) / lit(stddev_feature)).alias(new_name)
