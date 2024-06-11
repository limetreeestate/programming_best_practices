from pyspark.sql import DataFrame

from SOLID.SRP.common.log import log_error
from SOLID.SRP.dal.spark import get_spark


def read_dataset(path: str) -> DataFrame:
    df: DataFrame = None
    # Load parquet file into DataFrame
    try:
        df = get_spark().read.parquet(path)
    except:
        log_error("An error has occurred reading dataset")
    finally:
        return df


def write_dataset(df: DataFrame, path: str) -> None:
    try:
        df.write.parquet(path)
    except:
        log_error("An error has occurred reading dataset")