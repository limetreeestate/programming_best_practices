import logging
from datetime import datetime as dt
from typing import List, Union, Callable

from pyspark.sql import DataFrame, Column, Window
from pyspark.sql import SparkSession
from pyspark.sql.functions import stddev, mean, col, lit, asc, row_number, desc
from pyspark.sql.functions import when

from SOLID.DIP.common.conf import load_conf

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Example")


def log_info(message: str) -> None:
    logger.info(f"{dt.now()}: {message}")


def log_error(message: str) -> None:
    logger.error(f"{dt.now()}: {message}")


def get_spark() -> SparkSession:
    return (SparkSession.
            builder.
            appName("DataFrame Conversion").
            getOrCreate())


class DataFrameReader:

    def read(self, path: str) -> DataFrame:
        pass


class ParquetDataFrameReader(DataFrameReader):

    def read(self, path: str) -> DataFrame:
        df: DataFrame = None
        # Load parquet file into DataFrame
        try:
            df = get_spark().read.parquet(path)
        except:
            logger.error(dt.now(), "An error has occurred reading dataset")
        finally:
            return df


class CSVDataFrameReader(DataFrameReader):

    def read(self, path: str) -> DataFrame:
        df: DataFrame = None
        # Load parquet file into DataFrame
        try:
            df = get_spark().read.option("header", True).csv(path)
        except:
            log_error("An error has occurred reading dataset")
        finally:
            return df


class DataFrameWriter:

    def write(self, df: DataFrame, path: str) -> None:
        pass


class ParquetDataFrameWriter(DataFrameWriter):

    def write(self, df: DataFrame, path: str) -> None:
        try:
            df.write.parquet(path)
        except:
            log_error("An error has occurred reading dataset")


class DeltaDataFrameWriter(DataFrameWriter):

    def write(self, df: DataFrame, path: str) -> None:
        try:
            df.write.mode("delta").save(path)
        except:
            log_error("An error has occurred reading dataset")


class DataFrameProcessor:

    def process(self, df: DataFrame) -> List[Column]:
        pass


class StandardizerProcessor(DataFrameProcessor):

    def __init__(self,
                 column: str,
                 new_name: str):
        self.column = column
        self.new_name = new_name

    def process(self, df: DataFrame) -> List[Column]:
        stddev_feature = df.select(stddev(self.column)).first()[0]
        mean_feature = df.select(mean(self.column)).first()[0]

        return [((col("feature_a").na - lit(mean_feature)) / lit(stddev_feature)).alias(self.new_name)]


class NullValueProcessor(DataFrameProcessor):

    def __init__(self,
                 col_name: str,
                 replacement: Union[Column, str, int]):
        self.col_name = col_name
        self.replacement = replacement

    def process(self, df: DataFrame) -> List[Column]:
        return [when(col(self.col_name).isNull(), self.replacement).otherwise(col(self.col_name))]


class ColumnRenameProcessor(DataFrameProcessor):

    def __init__(self,
                 old_name: str,
                 new_name: str):
        self.old_name = old_name
        self.new_name = new_name

    def process(self, df: DataFrame) -> List[Column]:
        return [col(self.old_name).alias(self.new_name)]


class ColumnExclusionProcessor(DataFrameProcessor):

    def __init__(self,
                 cols: List[str]):
        self.cols = cols

    def process(self, df: DataFrame) -> List[Column]:
        existing_cols = set(df.columns)
        new_cols = list(existing_cols - set(self.cols))

        return [col(c) for c in new_cols]


class WindowFunctionProcessor(DataFrameProcessor):

    def __init__(self,
                 window_func: Callable,
                 partition_col: str,
                 order_col: str,
                 order_method=asc,
                 new_name: str = "window_col"):
        self.partition_col = partition_col
        self.order_col = order_col
        self.window_func = window_func
        self.order_method = order_method
        self.new_name = new_name

    def process(self, df: DataFrame) -> List[Column]:
        w = Window().partitionBy(self.partition_col).orderBy(self.order_method(self.order_col))
        return [self.window_func().over(w).alias()]


def apply_processing(df: DataFrame,
                     processors: List[DataFrameProcessor]) -> DataFrame:
    # Iterate through processors and apply processing
    cols: List[Column] = None
    for processor in processors:
        cols += processor.process(df)

    return df.select(cols)


def main():
    # Read conf values
    conf: dict = load_conf("SOLID/SRP/conf/general.yaml")
    input_path: str = conf["input_path"]
    output_path: str = conf["output_path"]

    log_info(f"Fetching dataset from path: {input_path}")
    df = CSVDataFrameReader().read(input_path)

    log_info("Processing dataset")
    processors = [
        StandardizerProcessor("age", "std_age"),
        StandardizerProcessor("total_spent", "std_total_spent"),
        NullValueProcessor("gender", -1),
        WindowFunctionProcessor(row_number,
                                "age",
                                "total_spend",
                                desc,
                                "total_spend_on_age_rank"),
        ColumnRenameProcessor("f_name", "first_name"),
        ColumnRenameProcessor("l_name", "last_name"),
        ColumnExclusionProcessor(["address", "nic", "gender", "f_name", "l_name"])
    ]
    processed_df: DataFrame = apply_processing(df, processors)

    log_info(f"Saving dataset: {output_path}")
    writer = DeltaDataFrameWriter
    writer.write(processed_df, output_path)
