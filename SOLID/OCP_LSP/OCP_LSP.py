import logging
from datetime import datetime as dt
from typing import List, Union

from pyspark.sql import DataFrame, Column
from pyspark.sql import SparkSession
from pyspark.sql.functions import stddev, mean, col, lit
from pyspark.sql.functions import when

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
        return [when(col(self.col_name).isNull(), self.replacement)
                .otherwise(col(self.col_name))
                .alias(self.col_name + "_filled")]


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


def apply_processing(df: DataFrame,
                     processors: List[DataFrameProcessor]) -> DataFrame:

    # Iterate through processors and apply processing
    cols: List[Column] = None
    for processor in processors:
        cols += processor.process(df)

    return df.select(cols)


def main():
    path = "data/data.parquet"

    log_info(f"Fetching dataset from path: {path}")
    df = CSVDataFrameReader().read(path)

    log_info("Processing dataset")
    processors = [
        StandardizerProcessor("age", "std_age"),
        StandardizerProcessor("total_spent", "std_total_spent"),
        NullValueProcessor("gender", -1),
        ColumnRenameProcessor("f_name", "first_name"),
        ColumnRenameProcessor("l_name", "last_name"),
        ColumnExclusionProcessor(["address", "nic", "gender", "f_name", "l_name"])
    ]
    processed_df: DataFrame = apply_processing(df, processors)

    output_path = "data/preprocessed_data.parquet"
    log_info(f"Saving dataset: {output_path}")
    writer = DeltaDataFrameWriter()
    writer.write(processed_df, output_path)
