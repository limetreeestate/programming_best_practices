import logging
from datetime import datetime as dt
from typing import List, Any

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


def standardize_feature(df: DataFrame, column: str, new_name: str) -> Column:
    stddev_feature = df.select(stddev(column)).first()[0]
    mean_feature = df.select(mean(column)).first()[0]

    return ((col(column).na - lit(mean_feature)) / lit(stddev_feature)).alias(new_name)


def drop_cols(df: DataFrame, cols: List[str]) -> List[Column]:
    existing_cols = set(df.columns)
    new_cols = list(existing_cols - set(cols))

    return [col(c) for c in new_cols]


def fill_missing_values(col_name: str, replacement: Any) -> Column:
    return when(col(col_name).isNull(), replacement).otherwise(col(col_name)).alias(col_name + "_filled")


def rename_col(old_name: str, new_name: str) -> Column:
    return col(old_name).alias(new_name)


def process(df: DataFrame) -> DataFrame:
    # Standardize columns
    log_info("Standardizing columns")
    standardized_age: Column = standardize_feature(df, "age", "std_age")
    standardized_total_spent: Column = standardize_feature(df, "total_spent", "std_total_spent")

    # Fill missing values
    log_info("Filling null values")
    gender_filled: Column = fill_missing_values("gender", -1)

    # Rename columns appropriately
    log_info("Renaming columns")
    first_name: Column = rename_col("f_name", "first_name")
    last_name: Column = rename_col("l_name", "last_name")

    # Drop unnecessary columns
    log_info("Dropping unwanted columns")
    cols_after_exclusion: List[Column] = drop_cols(["address", "nic", "gender", "f_name", "l_name"])

    # Combine projection
    project_cols: List[Column] = cols_after_exclusion + [standardized_age,
                                                         standardized_total_spent,
                                                         gender_filled,
                                                         first_name,
                                                         last_name]

    return df.select(project_cols)


def main():
    path = "data/data.parquet"

    log_info(f"Fetching dataset from path: {path}")
    df = read_dataset(path)

    log_info("Processing dataset")
    processed_df: DataFrame = process(df)

    output_path = "data/preprocessed_data.parquet"
    log_info(f"Saving dataset: {output_path}")
    write_dataset(processed_df, output_path)


if __name__ == "__main__":
    main()
