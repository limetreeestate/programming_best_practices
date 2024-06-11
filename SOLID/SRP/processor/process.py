from typing import List

from pyspark.sql import DataFrame, Column

from SOLID.SRP.common.log import log_info
from SOLID.SRP.processor.column_processor.column_excluder import drop_cols
from SOLID.SRP.processor.column_processor.column_renamer import rename_col
from SOLID.SRP.processor.column_processor.missing_value_handler import fill_missing_values
from SOLID.SRP.processor.column_processor.standardizer import standardize_feature


def apply_processing(df: DataFrame) -> DataFrame:
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