from typing import List

from pyspark.sql import DataFrame, Column

from SOLID.OCP_LSP.processor.column_processor.DataFrameProcessor import DataFrameProcessor


def apply_processing(df: DataFrame,
                     processors: List[DataFrameProcessor]) -> DataFrame:

    # Iterate through processors and apply processing
    cols: List[Column] = None
    for processor in processors:
        cols += processor.process(df)

    return df.select(cols)