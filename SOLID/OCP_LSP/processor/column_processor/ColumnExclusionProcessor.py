from typing import List

from pyspark.sql import DataFrame, Column
from pyspark.sql.functions import col

from SOLID.OCP_LSP.processor.column_processor.DataFrameProcessor import DataFrameProcessor


class ColumnExclusionProcessor(DataFrameProcessor):

    def __init__(self,
                 cols: List[str]):
        self.cols = cols

    def process(self, df: DataFrame) -> List[Column]:
        existing_cols = set(df.columns)
        new_cols = list(existing_cols - set(self.cols))

        return [col(c) for c in new_cols]

