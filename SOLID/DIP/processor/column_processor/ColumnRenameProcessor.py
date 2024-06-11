from typing import List

from pyspark.sql import DataFrame, Column
from pyspark.sql.functions import col

from SOLID.OCP_LSP.processor.column_processor.DataFrameProcessor import DataFrameProcessor


class ColumnRenameProcessor(DataFrameProcessor):

    def __init__(self,
                 old_name: str,
                 new_name: str):
        self.old_name = old_name
        self.new_name = new_name

    def process(self, df: DataFrame) -> List[Column]:
        return [col(self.old_name).alias(self.new_name)]