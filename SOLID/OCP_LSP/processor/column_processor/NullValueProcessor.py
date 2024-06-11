from typing import Union, List

from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import when, col

from SOLID.OCP_LSP.processor.column_processor.DataFrameProcessor import DataFrameProcessor


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

