from typing import List

from pyspark.sql import DataFrame, Column
from pyspark.sql.functions import stddev, mean, col, lit

from SOLID.OCP_LSP.processor.column_processor.DataFrameProcessor import DataFrameProcessor


class StandardizerProcessor(DataFrameProcessor):

    def __init__(self,
                 column: str,
                 new_name: str):
        self.column = column
        self.new_name = new_name

    def process(self, df: DataFrame) -> List[Column]:
        stddev_feature = df.select(stddev(self.column)).first()[0]
        mean_feature = df.select(mean(self.column)).first()[0]

        return [((col(self.column).na - lit(mean_feature)) / lit(stddev_feature)).alias(self.new_name)]

