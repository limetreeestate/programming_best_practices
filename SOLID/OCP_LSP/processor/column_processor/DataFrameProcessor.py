from typing import List

from pyspark.sql import DataFrame, Column


class DataFrameProcessor:

    def process(self, df: DataFrame) -> List[Column]:
        pass