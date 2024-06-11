from typing import Callable, List

from pyspark.sql import DataFrame, Column, Window
from pyspark.sql.functions import asc

from SOLID.DIP.DIP import DataFrameProcessor


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
