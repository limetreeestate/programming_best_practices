from pyspark.sql import DataFrame

from SOLID.OCP_LSP.common.log import log_error
from SOLID.OCP_LSP.dal.reader.DataFrameReader import DataFrameReader
from SOLID.OCP_LSP.dal.spark import get_spark


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

