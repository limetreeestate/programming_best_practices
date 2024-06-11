from pyspark.sql import DataFrame

from SOLID.OCP_LSP.common.log import log_error
from SOLID.OCP_LSP.dal.writer.DataFrameWriter import DataFrameWriter


class ParquetDataFrameWriter(DataFrameWriter):

    def write(self, df: DataFrame, path: str) -> None:
        try:
            df.write.parquet(path)
        except:
            log_error("An error has occurred reading dataset")