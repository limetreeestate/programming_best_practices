from typing import List

from pyspark.sql import DataFrame
from pyspark.sql.functions import row_number, desc

from SOLID.DIP.dal.reader.CSVDataFrameReader import CSVDataFrameReader
from SOLID.DIP.dal.writer.DeltaDataFrameWriter import DeltaDataFrameWriter
from SOLID.DIP.processor.column_processor.ColumnExclusionProcessor import ColumnExclusionProcessor
from SOLID.DIP.processor.column_processor.ColumnRenameProcessor import ColumnRenameProcessor
from SOLID.DIP.processor.column_processor.NullValueProcessor import NullValueProcessor
from SOLID.DIP.processor.column_processor.StandardizerProcessor import StandardizerProcessor
from SOLID.DIP.common.conf import load_conf
from SOLID.DIP.common.log import log_info
from SOLID.DIP.processor.column_processor.WindowFunctionProcessor import WindowFunctionProcessor
from SOLID.DIP.processor.process import apply_processing


def main():
    # Read conf values
    conf: dict = load_conf("SOLID/SRP/conf/general.yaml")
    input_path: str = conf["input_path"]
    output_path: str = conf["output_path"]
    drop_cols: List[str] = conf["drop_cols"]

    # Read dataset
    log_info(f"Fetching dataset from path: {input_path}")
    df = CSVDataFrameReader().read(input_path)

    log_info("Processing dataset")
    # Define processors
    processors = [
        StandardizerProcessor("age", "std_age"),
        StandardizerProcessor("total_spent", "std_total_spent"),
        NullValueProcessor("gender", -1),
        WindowFunctionProcessor(row_number,
                                "age",
                                "total_spend",
                                desc,
                                "total_spend_on_age_rank"),
        ColumnRenameProcessor("f_name", "first_name"),
        ColumnRenameProcessor("l_name", "last_name"),
        ColumnExclusionProcessor(drop_cols)
    ]
    # Apply processors
    processed_df: DataFrame = apply_processing(df, processors)

    # Save dataset
    log_info(f"Saving dataset: {output_path}")
    writer = DeltaDataFrameWriter()
    writer.write(processed_df, output_path)


if __name__ == "__main__":
    main()