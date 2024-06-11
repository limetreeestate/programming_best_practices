from pyspark.sql import DataFrame

from SOLID.SRP.common.conf import load_conf
from SOLID.SRP.common.log import log_info
from SOLID.SRP.dal.io import read_dataset, write_dataset
from SOLID.SRP.processor.process import apply_processing


def main():
    # Read conf values
    conf: dict = load_conf("SOLID/SRP/conf/general.yaml")
    input_path = conf["input_path"]
    output_path = conf["output_path"]

    log_info(f"Fetching dataset from path: {input_path}")
    df = read_dataset(input_path)

    log_info("Processing dataset")
    processed_df: DataFrame = apply_processing(df)

    log_info(f"Saving dataset: {output_path}")
    write_dataset(processed_df, output_path)


if __name__ == "__main__":
    main()