import logging

logging.basicConfig(level=logging.INFO)


def process(input_path, output_path):
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import stddev, mean, col
    from pyspark.sql.functions import when

    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("DataFrame Conversion") \
        .getOrCreate()

    # Load parquet file into DataFrame
    df = spark.read.parquet(input_path)

    # Calculate standard deviation and mean of 'age'
    stddev_age = df.select(stddev("age")).first()[0]
    mean_age = df.select(mean("age")).first()[0]

    # Standardize 'age'
    df = df.withColumn("standardized_feature", (col("age") - mean_age) / stddev_age)

    # Calculate standard deviation and mean of 'feature_a'
    stddev_total_spend = df.select(stddev("total_spend")).first()[0]
    mean_total_spend = df.select(mean("total_spend")).first()[0]

    # Standardize 'total_spend'
    df = df.withColumn("standardized_feature", (col("total_spend") - mean_total_spend) / stddev_total_spend)

    # Fill missing values in 'gender' with -1
    df = df.withColumn("filled_feature", when(col("gender").isNull(), -1).otherwise(col("gender")))

    # Drop unnecessary columns
    df = df.drop("address", "nic", "gender", "f_name", "l_name")

    df.write.parquet(output_path)


def main():
    path = "data/data.parquet"
    output_path = "data/preprocessed_data.parquet"
    process(path, output_path)


if __name__ == "__main__":
    main()
