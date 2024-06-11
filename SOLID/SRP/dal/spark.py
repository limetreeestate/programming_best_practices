from pyspark.sql import SparkSession


def get_spark() -> SparkSession:
    return (SparkSession.
            builder.
            appName("DataFrame Conversion").
            getOrCreate())
