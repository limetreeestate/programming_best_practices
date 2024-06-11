import logging

logging.basicConfig(level=logging.INFO)


def process(path, path2):
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import stddev, mean, col
    from pyspark.sql.functions import when

    s = SparkSession.builder \
        .appName("DataFrame Conversion") \
        .getOrCreate()

    df = s.read.parquet(path)

    stddev = df.select(stddev("age")).first()[0]
    mean2 = df.select(mean("age")).first()[0]

    df = df.withColumn("standardized_age", (col("age") - mean2) / stddev)

    stddev = df.select(stddev("total_spent")).first()[0]
    mean2 = df.select(mean("total_spent")).first()[0]

    df = df.withColumn("standardized_ts", (col("total_spent") - mean2) / stddev)

    df = df.withColumn("filled_feature", when(col("gender").isNull(), -1).otherwise(col("gender")))

    df = df.withColumn("filled_feature_age", when(col("age").isNull(), -1).otherwise(col("age")))
    df = df.withColumn("filled_feature_age", when(col("total_spent").isNull(), -1).otherwise(col("total_spent")))

    df = df.drop("address", "nic")

    most_common_embarked = df.groupBy("Embarked").count().orderBy(col("count").desc()).first()[0]
    df = df.withColumn("Embarked", when(col("Embarked").isNull(), most_common_embarked).otherwise(col("Embarked")))


    min_fare = df.selectExpr("min(Fare)").first()[0]
    max_fare = df.selectExpr("max(Fare)").first()[0]
    df = df.withColumn("Fare", (col("Fare") - min_fare) / (max_fare - min_fare))

    df.show()

    df.write.parquet(path2)

    s.stop()

    return df


def main():
    path = "data/data.parquet"
    output_path = "data/preprocessed_data.parquet"
    process(path, output_path)


if __name__ == "__main__":
    main()
