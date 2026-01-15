from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType,
    DoubleType, DecimalType)

def get_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("spark_ecommerce_pipeline")
        .getOrCreate()
    )

def read_sales_raw(spark: SparkSession, path: str):
    schema = StructType([
        StructField("OrderID", StringType(), False),
        StructField("OrderDate", StringType(), False),
        StructField("Product", StringType(), True),
        StructField("Category", StringType(), True),
        StructField("Brand", StringType(), True),
        StructField("Price", DecimalType(10, 2), True),
        StructField("Quantity", IntegerType(), True),
        StructField("Rating", DoubleType(), True),
        StructField("Reviews", IntegerType(), True),
        StructField("City", StringType(), True),
    ])

    df = (spark.read
          .option("header", "true")
          .schema(schema)
          .csv(path)
          )

    return df