from ingestion import get_spark_session, read_sales_raw

spark = get_spark_session()
df = read_sales_raw(spark, "data/raw/sales.csv")

df.printSchema()
df.show(5)
