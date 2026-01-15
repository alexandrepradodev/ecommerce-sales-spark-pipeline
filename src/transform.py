from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, lower, trim,
    to_date, when
)

def transform_sales(df: DataFrame) -> DataFrame:
    for col_name in df.columns:
        new_name = col_name.lower().replace(" ", "_")
        df = df.withColumnRenamed(col_name, new_name)

    return df

df_norm = transform_sales()
