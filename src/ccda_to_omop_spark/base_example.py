
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import pandas_udf

spark = SparkSession.builder.getOrCreate()

df = spark.createDataFrame( [
    Row(a=1, b=2, c='string 1'),
    Row(a=2, b=3, c='string 2'),
    Row(a=4, b=5, c='string 3'),
])
df.show()

@pandas_udf('long')
def pandas_plus_one(series: pd.Series) -> pd.Series:
    return series + 1

df.select(pandas_plus_one(df.a)).show()

