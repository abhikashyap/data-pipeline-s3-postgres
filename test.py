from pyspark.sql import SparkSession
from pyspark.sql import DataFrame  # or: from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as F, types as T

spark = SparkSession.builder.appName("check").getOrCreate() # type: ignore # ignore

df: DataFrame = (
    spark.read.format("csv")
    .option("inferSchema", True)
    .option("header", True)
    .load("fk.csv")
)
df.show()
