from pyspark.sql import SparkSession
from pyspark.sql import DataFrame  # or: from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as F, types as T
from main.utility.logging_config import logger
try:
    spark = SparkSession.builder.appName("check").getOrCreate() # type: ignore
    logger.debug("Spark session created")

    df: DataFrame = (
        spark.read.format("csv")
        .option("inferSchema", True)
        .option("header", True)
        .load("../fk.csv")
    )
    logger.info(f"No of rows in {df.count()}")

except Exception as e:
    logger.exception(f"Spark job failed with error: {e}")
