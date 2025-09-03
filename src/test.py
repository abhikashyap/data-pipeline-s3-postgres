from pyspark.sql import SparkSession
from pyspark.sql import DataFrame  # or: from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as F, types as T
from main.utility.logging_config import logger
from resources.config_use import config
test_file_path=config['Files']["fk_csv"]
try:
    spark = SparkSession.builder.appName("check").getOrCreate() # type: ignore
    logger.debug("Spark session created")

    df: DataFrame = (
        spark.read.format("csv")
        .option("inferSchema", True)
        .option("header", True)
        .load(test_file_path)
    )
    logger.info(f"No of rows in {df.count()}")

except Exception as e:
    logger.exception(f"Spark job failed with error: {e}")
