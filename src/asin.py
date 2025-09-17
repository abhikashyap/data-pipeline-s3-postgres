ip='172.31.19.202'
db='amazon_sp_api'
password='Techblooprint123'
user="blooprint"
from pyspark.sql import SparkSession
import os
from clickhouse_driver import Client

spark = SparkSession.builder \
    .appName("BigCSVWrite") \
    .config("spark.driver.memory", "5g") \
    .config("spark.executor.memory", "6g") \
    .getOrCreate()
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "50")
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame  # or: from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as F, types as T
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
from pyspark.sql.functions import broadcast
from pyspark.sql import functions as F
# Should not raise:
spark.sparkContext._jvm.java.lang.Class.forName("org.postgresql.Driver")

URL  = os.getenv("PG_JDBC_URL", f"jdbc:postgresql://{ip}:5433/{db}")
USER = os.getenv("PG_USER", user)
PWD  = os.getenv("PG_PASSWORD", password)
portfolio_query = "(SELECT * FROM public.portfolio_id_name_mapping) AS p"

portfolio_id_name_mapping = (spark.read.format("jdbc")
      .option("url", URL)
      .option("user", USER)
      .option("password", PWD)
      .option("driver", "org.postgresql.Driver")
      .option("dbtable", portfolio_query)
      .load())

sp_advertised_query = """
(SELECT account_name,
            "advertisedAsin" AS asin,
            "campaignId" AS campaign_id,
            "campaignName" AS campaign_name,
            "adGroupName" AS ad_group_name,
            date AS date,
            impressions,
            clicks,
            spend AS ads_spend,
            sales30d AS ads_sales,
            "unitsSoldClicks30d" AS units
           FROM public.sp_advertised_product) as ad_p

"""

sp_advertised_df = (spark.read.format("jdbc")
      .option("url", URL)
      .option("user", USER)
      .option("password", PWD)
      .option("driver", "org.postgresql.Driver")
      .option("dbtable", sp_advertised_query)
      .load())
sp_advertised_df=sp_advertised_df.withColumn("date",
    F.col("date").cast(T.DateType())
)
sd_advertised_query = """
(SELECT account_name,
            "promotedAsin" AS asin,
            "campaignId" AS campaign_id,
            "campaignName" AS campaign_name,
            "adGroupName" AS ad_group_name,
            date AS date,
            impressions,
            clicks,
            cost AS ads_spend,
            sales AS ads_sales,
            "unitsSold" AS units
           FROM public.sd_advertised_product) as sd_advertised_query
"""

sd_advertised_df = (spark.read.format("jdbc")
      .option("url", URL)
      .option("user", USER)
      .option("password", PWD)
      .option("driver", "org.postgresql.Driver")
      .option("dbtable", sd_advertised_query)
      .load())
sd_advertised_df=sd_advertised_df.withColumn("date",
    F.col("date").cast(T.DateType())
)
sb_advertised_query = """
(select account_name,
            "purchasedAsin" AS asin,
            "campaignId" AS campaign_id,
            "campaignName" AS campaign_name,
            "adGroupName" AS ad_group_name,
            date AS date,
            NULL::numeric AS impressions,
            NULL::numeric AS clicks,
            NULL::numeric AS ads_spend,
            sales14d AS ads_sales,
            "unitsSold14d" AS units from public.sb_purchased_product) as sb_advertised_query
"""

sb_advertised_df = (spark.read.format("jdbc")
      .option("url", URL)
      .option("user", USER)
      .option("password", PWD)
      .option("driver", "org.postgresql.Driver")
      .option("dbtable", sb_advertised_query)
      .load())
sb_advertised_df=sd_advertised_df.withColumn("date",
    F.col("date").cast(T.DateType())
)
advertised_asin=sp_advertised_df.unionByName(sb_advertised_df).unionByName(sd_advertised_df)
advertised_asin=advertised_asin.withColumn("campaign_id",
                           F.split(
                               F.col("campaign_id").cast(T.StringType()),r'\.'
                               )[0].astype("long")
                            )
campaign_query = "(SELECT * FROM public.campaigns) as c"
campaigns = (spark.read.format("jdbc")
      .option("url", URL)
      .option("user", USER)
      .option("password", PWD)
      .option("driver", "org.postgresql.Driver")
      .option("dbtable", campaign_query)
      .load())

campaign_df=campaigns.select(F.col("campaignId").alias("campaign_id"),
                 F.col("portfolioId").alias("portfolioId")
                 ).dropDuplicates(['campaign_id'])
portfolio_df=portfolio_id_name_mapping.select(F.col("portfolioId").alias("portfolioId"),
                                 F.col("name").alias("portfolio_name")).dropDuplicates(['portfolioId'])
portfolio_mapping=campaign_df.join(portfolio_df,on='portfolioId',how='left')
portfolio_mapping=portfolio_mapping.drop_duplicates()
asin_level_data=portfolio_mapping.join(advertised_asin,how='outer',on="campaign_id")

asin_data=asin_level_data.fillna({"impressions": 0,
                "clicks":0,
                 "ads_spend":0,
                 "UNITS":0,
                 "ads_sales":0,
                 "units":0,
                }).select(
                    F.col("account_name").alias("ACCOUNT NAME"),
                    F.col("asin").alias("ASINS"),
                    F.col("portfolio_name").alias("PORTFOLIOS"),
                    F.col("campaign_name").alias("CAMPAIGNS"),
                    F.col("ad_group_name").alias("AD GRP"),

                    F.col("date").alias("DATE"),
                    F.col("impressions").astype("int").alias("IMPRSN"),
                    F.col("clicks").astype("int").alias("CLICKS"),
                    F.col("ads_spend").astype("int").alias("SPEND"),
                    F.col("ads_sales").astype("int").alias("SALES"),
                    F.col("units").astype("int").alias("UNITS")

                )
                
dummy_asin_data=asin_data.filter(F.col("ACCOUNT NAME")=='awenest')
pattern = r"(?i)\bawenest\b"   
string_cols = [c for c, t in dummy_asin_data.dtypes if t == "string"]

dummy_asin_data = dummy_asin_data.select([
    F.regexp_replace(F.col(c), pattern, "Dummy").alias(c) if c in string_cols else F.col(c)
    for c, _ in dummy_asin_data.dtypes
])


from pyspark.sql.window import Window

# Step 1: Rename AD GRP → AD_GRP for processing
df = dummy_asin_data.withColumnRenamed("AD GRP", "AD_GRP")

w_asin = Window.orderBy("ASINS")
asin_map = (
    df.select("ASINS").distinct()
      .withColumn("ASINS_NEW", F.concat(F.lit("asin"), F.dense_rank().over(w_asin)))
)
df = df.join(asin_map, on="ASINS", how="left").drop("ASINS").withColumnRenamed("ASINS_NEW", "ASINS")

# --- PORTFOLIOS ---
w_portfolio = Window.orderBy("PORTFOLIOS")
portfolio_map = (
    df.select("PORTFOLIOS").distinct()
      .withColumn("PORTFOLIOS_NEW", F.concat(F.lit("portfolio"), F.dense_rank().over(w_portfolio)))
)
df = df.join(portfolio_map, on="PORTFOLIOS", how="left").drop("PORTFOLIOS").withColumnRenamed("PORTFOLIOS_NEW", "PORTFOLIOS")

# --- CAMPAIGNS ---
w_campaign = Window.partitionBy("PORTFOLIOS").orderBy("CAMPAIGNS")
campaign_map = (
    df.select("PORTFOLIOS", "CAMPAIGNS").distinct()
      .withColumn("CAMPAIGNS_NEW", F.concat(F.lit("campaign"), F.dense_rank().over(w_campaign)))
)
df = df.join(campaign_map, on=["PORTFOLIOS", "CAMPAIGNS"], how="left").drop("CAMPAIGNS").withColumnRenamed("CAMPAIGNS_NEW", "CAMPAIGNS")

# --- AD GROUPS ---
w_adgroup = Window.partitionBy("PORTFOLIOS", "CAMPAIGNS").orderBy("AD_GRP")
adgroup_map = (
    df.select("PORTFOLIOS", "CAMPAIGNS", "AD_GRP").distinct()
      .withColumn("AD_GRP_NEW", F.concat(F.lit("adgroup"), F.dense_rank().over(w_adgroup)))
)
df = df.join(adgroup_map, on=["PORTFOLIOS", "CAMPAIGNS", "AD_GRP"], how="left").drop("AD_GRP").withColumnRenamed("AD_GRP_NEW", "AD_GRP")

# Step 2: Multiply numeric metrics
cols_to_multiply = ["IMPRSN", "CLICKS", "SPEND", "SALES", "UNITS"]
for c in cols_to_multiply:
    df = df.withColumn(c, F.col(c).cast("double") * 5)



# Step 4: Rename AD_GRP back
df = df.withColumnRenamed("AD_GRP", "AD GRP")

final_asin_data=asin_data.unionByName(df)

(
    final_asin_data.write
    .format("jdbc")
    .option("url", URL)
    .option("user", USER)
    .option("password", PWD)
    .option("driver", "org.postgresql.Driver")
    .option("dbtable", "public.asin_info")  # destination table
    .mode("overwrite")  # creates table if not exists, replaces if exists
    .save()
)
