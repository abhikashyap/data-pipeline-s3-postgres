
ip='172.31.19.202'
db='amazon_sp_api'
password='Techblooprint123'
user="blooprint"
from pyspark.sql import SparkSession
import os
spark = (
    SparkSession.builder
    .appName("BigCSVWrite")
    .config("spark.driver.memory", "8g")
    .config("spark.executor.memory", "8g")
    .config("spark.executor.memoryOverhead", "2g")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.sql.adaptive.skewJoin.enabled", "true")
    .config("spark.sql.shuffle.partitions", "400")
    .config("spark.default.parallelism", "400")
    .config("spark.memory.fraction", "0.5")
    .config("spark.memory.storageFraction", "0.3")
    .config("spark.memory.offHeap.enabled", "true")
    .config("spark.memory.offHeap.size", "2g")
    .getOrCreate()
)

from pyspark.sql import DataFrame  # or: from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as F, types as T
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
campaign_query = "(SELECT * FROM public.campaigns) as c"
campaigns = (spark.read.format("jdbc")
      .option("url", URL)
      .option("user", USER)
      .option("password", PWD)
      .option("driver", "org.postgresql.Driver")
      .option("dbtable", campaign_query)
      .load())
bid_query = "(SELECT * FROM public.bid_values) as b"
bid_df = (spark.read.format("jdbc")
      .option("url", URL)
      .option("user", USER)
      .option("password", PWD)
      .option("driver", "org.postgresql.Driver")
      .option("dbtable", bid_query)
      .load())
sb_kw_query = "(SELECT * FROM public.sponsored_brands_search_term) as sb"
sb_kw = (spark.read.format("jdbc")
      .option("url", URL)
      .option("user", USER)
      .option("password", PWD)
      .option("driver", "org.postgresql.Driver")
      .option("dbtable", sb_kw_query)
      .load())
sp_kw_query = "(SELECT * FROM public.sponsored_products_search_term) as sp"
sp_kw = (spark.read.format("jdbc")
      .option("url", URL)
      .option("user", USER)
      .option("password", PWD)
      .option("driver", "org.postgresql.Driver")
      .option("dbtable", sp_kw_query)
      .load())
sd_kw_query = "(SELECT * FROM public.sponsored_display_targeting) as sd"
sd_kw = (spark.read.format("jdbc")
      .option("url", URL)
      .option("user", USER)
      .option("password", PWD)
      .option("driver", "org.postgresql.Driver")
      .option("dbtable", sd_kw_query)
      .load())

targets_query = "(SELECT * FROM public.targets ) as t"
targets = (spark.read.format("jdbc")
      .option("url", URL)
      .option("user", USER)
      .option("password", PWD)
      .option("driver", "org.postgresql.Driver")
      .option("dbtable", targets_query)
      .load())
ads_query = "(SELECT * FROM public.ads ) as a"
ads_df = (spark.read.format("jdbc")
      .option("url", URL)
      .option("user", USER)
      .option("password", PWD)
      .option("driver", "org.postgresql.Driver")
      .option("dbtable", ads_query)
      .load())

campaign_df=campaigns.select(F.col("campaignId").alias("campaign_id"),
                 F.col("portfolioId").alias("portfolioId1"),
                 F.col("name").alias("campaign_name"),
                 ).dropDuplicates(['campaign_id'])

portfolio_df=portfolio_id_name_mapping.select(F.col("portfolioId").alias("portfolioId"),
                                 F.col("name").alias("portfolio_name")).dropDuplicates(['portfolioId'])
portfolio_mapping=campaign_df.join(portfolio_df,on=campaign_df['portfolioId1']==portfolio_df['portfolioId'],how='left')
sb_search_term=sb_kw.select(F.col("account_name"),
             F.split(F.col("campaignId").cast("string"),"\\.").getItem(0).alias("campaignId"),
             F.col("campaignName").alias("campaign_name"),
             F.split(F.col("adGroupId").cast("string"),"\\.").getItem(0).alias("ad_group_id"),
             F.col("adGroupName").alias("ad_group_name"),
             F.split(F.col("keywordId").cast("string"),"\\.").getItem(0).alias("keyword_id"),
             F.col("keywordText").alias("keyword"),

             )
sd_search_term=sd_kw.select(F.col("account_name"),
             F.split(F.col("campaignId").cast("string"),"\\.").getItem(0).alias("campaignId"),
             F.col("campaignName").alias("campaign_name"),
             F.split(F.col("adGroupId").cast("string"),"\\.").getItem(0).alias("ad_group_id"),
             F.col("adGroupName").alias("ad_group_name"),
             F.split(F.col("targetingId").cast("string"),"\\.").getItem(0).alias("keyword_id"),
             F.col("targetingText").alias("keyword"),

             )
sp_search_term=sp_kw.select(F.col("account_name"),
             F.split(F.col("campaignId").cast("string"),"\\.").getItem(0).alias("campaignId"),
             F.col("campaignName").alias("campaign_name"),
             F.split(F.col("adGroupId").cast("string"),"\\.").getItem(0).alias("ad_group_id"),
             F.col("adGroupName").alias("ad_group_name"),
             F.split(F.col("keywordId").cast("string"),"\\.").getItem(0).alias("keyword_id"),
             F.col("targeting").alias("keyword"),
             


             )
keyword_df=sp_search_term.unionByName(sb_search_term).unionByName(sd_search_term)



# In[3]:


keyword_df=keyword_df.dropDuplicates()


# In[6]:


bid_df = bid_df.select("keyword_id", "bid", "updated_date", "account_name").dropDuplicates()  # keep only what you need
keyword_df = keyword_df.select("keyword_id", "account_name", "campaign_name", "keyword","campaignId").dropDuplicates()


# In[7]:


b = bid_df.alias("b")
k = keyword_df.alias("k")

joined_df = (
    b.join(
        k,
        (F.col("b.keyword_id") == F.col("k.keyword_id")) &
        (F.col("b.account_name") == F.col("k.account_name")),
        how="left"
    )
    .select(
        F.col("b.keyword_id").alias("keyword_id"),
        F.col("b.bid"),
        F.col("b.updated_date").alias("bid_tym"),
        F.col("b.account_name").alias("account_name"),
        F.col("k.campaign_name"),
        F.col("k.keyword"),
        F.col("K.campaignId")
    )
)


# In[8]:


joined_df=joined_df.withColumnRenamed("campaignId","new_camp_id")


# In[9]:


portfolio_mapping=portfolio_mapping.drop("portfolioId1","campaign_name").dropDuplicates()


# In[10]:


pm = portfolio_mapping.alias("pm")
jd = joined_df.alias("jd")

result = (
    pm.join(
        jd,
        jd["new_camp_id"] == pm["campaign_id"],
        how="right"
    )
    .drop("new_camp_id")  # remove duplicate key
    .withColumn("bid_tym", F.col("bid_tym").cast(T.DateType()))  # cast properly
)


# In[11]:


result=result.drop("campaign_name")



result=result.select(
    F.col("campaign_id"),
    F.col("portfolio_name").alias("PORTFOLIOS"),
    F.col("bid_tym").alias("DATE"),
    F.col("keyword").alias("KEYWORD"),
    F.col('bid'),
    F.col("keyword_id"),
    F.col("account_name").alias("ACCOUNT NAME")


)


# In[14]:


dummy=result.filter(F.col("KEYWORD")=='awenest')


# In[15]:


from pyspark.sql.window import Window
df=dummy
df = df.repartition(1)
# --- PORTFOLIOS ---
w_portfolio = Window.orderBy("PORTFOLIOS")
portfolio_map = (
    df.select("PORTFOLIOS").distinct()
      .withColumn("PORTFOLIOS_NEW", F.concat(F.lit("portfolio"), F.dense_rank().over(w_portfolio)))
)
df = df.join(portfolio_map, on="PORTFOLIOS", how="left").drop("PORTFOLIOS").withColumnRenamed("PORTFOLIOS_NEW", "PORTFOLIOS")



pattern = r"(?i)\bawenest\b"   
string_cols = [c for c, t in df.dtypes if t == "string"]

df = df.select([
    F.regexp_replace(F.col(c), pattern, "Dummy").alias(c) if c in string_cols else F.col(c)
    for c, _ in df.dtypes
])



out_df=result.unionByName(df)


(
    out_df.write
    .format("jdbc")
    .option("url", URL)
    .option("user", USER)
    .option("password", PWD)
    .option("driver", "org.postgresql.Driver")
    .option("dbtable", "public.bid_info")  # destination table
    .mode("overwrite")  # creates table if not exists, replaces if exists
    .save()
)

