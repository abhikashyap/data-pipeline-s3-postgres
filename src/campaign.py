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
campaign_query = "(SELECT * FROM public.campaigns) as c"
campaigns = (spark.read.format("jdbc")
      .option("url", URL)
      .option("user", USER)
      .option("password", PWD)
      .option("driver", "org.postgresql.Driver")
      .option("dbtable", campaign_query)
      .load())
competitor_query = "(SELECT * FROM public.competitor_brand_data) as cb"
competitors = (spark.read.format("jdbc")
      .option("url", URL)
      .option("user", USER)
      .option("password", PWD)
      .option("driver", "org.postgresql.Driver")
      .option("dbtable", competitor_query)
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
bid_query="""(SELECT unnamed_subquery.keyword_id,
            unnamed_subquery.bid,
            unnamed_subquery.bid_tym
           FROM ( SELECT bid_values.keyword_id,
                    bid_values.bid,
                    bid_values.updated_date::date AS bid_tym,
                    rank() OVER (PARTITION BY bid_values.keyword_id ORDER BY bid_values.updated_date DESC) AS rank
                   FROM bid_values) unnamed_subquery
          WHERE unnamed_subquery.rank = 1 ) bid"""
bid_df = (spark.read.format("jdbc")
      .option("url", URL)
      .option("user", USER)
      .option("password", PWD)
      .option("driver", "org.postgresql.Driver")
      .option("dbtable", bid_query)
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
                 F.col("portfolioId").alias("portfolioId"),
                 F.col("name").alias("campaign_name"),
                 ).dropDuplicates(['campaign_id'])

campaign_df=campaigns.select(F.col("campaignId").alias("campaign_id"),
                 F.col("portfolioId").alias("portfolioId"),
                 F.col("name").alias("campaign_name"),
                 ).dropDuplicates(['campaign_id'])
portfolio_df=portfolio_id_name_mapping.select(F.col("portfolioId").alias("portfolioId"),
                                 F.col("name").alias("portfolio_name")).dropDuplicates(['portfolioId'])
portfolio_mapping=campaign_df.join(portfolio_df,on=campaign_df['portfolioId']==portfolio_df['portfolioId'],how='left')
sb_search_term=sb_kw.select(F.col("account_name"),
             F.split(F.col("campaignId").cast("string"),"\\.").getItem(0).alias("campaignId"),
             F.col("campaignName").alias("campaign_name"),
             F.split(F.col("adGroupId").cast("string"),"\\.").getItem(0).alias("ad_group_id"),
             F.col("adGroupName").alias("ad_group_name"),
             F.split(F.col("keywordId").cast("string"),"\\.").getItem(0).alias("keyword_id"),
             F.col("keywordText").alias("keyword"),
             F.col("searchTerm").alias("search_term"),
             F.col("date").cast("date").alias("date"),
             F.col("impressions").alias("impressions"),
             F.col("clicks").alias("clicks"),
             F.col("cost").alias("ad_spend"),
             F.col("sales").alias("ads_sale"),
             F.col("unitsSold").alias("ads_units"),
             F.col("matchType").alias("match_type"),


             )
sd_search_term=sd_kw.select(F.col("account_name"),
             F.split(F.col("campaignId").cast("string"),"\\.").getItem(0).alias("campaignId"),
             F.col("campaignName").alias("campaign_name"),
             F.split(F.col("adGroupId").cast("string"),"\\.").getItem(0).alias("ad_group_id"),
             F.col("adGroupName").alias("ad_group_name"),
             F.split(F.col("targetingId").cast("string"),"\\.").getItem(0).alias("keyword_id"),
             F.col("targetingText").alias("keyword"),
             F.lit("not_applicable").alias("search_term"),
             F.col("date").cast("date").alias("date"),
             F.col("impressions").alias("impressions"),
             F.col("clicks").alias("clicks"),
             F.col("cost").alias("ad_spend"),
             F.col("sales").alias("ads_sale"),
             F.col("unitsSold").alias("ads_units"),
             F.lit("DSP TGT").alias("match_type"),

             )
sp_search_term=sp_kw.select(F.col("account_name"),
             F.split(F.col("campaignId").cast("string"),"\\.").getItem(0).alias("campaignId"),
             F.col("campaignName").alias("campaign_name"),
             F.split(F.col("adGroupId").cast("string"),"\\.").getItem(0).alias("ad_group_id"),
             F.col("adGroupName").alias("ad_group_name"),
             F.split(F.col("keywordId").cast("string"),"\\.").getItem(0).alias("keyword_id"),
             F.col("targeting").alias("keyword"),
             F.col("searchTerm").alias("search_term"),
             F.col("date").cast("date").alias("date"),
             F.col("impressions").alias("impressions"),
             F.col("clicks").alias("clicks"),
             F.col("cost").alias("ad_spend"),
             F.col("sales30d").alias("ads_sale"),
             F.col("unitsSoldSameSku1d").alias("ads_units"),
             F.col("matchType").alias("match_type"),


             )
keyword_df=sp_search_term.unionByName(sb_search_term).unionByName(sd_search_term)
keyword_df=keyword_df.withColumn(
    "new_match_type",
    F.when(
        (F.col("match_type") == "TARGETING_EXPRESSION") & F.col("keyword").like("%asin=%"),
        F.lit("PT EXACT")
    ).when(
        (F.col("match_type") == "TARGETING_EXPRESSION") & F.col("keyword").like("%category=%"),
        F.lit("PT CAT")
    ).when(
        (F.col("match_type") == "TARGETING_EXPRESSION") & F.col("keyword").like("%asin-expanded=%"),
        F.lit("PT EXP")
    ).when(
        F.col("match_type") == "TARGETING_EXPRESSION_PREDEFINED",
        F.upper(F.col("keyword"))
    ).otherwise(F.col("match_type"))
)
our_asins_df=competitors.select(
    F.col("our_asins"),
    F.col("account_name")
).where(
    F.col("our_asins").isNotNull()

).dropDuplicates()
keyword_df=keyword_df.withColumn(
    "asin_extracted",
    F.when(
        (F.col("keyword").contains("asin=")) | (F.col("keyword").contains("asin-expanded=")),
        # split on "=" then take the right side
        F.split(F.col("keyword"), "=").getItem(1)   
    ).otherwise(F.lit(None))
)

keyword_df=keyword_df.withColumn("asin_extracted",F.translate(F.col("asin_extracted"),'"',""))
our_asins_df = our_asins_df.withColumnRenamed("account_name", "asin_account_name")

keyword_df = keyword_df.join(
    F.broadcast(our_asins_df),
    (keyword_df["account_name"] == our_asins_df["asin_account_name"]) &
    (keyword_df["asin_extracted"] == our_asins_df["our_asins"]),
    how="left"
)

keyword_df=keyword_df.drop('asin_account_name')


keyword_df.withColumn(
    "isbdasin",
    F.when(F.col("asin_extracted") == F.col("our_asins"), F.lit(True))
     .otherwise(F.lit(False))
)



self_brand_name=competitors.select(
    F.col("our_brand_name"),
    F.col("account_name")
).dropDuplicates()

self_brand_name=self_brand_name.filter(
    (F.col("our_brand_name").isNotNull())|(F.col("our_brand_name")!="")

)


self_brand_name = (
    self_brand_name
    .withColumn(
        "our_brand_name",
        F.regexp_replace("our_brand_name", "Your Everyday Greens", "Everyday")
    )
    .withColumn(
        "our_brand_name",
        F.regexp_replace("our_brand_name", "Beyond Appliances", "Beyond")
    )
)

brand_regex = (
    self_brand_name
      .groupBy("account_name")
      .agg(
          F.concat_ws("|", F.collect_set("our_brand_name")).alias("brand_pattern")
      )
      .withColumn("brand_pattern", F.concat(F.lit("(?i)("), F.col("brand_pattern"), F.lit(")")))
)


keyword_df = (
    keyword_df
    .join(F.broadcast(brand_regex), on="account_name", how="left")
    .withColumn("bd_brand",
        F.expr("keyword rlike brand_pattern")
    )
)

comp_brand_name=competitors.select(
    F.col("competitor_brand_name"),
    F.col("account_name")
).dropDuplicates()

comp_brand_name=comp_brand_name.filter(
    (F.col("competitor_brand_name").isNotNull())|(F.col("competitor_brand_name")!="")
).dropDuplicates()
comp_brand_name_regex=comp_brand_name.groupby("account_name").agg(F.concat_ws("|",F.collect_set("competitor_brand_name")).alias("competitor_brand_name"))

keyword_df=(
    keyword_df
    .join(F.broadcast(comp_brand_name_regex), on="account_name", how="left")
    .withColumn("ba_brand", F.expr("lower(keyword) rlike lower(competitor_brand_name)"))

)
keyword_df=keyword_df.withColumn(
    "strategy",
    F.when(
        (F.col("asin_extracted") == F.col("our_asins")) | F.col("bd_brand"),
        "BD"
    )
    .when(
        F.col("ba_brand") |
        (
            (F.col("asin_extracted").isNotNull()) &
            (
                (F.col("asin_extracted") != F.col("our_asins")) |
                (F.col("our_asins").isNull())
            )
        ),
        "BA"
    )
    .otherwise("GENERIC")
)

keyword_df=keyword_df.drop("asin_extracted","our_asins","brand_pattern",'bd_brand',"ba_brand","competitor_brand_name")


keyword_df=keyword_df.select('account_name',
                             "campaignId",
 'campaign_name',
 'ad_group_id',
 'ad_group_name',
 'keyword_id',
 'keyword',
 'search_term',
 'date',
 'impressions',
 'clicks',
 'ad_spend',
 'ads_sale',
 'ads_units',
 'match_type',
 'new_match_type',
 'strategy')



target_df = (
    targets
    .select(
        F.col("campaignId").alias("campaign_id"),
        F.col("targetType").alias("targeting_type"),
        F.col("targetDetails_event").alias("target_event")
    )
    .withColumn(
        "target_group",
        F.when(F.upper(F.col("targeting_type")) == "PRODUCT", "PT")
         .when(F.upper(F.col("targeting_type")) == "KEYWORD", "KT")
         .when(F.upper(F.col("targeting_type")).isin("AUTO", "THEME"), "AT")
         .when(F.upper(F.col("targeting_type")) == "PRODUCT_CATEGORY", "CT")
         .when(F.upper(F.col("targeting_type")) == "AUDIENCE", "ADT")
         .when(
             (F.upper(F.col("targeting_type")).isin("PRODUCT_CATEGORY_AUDIENCE", "PRODUCT_AUDIENCE")) &
             (F.upper(F.col("target_event")) == "PURCHASES"),
             "PR"
         )
         .when(
             (F.upper(F.col("targeting_type")).isin("PRODUCT_CATEGORY_AUDIENCE", "PRODUCT_AUDIENCE")) &
             (F.upper(F.col("target_event")) == "VIEWS"),
             "VR"
         )
         .otherwise("Not Applicable")
    )
    .dropDuplicates()
    .select("campaign_id","target_group")
)

ads_df=(ads_df.select(
    F.col("campaignId").alias("campaign_id"),
    F.col("adProduct").alias("ad_product"),
    F.col("adType").alias("ad_type"),
    F.col("state").alias("CAMP STATUS")
).dropDuplicates()
.withColumn("ads_type",
    F.when(F.col("ad_product")=="SPONSORED_PRODUCTS","SP")
    .when((F.col("ad_product")=="SPONSORED_BRANDS")&(F.col("ad_type").isin("BRAND_VIDEO","VIDEO")),"SBV")
    .when((F.col("ad_product")=="SPONSORED_BRANDS")&(F.col("ad_type")=="PRODUCT_COLLECTION"),"SBB")
    .when((F.col("ad_product")=="SPONSORED_BRANDS")&(F.col("ad_type")=="STORE_SPOTLIGHT"),"SBS")
    .when((F.col("ad_product")=="SPONSORED_DISPLAY")&(F.col("ad_type")=="IMAGE"),"SDI")
    .when((F.col("ad_product")=="SPONSORED_DISPLAY")&(F.col("ad_type")=="PRODUCT_AD"),"SDP")
    .when((F.col("ad_product")=="SPONSORED_DISPLAY")&(F.col("ad_type")=="VIDEO"),"SDV")
    .otherwise("Not Applicable")          
).select(
    "campaign_id","ads_type","CAMP STATUS"
)
)

ads_df = ads_df.dropDuplicates(["campaign_id"])

target_df = target_df.dropDuplicates(["campaign_id"])


portfolio_mapping = portfolio_mapping.dropDuplicates(["campaign_id"])

bid_df = bid_df.dropDuplicates(["keyword_id"])
campaign_info = (
    target_df
    .join(ads_df, on="campaign_id", how="outer")
    .withColumnRenamed("target_group", "targeting_type")
)


keyword_df=keyword_df.join(
    F.broadcast(bid_df),
    on="keyword_id",
    how="left"
)


portfolio_info=portfolio_mapping.select("portfolio_name","campaign_id",).join(campaign_info ,on ="campaign_id",how="left").withColumnRenamed("campaign_id", "campaignId")
final_df=keyword_df.join(portfolio_info,on="campaignId",how="left")
final_df=(
    final_df
    .withColumn(
        "CPC",
        F.when(F.col("clicks") != 0,
               F.round(F.col("ad_spend") / F.col("clicks"), 0)
        ).otherwise(F.lit(0))
    )
    .withColumn(
        "ROI",
        F.when(F.col("ad_spend") != 0,
               F.round(F.col("ads_sale") / F.col("ad_spend"), 0)
        ).otherwise(F.lit(0))
    )
    .withColumn(
        "ACOS",
        F.when(F.col("ads_sale") != 0,
               F.round((F.col("ad_spend") * 100) / F.col("ads_sale"), 0)
        ).otherwise(F.lit(0))
    )
    .withColumn(
        "CVR",
        F.when(F.col("clicks") != 0,
               F.round((F.col("ads_units") * 100) / F.col("clicks"), 0)
        ).otherwise(F.lit(0))
    )
)
final_df=(
    final_df
    .withColumnRenamed("account_name", "ACCOUNT NAME")
    .withColumnRenamed("date", "DATE")
    .withColumnRenamed("portfolio_name", "PORTFOLIOS")
    .withColumnRenamed("campaign_name", "CAMPAIGNS")
    .withColumnRenamed("ad_group_name", "AD GRP")
    # CAMP STATUS not present in final_df, will need join (skip for now)
    .withColumnRenamed("ads_type", "ADS TYP")
    .withColumnRenamed("strategy", "STRATEGY")
    .withColumn(
        "MATCH TYP",
        F.when(F.lower(F.col("new_match_type")) == "close-match", "CLOSE")
         .when(F.lower(F.col("new_match_type")) == "complements", "COMP")
         .when(F.lower(F.col("new_match_type")) == "substitutes", "SUB")
         .when(F.lower(F.col("new_match_type")) == "loose-match", "LOOSE")
         .otherwise(F.col("new_match_type"))
    )
    .withColumnRenamed("targeting_type", "TARGETING TYP")
    .withColumnRenamed("keyword_id", "keyword_id")
    .withColumnRenamed("keyword", "KEYWORD")
    .withColumnRenamed("search_term", "SEARCH TERM")
    .withColumnRenamed("bid", "bid")
    .withColumnRenamed("bid_tym", "BID TYM")
    .withColumnRenamed("ad_spend", "SPEND")
    .withColumnRenamed("ads_sale", "SALES")
    .withColumnRenamed("ads_units", "UNITS")
    .withColumnRenamed("clicks", "CLICKS")
    .withColumnRenamed("CPC", "CPC")
    .withColumnRenamed("ROI", "ROI")
    .withColumnRenamed("ACOS", "ACOS")
    .withColumnRenamed("CVR", "CVR")
    .withColumnRenamed("impressions", "IMPRSN")
    .withColumnRenamed("campaignId", "campaign_id")
    .withColumnRenamed("ad_group_id", "ad_group_id")
)
final_df=final_df.drop("ne_match_type","match_type")

final_df=final_df.fillna({"CPC": 0,
                "SPEND":0,
                 "SALES":0,
                 "UNITS":0,
                 "CLICKS":0,
                 "CPC":0,
                 "ROI":0,
                 "CVR":0,
                 "IMPRSN":0,
                  "bid":0
                })
dummy_df=final_df.filter(F.col("ACCOUNT NAME") == 'awenest')
from pyspark.sql import functions as F

pattern = r"(?i)\bawenest\b"   
string_cols = [c for c, t in final_df.dtypes if t == "string"]

dummy_df = dummy_df.select([
    F.regexp_replace(F.col(c), pattern, "Dummy").alias(c) if c in string_cols else F.col(c)
    for c, _ in final_df.dtypes
])
from pyspark.sql.window import Window

# Step 1: Rename columns with spaces to safe names
df = dummy_df.withColumnRenamed("AD GRP", "AD_GRP")

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

# --- KEYWORDS ---
w_keyword = Window.partitionBy("PORTFOLIOS", "CAMPAIGNS", "AD_GRP").orderBy("KEYWORD")
keyword_map = (
    df.select("PORTFOLIOS", "CAMPAIGNS", "AD_GRP", "KEYWORD").distinct()
      .withColumn("KEYWORD_NEW", F.concat(F.lit("keyword"), F.dense_rank().over(w_keyword)))
)
df = df.join(keyword_map, on=["PORTFOLIOS", "CAMPAIGNS", "AD_GRP", "KEYWORD"], how="left").drop("KEYWORD").withColumnRenamed("KEYWORD_NEW", "KEYWORD")

# Step 2: Rename AD_GRP back to "AD GRP"
df = df.withColumnRenamed("AD_GRP", "AD GRP")
cols_to_multiply = ["IMPRSN", "CLICKS", "SPEND", "SALES", "UNITS"]

for c in cols_to_multiply:
    df = df.withColumn(c, F.col(c)* 5)
df=(
    df
    .withColumn(
        "CPC",
        F.when(F.col("CLICKS") != 0,
               F.round(F.col("SPEND") / F.col("CLICKS"), 0)
        ).otherwise(F.lit(0))
    )
    .withColumn(
        "ROI",
        F.when(F.col("SPEND") != 0,
               F.round(F.col("SALES") / F.col("SPEND"), 0)
        ).otherwise(F.lit(0))
    )
    .withColumn(
        "ACOS",
        F.when(F.col("SALES") != 0,
               F.round((F.col("SPEND") * 100) / F.col("SALES"), 0)
        ).otherwise(F.lit(0))
    )
    .withColumn(
        "CVR",
        F.when(F.col("CLICKS") != 0,
               F.round((F.col("UNITS") * 100) / F.col("CLICKS"), 0)
        ).otherwise(F.lit(0))
    )
)
out_df=final_df.unionByName(df)


(
    out_df.write
    .format("jdbc")
    .option("url", URL)
    .option("user", USER)
    .option("password", PWD)
    .option("driver", "org.postgresql.Driver")
    .option("dbtable", "public.campaign_info")  # destination table
    .mode("overwrite")  # creates table if not exists, replaces if exists
    .save()
)
