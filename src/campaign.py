from pyspark.sql import SparkSession
import os
from pyspark.sql.functions import broadcast

spark = SparkSession.builder \
    .appName("BigCSVWrite") \
    .config("spark.driver.memory", "5g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "20") \
    .getOrCreate()

from pyspark.sql import SparkSession
from pyspark.sql import DataFrame  # or: from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as F, types as T
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

# Should not raise:
spark.sparkContext._jvm.java.lang.Class.forName("org.postgresql.Driver")
ip='172.31.19.202'
db='amazon_sp_api'
password='Techblooprint123'
user="blooprint"

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

from pyspark.sql import functions as F

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
    F.col("adType").alias("ad_type")
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
    "campaign_id","ads_type"
)
)

camp_strategy=(
    keyword_df
    .groupBy("campaignId")
    .agg(
        F.sum(F.when(F.col("strategy") == "GENERIC", 1).otherwise(0)).alias("generic_count"),
        F.sum(F.when(F.col("strategy") == "BA", 1).otherwise(0)).alias("ba_count"),
        F.sum(F.when(F.col("strategy") == "BD", 1).otherwise(0)).alias("bd_count"),
        F.count("*").alias("total_count")
    )
    .withColumn(
        "strategy_camp",
        F.when(F.col("generic_count") > 0, F.lit("GENERIC"))
         .when((F.col("ba_count") == F.col("total_count")), F.lit("BA"))
         .when((F.col("bd_count") == F.col("total_count")), F.lit("BD"))
         .otherwise(F.lit("Not Applicable"))
    ).dropDuplicates()
    .select("campaignId", "strategy_camp").withColumnRenamed("campaignId","campaign_id")
)
ads_df = ads_df.dropDuplicates(["campaign_id"])

target_df = target_df.dropDuplicates(["campaign_id"])

camp_strategy = camp_strategy.dropDuplicates(["campaign_id"])

portfolio_mapping = portfolio_mapping.dropDuplicates(["campaign_id"])

bid_df = bid_df.dropDuplicates(["keyword_id"])
campaign_info = (
    target_df
    .join(ads_df, on="campaign_id", how="outer")
    .withColumnRenamed("target_group", "targeting_type")
    .join(camp_strategy, on="campaign_id", how="outer")
    .withColumn(
        "strategy_camp",
        F.when(F.col("strategy_camp").isNull(), F.lit("Not Applicable"))
         .otherwise(F.col("strategy_camp"))
    )
)


keyword_df=keyword_df.join(
    F.broadcast(bid_df),
    on="keyword_id",
    how="left"
)


portfolio_info=portfolio_mapping.select("portfolio_name","campaign_id",).join(campaign_info ,on ="campaign_id",how="left").withColumnRenamed("campaign_id", "campaignId")
final_df=keyword_df.join(portfolio_info,on="campaignId",how="left")
(
    final_df.write
    .format("jdbc")
    .option("url", URL)
    .option("user", USER)
    .option("password", PWD)
    .option("driver", "org.postgresql.Driver")
    .option("dbtable", "public.campaign_info")  # destination table
    .mode("overwrite")  # creates table if not exists, replaces if exists
    .save()
)
