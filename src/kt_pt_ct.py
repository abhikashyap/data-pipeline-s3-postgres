ip='172.31.19.202'
db='amazon_sp_api'
password='Techblooprint123'
user="blooprint"
from pyspark.sql import SparkSession
import os
from clickhouse_driver import Client
import datetime
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
import pygsheets
import os
import pandas
def read_sheet(spreadsheet_id,sheet_name,index=0):
    try:
        index=int(index)
        index=index%12
        service_file_path = os.path.join(os.getcwd(), f"key{index}.json")
        gc = pygsheets.authorize(service_file=service_file_path)
        sh = gc.open_by_key(spreadsheet_id)
        selected_sheet = sh.worksheet_by_title(sheet_name)
        df = selected_sheet.get_as_df()
        # database.rename_columns(df)
        return df
    except Exception as e:
        print(f"jhqwdf",e)
        pass
base_sheet='1_zTZSaaAM0svrV26XoX59ZcTJ6MMcpSNV64UOvRDHeA'
df=read_sheet(base_sheet,'AZ',2)
all_sheet_ids=df['sheet_id'].to_list()
all_target_df=[]
for sheet_id in all_sheet_ids:
    print(sheet_id)
    target_df=read_sheet(sheet_id,'Target ACOS AZ')
    if target_df is not None:
        target_df=target_df[['campaign_name','Target ACOS']].drop_duplicates()
        all_target_df.append(target_df)
import pandas as pd
all_campaign_target=pd.concat(all_target_df)
all_campaign_target.to_csv("all_camp_target.csv",index=False)
# 2. Read CSV file
target_df = spark.read.csv(
    "all_camp_target.csv",
    header=True,      # use first row as column names
    inferSchema=True  # infer data types automatically
)




(
    target_df.write
    .format("jdbc")
    .option("url", URL)
    .option("user", USER)
    .option("password", PWD)
    .option("driver", "org.postgresql.Driver")
    .option("dbtable", "public.target_info")  # destination table
    .mode("overwrite")  # creates table if not exists, replaces if exists
    .save()
)
os.remove('all_camp_target.csv')

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
                 F.col("portfolioId").alias("portfolioId_camp"),
                 F.col("name").alias("campaign_name"),
                 ).dropDuplicates(['campaign_id'])

portfolio_df=portfolio_id_name_mapping.select(F.col("portfolioId").alias("portfolioId"),
                                 F.col("name").alias("portfolio_name")).dropDuplicates(['portfolioId'])



portfolio_mapping=campaign_df.join(portfolio_df,on=campaign_df['portfolioId_camp']==portfolio_df['portfolioId'],how='left')

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



             ).withColumn("kt_type",F.lit("keyword_id"))
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

             ).withColumn("kt_type",F.lit("targeting_id"))
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


             ).withColumn("kt_type",F.lit("targeting_id"))
             
keyword_df=sp_search_term.unionByName(sb_search_term).unionByName(sd_search_term)



keyword_df=keyword_df.groupby("account_name","campaignId","ad_group_id","ad_group_name","kt_type","keyword_id","keyword","date").agg(
F.sum("impressions").alias("impressions"),
F.sum("clicks").alias("clicks"),
F.sum("ad_spend").alias("ad_spend"),
F.sum("ads_sale").alias("ads_sale"),
F.sum("ads_units").alias("ads_units")
)

ads_df=(ads_df.select(
    F.col("account_name").alias("ads_account_name"),
    F.col("campaignId").alias("ads_campaign_id"),
    F.col("adProduct").alias("ad_product"),
    F.col("adType").alias("ad_type"),
    F.col("state").alias("CAMP STATUS"),
    F.col("creative_products")
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
    "ads_account_name","ads_campaign_id","ads_type","CAMP STATUS","creative_products"
)
)

ads_df = ads_df.dropDuplicates(["ads_campaign_id"])

keyword_df=keyword_df.join(
    F.broadcast(ads_df),
    (keyword_df["account_name"] == ads_df["ads_account_name"]) &
    (keyword_df["campaignId"] == ads_df["ads_campaign_id"]),
    how="inner"
).drop("ads_account_name","ads_campaign_id")\
.filter(F.col("CAMP STATUS")=="ENABLED")\
    .drop("CAMP STATUS")

keyword_df=keyword_df.withColumn("month",
    F.date_format(F.col("date"),'MMM')
).withColumn("year",
    F.year(F.col("date"))
)
import datetime

yesterday = datetime.datetime.today() - datetime.timedelta(days = 1)
last_7_days= datetime.datetime.today() - datetime.timedelta(days = 7)
last_14_days= datetime.datetime.today() - datetime.timedelta(days = 14)
last_30_days= datetime.datetime.today() - datetime.timedelta(days = 30)
last_60_days= datetime.datetime.today() - datetime.timedelta(days = 60)
yesterday = yesterday.strftime('%Y-%m-%d')
last_7_days = last_7_days.strftime('%Y-%m-%d')
last_14_days = last_14_days.strftime('%Y-%m-%d')
last_30_days = last_30_days.strftime('%Y-%m-%d')
last_60_days = last_60_days.strftime('%Y-%m-%d')




def calculate_metrics(i, mode=1):
    if mode == 1:
        if i == last_14_days:
            d = 'last_14_days'
        elif i == last_7_days:
            d = 'last_7_days'
        elif i == yesterday:
            d = 'yesterday'
        elif i == last_30_days:
            d = 'last_30_days'
        elif i == last_60_days:
            d = 'last_60_days'

        temp_df = keyword_df.filter(F.col('date') >= i)
    else:
        month = i[0]
        year = i[1]
        d = month

        temp_df = keyword_df.filter(
            (F.col('month') == month) & (F.col('year') == year)
        )

    agg_df = temp_df.groupby("account_name", "campaignId", "ad_group_id", "keyword_id").agg(
        F.round(F.sum("impressions"), 0).alias(f"impressions_{d}"),
        F.round(F.sum("clicks"), 0).alias(f"clicks_{d}"),
        F.round(F.sum("ad_spend"), 0).alias(f"ad_spend_{d}"),
        F.round(F.sum("ads_sale"), 0).alias(f"ads_sale_{d}"),
        F.round(F.sum("ads_units"), 0).alias(f"ads_units_{d}")
    )

    agg_df = (
        agg_df
        .withColumn(
            f"CPC_{d}",
            F.when(F.col(f"clicks_{d}") != 0,
                   F.round(F.col(f"ad_spend_{d}") / F.col(f"clicks_{d}"), 2))
            .otherwise(F.lit(0))
        )
        .withColumn(
            f"CPA_{d}",
            F.when(F.col(f"ads_units_{d}") != 0,
                   F.round(F.col(f"ad_spend_{d}") / F.col(f"ads_units_{d}"), 2))
            .otherwise(F.lit(0))
        )
        .withColumn(
            f"ACOS_{d}",
            F.when(F.col(f"ads_sale_{d}") != 0,
                   F.round((F.col(f"ad_spend_{d}") * 100) / F.col(f"ads_sale_{d}"), 2))
            .otherwise(F.lit(0))
        )
        .withColumn(
            f"CVR_{d}",
            F.when(F.col(f"clicks_{d}") != 0,
                   F.round((F.col(f"ads_units_{d}") * 100) / F.col(f"clicks_{d}"), 2))
            .otherwise(F.lit(0))
        )
        .withColumn(
            f"CTR_{d}",
            F.when(F.col(f"impressions_{d}") != 0,
                   F.round((F.col(f"clicks_{d}") * 100) / F.col(f"impressions_{d}"), 2))
            .otherwise(F.lit(0))
        )
    )

    # ✅ Replace nulls in numeric columns with 0
    agg_df = agg_df.fillna(0)


    return agg_df

current_month = datetime.date.today().month
from functools import reduce

dfs = []
time_cut=[yesterday,last_7_days,last_14_days,last_30_days,last_60_days]
for time_duration in time_cut:
    df=calculate_metrics(time_duration, 1)
    dfs.append(df)


for m in range(current_month ,current_month - 4,-1):
    month_name = datetime.date(1900, m, 1).strftime("%b")

    df = calculate_metrics([month_name, 2025], 2)
    
    dfs.append(df)



if dfs:
    final_df = reduce(
        lambda left, right: left.join(
            right,
            on=["account_name", "campaignId", "ad_group_id", "keyword_id"],
            how="outer"
        ),
        dfs
    )

final_df=final_df.fillna(0)
kw_meta_data=keyword_df.select(
    "account_name","ads_type","campaignId","ad_group_id","ad_group_name","kt_type","keyword_id","keyword","creative_products"
).dropDuplicates()

portfolio_meta_data=kw_meta_data.join(F.broadcast(portfolio_mapping),how='left',on=kw_meta_data['campaignId']==portfolio_mapping['campaign_id'])
campaign_target_query = "(SELECT * FROM public.target_info) AS p"

campaign_target_df = (spark.read.format("jdbc")
      .option("url", URL)
      .option("user", USER)
      .option("password", PWD)
      .option("driver", "org.postgresql.Driver")
      .option("dbtable", campaign_target_query)
      .load())
# Keys to join on
final_df = (
    final_df

    .withColumnRenamed("campaignId", "campaignId_kw")
    .withColumnRenamed("ad_group_id", "ad_group_id_kw")
    .withColumnRenamed("keyword_id", "keyword_id_kw")
)



condition = (
    (final_df['campaignId_kw'] == portfolio_meta_data['campaignId']) &
    (final_df['ad_group_id_kw'] == portfolio_meta_data['ad_group_id']) &
    (final_df['keyword_id_kw'] == portfolio_meta_data['keyword_id'])
)

final_mapped_data = (
    final_df
    .join(
        F.broadcast(portfolio_meta_data),
        on=condition,
        how="left"   # keeps all rows from final_df
    )
)
mapped_acos=(
    final_mapped_data
    .join(
        F.broadcast(campaign_target_df),
        on='campaign_name',   # join on multiple columns
        how="left"      # keep all rows from final_df
    )
)
ads_automation_sheet='14aA8eKlsJqCd8mrNZcZOcnwW5pGICTZKs7DVE-IjO8Q'
df=read_sheet(ads_automation_sheet,'Account_level_setting',2)
df.to_csv("account_setting.csv",index=False)

account_setting=spark.read.csv(
    "account_setting.csv",
    header=True,      # use first row as column names
    inferSchema=True  # infer data types automatically
)
account_setting_joined = mapped_acos.alias("m").join(
    account_setting.alias("a"),
    on="account_name",
    how="left"
)


# Fill null Target ACOS from account_setting.target_acos
mapped_acos_ratio=account_setting_joined.withColumn(
    "Target ACOS",
    F.coalesce(F.col("m.Target ACOS"), F.col("a.target_acos"))
).drop("a.target_acos")
df=mapped_acos_ratio
from pyspark.sql import functions as F, Window
w = Window.partitionBy("portfolio_name","keyword_id").orderBy(F.col("ads_sale_last_60_days").desc())
w_unordered = Window.partitionBy("portfolio_name")

df = df.withColumn("cumulative_sales", F.sum("ads_sale_last_60_days").over(w))

df = df.withColumn("cumulative_sales", F.sum("ads_sale_last_60_days").over(w))

# total sales per portfolio
df = df.withColumn("total_sales_portfolio", F.sum("ads_sale_last_60_days").over(w_unordered))

# cumulative percentage
df = df.withColumn("cumulative_percent", F.col("cumulative_sales") / F.col("total_sales_portfolio"))

df = df.withColumn(
    "priority_index",
    F.when(F.col("ads_sale_last_60_days") == 0, F.lit(-1)) # if sales = 0
        .when(F.col("cumulative_percent") <= 0.8, F.lit(1))   # top 80%
        .otherwise(F.lit(0))                                  # rest
)
df.write \
    .format("jdbc") \
    .option("url", URL) \
    .option("user", USER) \
    .option("password", PWD) \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", "public.kt_pt_ct_report") \
    .mode("overwrite") \
    .save()
