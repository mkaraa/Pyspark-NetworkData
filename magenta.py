import sys
import os
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import aggregate as agg, avg, count, substring_index, substring
from pyspark import StorageLevel as storageLevel
# import matplotlib.pyplot as plt
import plotly.express as px
# from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# I have no any hadoop setup or bin in my computer, so I need to add as hadoop/bin dir
os.environ['HADOOP_HOME'] = r"C:\Users\METE\Desktop\Magenta\hadoop-3.0.0"
sys.path.append(r"C:\Users\METE\Desktop\Magenta\hadoop-3.0.0\bin")

spark = SparkSession.builder.appName("Magenta").master("local[2]").getOrCreate()

print("1 - retrieves data")
raw_df = spark.read.option("header", True).option("delimiter", ",").csv("./netztest-opendata_hours-048.csv")
raw_df.printSchema()
# raw_df.show()

print(" 2 - Persisted raw dataframe as shown.")
# use random a persistince type || I can use cache 
persisted_raw_df = raw_df.persist(storageLevel.MEMORY_ONLY)  # raw_df.unpersist() -> to remove memory
# persisted_raw_df.show()

print(" 3 - aggregates speed_per_techology and speed_per_device >> agg. cat_technology and download_kbit")
aggregated_technology_device_df = raw_df.filter(
    raw_df["cat_technology"].isNotNull() & raw_df["download_kbit"].isNotNull()).groupby("cat_technology").agg(
    avg("download_kbit").alias("speed"))
# aggregated_technology_device_df.show()

print(" 4 - persist the aggregated data")
# use random a persistince type || I can use cache 
persisted_agg_df = aggregated_technology_device_df.persist(storageLevel.DISK_ONLY)
# persisted_agg_df.show()


print(" 5 - visualize average speed per hour ")
# take only hours from datetime
new_raw_df = raw_df.withColumn("per_hour", substring_index(substring("time_utc", 12, 14), ":", 1))
new_raw_df = new_raw_df.groupby("per_hour").agg(avg("download_kbit").alias("avg_speed"))

# using intellij idea for this project, so I cannot see the bar chart but codes are below
# figure = px.bar(new_raw_df, x='per_hour', y='avg_speed')
# display(figure)

print(" BONUS QUESTIONS")
print(" 6 - the data pipeline run every day")
# Servers generally running on Linux platforms and I use default Crontab (cron job) for scheduling data pipeline running. Belows command in Linux Systems in command line:
# crontab -e -> add or edit cron jobs -> crontab -u 'username' -e >> you can edit other users' job
print(" crontab -e ")
# then, opened an editor (nano,vim or whatever you use as an editor)
# below code runs the magenta.py pipeline scripts as expected and maybe we can collect logs of this operation
print(" 0 0 * * * python magenta.py >> /tmp/logs/dailyScripts.log")

print(" 7 - remove data which are older than 3 weeks")
# First Solution : I can filter the data for time_utc column. I can remove for that
# I assume that cron jobs running everyday
last_3_week_df = raw_df.withColumn("last_3_weeks_dates", substring_index(substring("time_utc", 1, 10), ":", 1))
three_weeks_ago_date = (datetime.now() - timedelta(21)).strftime('%Y-%m-%d')    # get to 3 weeeks ago date as specific format
last_3_week_df = last_3_week_df.filter(last_3_week_df["last_3_weeks_dates"] > three_weeks_ago_date)
# last_3_week_df.show()

print(" 8 - Guarantee idempotency of the data pipeline")    # to prevent fail or remove stale data because of creating duplicates of data
# If I need to prevent duplcation of the data, I can make unique id
unique_df = raw_df.select("open_uuid").distinct()
# unique_df.show()

