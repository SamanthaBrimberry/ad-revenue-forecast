# Databricks notebook source
from pyspark.sql.functions import *
import dlt

# COMMAND ----------

@dlt.table(name="time_features")

def get_time_features():
  data = spark.read.format('deltashare').table("`ad-forecast-delta-share`.ad_forecast.ad_log").select('timestamp')
  data = data.distinct()

  def get_season(month):
    if month in [12, 1, 2]:   # Winter
        return 'Winter'
    elif month in [3, 4, 5]:  # Spring
        return 'Spring'
    elif month in [6, 7, 8]:  # Summer
        return 'Summer'
    elif month in [9, 10, 11]: # Autumn
        return 'Fall'
    else:
        return 'None'      # Handle unexpected cases

  get_season_udf = udf(lambda x: get_season(x.month) if x is not None else None, StringType())

  data = data.withColumn('dateFormat', date_format('timestamp','yyyy-MM-dd'))
  data = data.withColumn('year', year(col('timestamp')))
  data = data.withColumn('month', month(col('timestamp')))
  data = data.withColumn('day', dayofmonth(col('timestamp')))
  data = data.withColumn('dayofweek', dayofweek(col('timestamp')))
  data = data.withColumn('weekofyear', weekofyear(col('timestamp')))
  data = data.withColumn('season', get_season_udf(col('timestamp')))

  return data

# COMMAND ----------

@dlt.table(name="forecast_features")
# declare any data quality expectations
@dlt.expect("valid_date", "date > '2022-01-01'")
# Note: expectations are more flexible and powerful than Delta Constraints... you decide how bad records are handled...
# ... track, drop, fail

def forecast_features():
  ad_logs = spark.read.format('deltashare').table("`ad-forecast-delta-share`.ad_forecast.ad_log").drop('NetValue')
  ad_logs = ad_logs.withColumn('date', date_format(col('timestamp'), 'yyyy-MM-dd'))

  ir = spark.read.format('deltashare').table("`ad-forecast-delta-share`.ad_forecast.ir_df")
  qoe = spark.read.format('deltashare').table("`ad-forecast-delta-share`.ad_forecast.qoe_df")

  time_df = dlt.read("time_features")

  df = ad_logs.join(ir, "user_id", "left")
  df = df.join(qoe, "user_id", "left")
  df = df.withColumn('date', date_format(col('date'), 'yyyy-MM-dd'))

  df = df.groupby('date','device_type').agg(
    sum('net_value').alias('revenue'),
    countDistinct('ad_impression_id').alias('impressions'))
  
  df = df.join(time_df, time_df.dateFormat == df.date, "left")
  df = df.drop('timestamp','dateFormat')
  df = df.dropDuplicates()

  return df