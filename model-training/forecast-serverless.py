# Databricks notebook source
# DBTITLE 1,Install
!pip install mlflow

# COMMAND ----------

# DBTITLE 1,Import
import mlflow
from mlflow import MlflowClient

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exploratory Data Analysis

# COMMAND ----------

# DBTITLE 1,Forecast Features DataFrame
# Display our feature data we've created in 01-pipeline
df = spark.table('sb.ad_forecast.forecast_features')
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create a forecasting experiment with the UI
# MAGIC Go to your Databricks landing page and click [Experiments](https://e2-demo-field-eng.cloud.databricks.com/ml/experiments?o=1444828305810485) in the sidebar.

# COMMAND ----------

# MAGIC %md
# MAGIC ### View perviously created training experiment in UI
# MAGIC We've already completed an experiment, view [here](https://e2-demo-field-eng.cloud.databricks.com/ml/experiments/1375557022162509?o=1444828305810485&searchFilter=&orderByKey=metrics.%60val_smape%60&orderByAsc=true&startTime=ALL&lifecycleFilter=Active&modelVersionFilter=All+Runs&datasetsFilter=W10%3D).

# COMMAND ----------

# MAGIC %md
# MAGIC ### Batch Inference

# COMMAND ----------

# DBTITLE 1,Predicting with MLflow Model on Spark DataFrame
import mlflow
from pyspark.sql.functions import struct, col
logged_model = 'runs:/109c0ba6cac747eaa56a139b562ab13a/model'

# Load model as a Spark UDF
loaded_model = mlflow.pyfunc.spark_udf(spark, model_uri=logged_model)

df = spark.read.table("sb.ad_forecast.forecast_features").limit(100)

# Predict on a Spark DataFrame.
display(df.withColumn('predictions', loaded_model(struct(*map(col, df.columns)))))

# COMMAND ----------

# DBTITLE 1,Call Forecast Endpoint via SQL
# %sql
# SELECT date, device_type,
#        ai_query(
#            'ad-revenue-forecast', 
#            struct(*),
#            returnType => 'FLOAT'
#        ) AS predicted_revenue
# FROM sb.ad_forecast.forecast_features

# COMMAND ----------

# MAGIC %md
# MAGIC ### AI functions in SQL
# MAGIC we can also interact with our model serving endpoint via the [SQL Editor](https://e2-demo-field-eng.cloud.databricks.com/sql/editor/9fa19f42-c6b9-4f9e-b1df-44b681e93ea6?o=1444828305810485)