# Databricks notebook source
# MAGIC %run ./setup/widgets

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog ${catalog};
# MAGIC create catalog if not exists ${catalog};
# MAGIC create schema if not exists ${schema};

# COMMAND ----------

# MAGIC %run ./setup/data-setup

# COMMAND ----------

# MAGIC %run ./pipeline/pipeline-setup

# COMMAND ----------

# MAGIC %run ./model-training/forecast-foundation

# COMMAND ----------

# MAGIC %run ./model-serving/create-ml-endpoint
