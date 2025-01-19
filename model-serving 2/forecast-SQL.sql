-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Custom endpoint
-- MAGIC We can make predictions on our data by interacting with the model via `ai_query()` and pass our model endpoint through as a parameter.

-- COMMAND ----------

SELECT date, device_type,
       ai_query(
           'ad-forecast-endpoint', 
           struct(*),
           returnType => 'FLOAT'
       ) AS predicted_revenue
FROM sb.ad_forecast.forecast_features

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Foundation Model API
-- MAGIC Databricks manages the serving infrastructure for over 30 models. We will be interacting with the Foundation Model Endpoint for the Chronos model from [Hugging Face](https://huggingface.co/amazon/chronos-t5-large).

-- COMMAND ----------

SELECT date, device_type,
       ai_query(
           'chronos-t5-large', 
           struct(*),
           returnType => 'FLOAT'
       ) AS predicted_revenue
FROM sb.ad_forecast.forecast_features

-- COMMAND ----------

