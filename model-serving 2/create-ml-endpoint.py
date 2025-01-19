# Databricks notebook source
# MAGIC %pip install "databricks-sdk>=0.28.0"
# MAGIC dbutils.library.restartPython()  

# COMMAND ----------

# MAGIC %run ../widgets

# COMMAND ----------

# DBTITLE 1,Initialize MLflow and Databricks Clients
import mlflow
from mlflow.deployments import get_deploy_client
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import EndpointCoreConfigInput, ServedModelInput, ServedModelInputWorkloadSize

client = get_deploy_client("databricks")
mf_client = mlflow.tracking.MlflowClient()
w = WorkspaceClient()

# COMMAND ----------

# DBTITLE 1,Check and Create Ad Revenue Forecast Endpoint
serving_endpoint_name = 'ad-forecast-endpoint'
champion_version = mf_client.get_model_version_by_alias(
    f"{catalog}.{schema}.ad_revenue_forecast", 
    "champion"
)
model_name = champion_version.name
latest_model_version = champion_version.version

existing_endpoint = next(
    (e for e in w.serving_endpoints.list() if e.name == serving_endpoint_name), 
    None
)

if existing_endpoint is None:
  endpoint = client.create_endpoint(
      name=f"{serving_endpoint_name}",
      config={
          "served_entities": [
              {
                  "name": "ad-revenue-forecast",
                  "entity_name": f"{catalog}.{schema}.ad_revenue_forecast",
                  "entity_version": f"{latest_model_version}",
                  "workload_size": "Small",
                  "scale_to_zero_enabled": True
              }
          ],
          "traffic_config": {
              "routes": [
                  {
                      "served_model_name": "ad-revenue-forecast",
                      "traffic_percentage": 100
                  }
              ]
          }
      }
  )

# COMMAND ----------

# DBTITLE 1,Set Up Chronos-T5-Large Serving Endpoint
serving_endpoint_name = 'chronos-t5-large'

existing_endpoint = next(
    (e for e in w.serving_endpoints.list() if e.name == serving_endpoint_name), 
    None
)

if existing_endpoint is None:
  endpoint = client.create_endpoint(
      name=f"{serving_endpoint_name}",
      config={
          "served_entities": [
              {
                  "name": "chronos-t5-large",
                  "entity_name": "system.ai.chronos-t5-large",
                  "entity_version": 1,
                  "workload_size": "Small",
                  "scale_to_zero_enabled": True
              }
          ],
          "traffic_config": {
              "routes": [
                  {
                      "served_model_name": "chronos-t5-large",
                      "traffic_percentage": 100
                  }
              ]
          }
      }
  )