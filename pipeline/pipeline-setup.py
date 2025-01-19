# Databricks notebook source
import json
import requests

# COMMAND ----------

token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
current_user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
base_url = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()

# get widget values
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# COMMAND ----------

config = {
    "pipeline_type": "WORKSPACE",
    "development": True,
    "continuous": False,
    "channel": "PREVIEW",
    "photon": True,
    "libraries": [
        {
            "notebook": {
                "path": f"/Users/{current_user}/demo-playground/ad-impression/01-pipeline"
            }
        }
    ],

    "configuration" : {
        "ad_forecast_pipeline.catalog": catalog,
        "ad_forecast_pipeline.schema": schema
    },
    
    "name": "ad-forecast",
    "catalog": catalog,
    "schema": schema,
    "serverless": True,
    "data_sampling": False
}

response = requests.post(
    f'{base_url}/api/2.0/pipelines',
    headers={'Authorization': f'Bearer {token}'},
    data=json.dumps(config)
)

display(response.json())