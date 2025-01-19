# Databricks notebook source
# DBTITLE 1,Data Import and Visualization Libraries Setup
import pandas as pd
import numpy as np
import torch
from chronos import BaseChronosPipeline
from pyspark.sql.functions import *
import matplotlib.pyplot as plt
from matplotlib.dates import DateFormatter
from mlflow.models import infer_signature
import mlflow
from mlflow import MlflowClient

mlflow.set_registry_uri('databricks-uc')

client = MlflowClient()

# COMMAND ----------

# MAGIC %run ../widgets

# COMMAND ----------

# get widget values
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
# training data
df = spark.read.table(f"{catalog}.{schema}.forecast_features")
df = df.groupby('date').agg(sum('revenue').alias('revenue'))
df = df.toPandas()
df = df.loc[:, ['date', 'revenue']]

# COMMAND ----------

# DBTITLE 1,Chronos Model Wrapper for Revenue Prediction
class ChronosModelWrapper(mlflow.pyfunc.PythonModel):
    def load_context(self, context):
        self.pipeline = BaseChronosPipeline.from_pretrained(
            f"amazon/chronos-bolt-small",
            device_map="cuda",
            torch_dtype=torch.bfloat16,
        )

    def predict(self, context, model_input):
        if not hasattr(self, 'pipeline'):
            self.load_context(context)
        
        quantiles, mean = self.pipeline.predict_quantiles(
            context=torch.tensor(model_input["revenue"]),
            prediction_length=12,
            quantile_levels=[0.1, 0.5, 0.9]
        )
        
        # Convert tensor to numpy array
        forecast_np = quantiles.numpy()

        # Reshape the array to 2D (samples x prediction_length)
        forecast_2d = forecast_np.reshape(-1, 12)

        # Create DataFrame
        df_forecast = pd.DataFrame(forecast_2d)

        # Get the last date from the input data
        last_date = pd.to_datetime(model_input['date'].sort_values(ascending=True).iloc[-1])

        # Add column names (assuming monthly data)
        df_forecast.columns = pd.date_range(start=last_date + pd.DateOffset(months=1), periods=12, freq='M')

        # Add sample number as index
        df_forecast.index.name = 'quantile'
        df_forecast.index += 1

        # Convert the median (second row) to a DataFrame
        single_row_df = pd.DataFrame([df_forecast.iloc[1]])

        # Transpose the DataFrame to reset columns to row values
        transposed_df = single_row_df.T.reset_index()
        transposed_df.columns = ['date', 'revenue_predictions']

        return transposed_df

with mlflow.start_run(run_name="Chronos_Bolt_Small_Inference"):
    # Backtest
    actual_values = df["revenue"].iloc[-12:].values  # Last 12 months of actual data
    predictions = ChronosModelWrapper().predict(None, df)["revenue_predictions"].values

    # Calculate metrics
    mae = mean_absolute_error(actual_values, predictions)
    mse = mean_squared_error(actual_values, predictions)
    rmse = np.sqrt(mse)
    mape = np.mean(np.abs((actual_values - predictions) / actual_values)) * 100

    # Log metrics
    mlflow.log_metric("MAE", mae)
    mlflow.log_metric("MSE", mse)
    mlflow.log_metric("RMSE", rmse)
    mlflow.log_metric("MAPE", mape)

    signature = infer_signature(df.head(), ChronosModelWrapper().predict(None, df)["revenue_predictions"])
    model_path = "chronos_model"
    mlflow.pyfunc.save_model(path=model_path, python_model=ChronosModelWrapper())
    mlflow.pyfunc.log_model(artifact_path="chronos_model",
                            python_model=ChronosModelWrapper(),
                            signature=signature,
                            registered_model_name=f"{catalog}.{schema}.ad_revenue_forecast"
                            )

# COMMAND ----------

# set alias
client.set_registered_model_alias(
  name=f"{catalog}.{schema}.ad_revenue_forecast",
  alias="champion",
  version=1
)

# uri
logged_model = f"models:/{catalog}.{schema}.ad_revenue_forecast@champion"

# load model
loaded_model = mlflow.pyfunc.load_model(logged_model)

# COMMAND ----------

# predict
loaded_model.predict(df)