# Overview
This project utilizes [Amazon's ChronoBolt](https://huggingface.co/amazon/chronos-bolt-small) foundation time series model to predict advertising revenue.

## Features Included
#### Time Series Forecasting
- Utilizes Amazon's ChronoBolt foundation model
- Predicts ad revenue for the next 12 months
- Supports multiple model sizes (tiny to base)
- MLflow integration for experiment tracking and model versioning
#### DLT Feature Pipeline
- Declarative syntax allows easy definition of complex feature engineering workflows
- Only validated features are used in training and inference
#### Model Serving Infrastructure
- Scalable model deployment endpoints
- Real-time prediction capabilities
- Low-latency inference
#### Query Interface
- Access to predictive models using SQL

## Get Started
Recommended to use a single-node cluster with multiple GPU instances such as g4dn.12xlarge [T4] on AWS or Standard_NC64as_T4_v3 on Azure.

To get started run the [RUNME](./RUNME.py) file!
