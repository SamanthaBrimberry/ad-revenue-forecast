# Databricks notebook source
dbutils.widgets.text("catalog", "sb")
dbutils.widgets.text("schema", "ad_forecast")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")