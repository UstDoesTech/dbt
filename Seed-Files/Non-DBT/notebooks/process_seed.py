# Databricks notebook source
dbutils.widgets.text("fileName", "", "File Name")
dbutils.widgets.text("tableName", "", "Table Name")
dbutils.widgets.text("schemaName", "Enriched", "Schema Name")
dbutils.widgets.text("writePath", "", "Write Path (optional)")

# Setup variables to hold parameter inputs
fileName = dbutils.widgets.get("fileName")
tableName = dbutils.widgets.get("tableName")
schemaName = dbutils.widgets.get("schemaName")
writePath = dbutils.widgets.get("writePath")

# COMMAND ----------
df = (spark.read.option("header", True).option("delimiter", ",").csv(f'dbfs:/data/{fileName}'))

spark.sql(f"CREATE DATABASE IF NOT EXISTS {schemaName}")

if not writePath:
  df.write.mode("overwrite").saveAsTable(f'{schemaName}.{tableName}')
else:
  df.write.mode("overwrite").option("path", writePath).saveAsTable(f'{schemaName}.{tableName}')

# COMMAND ----------