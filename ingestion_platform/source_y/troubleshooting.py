# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Overview
# MAGIC This notebook gets reads in the source data file in CSV format from and updates the respective delta table in Unity Catalog. The files arrive every business day at 9:00 AM CST, and the corresponding GoAnywhere job runs accordingly. 
# MAGIC
# MAGIC ### Frequency
# MAGIC Business days at 9:15 AM CST.
# MAGIC
# MAGIC ### Downstream
# MAGIC - TBD
# MAGIC
# MAGIC ### Troubleshooting
# MAGIC In the event of a failure or partial run, this notebook can be re-run without issue because the process is idempotent. That is, the `daily_snapshot` mode in our UnityWriter's write method overwrites the given day's data or inserts new data if the day's data does not exist in the table. This is similar to a SQL MERGE statement, except it updates data on a partition level instead of a row level and the `replaceWhere` option that UnityWriter utlizes is atomic.

# COMMAND ----------

# from data_eng_sdk.write import UnityWriter, UnityConfig
# from data_eng_sdk.utils import is_business_day, get_business_days, get_weekends, clean_columns
from tests.qa_checks import test_null_columns
from pyspark.sql import DataFrame
from datetime import date
import re

# COMMAND ----------

start_date = date.today()
end_date = date.today()

date_list = get_business_days(start_date, end_date)

file_name = "yardi_pha"

paths = [f"/mnt/storage_account/blob_container/{run_date:%Y/%m/%d}/{file_name}_{run_date.strftime("%Y%m%d")}.csv" for run_date in date_list]

# COMMAND ----------

df = spark.read.csv(
    paths[-1], 
    header=True,
    quote='"',        
    escape='"',       
    multiLine=True,   
    sep=','
)

# COMMAND ----------

qa_columns = df.columns
qa_columns.remove('address_line_2')

test_null_columns(df, qa_columns)

# COMMAND ----------

unity_config = UnityConfig(
    unity_path="catalog.schema",
    table_name="table",
    mode=WriteUnityMode.DAILY_SNAPSHOT
)

unity_writer = UnityWriter(unity_config)

# COMMAND ----------

for run_date in date_list:
    cleaned_df = clean_columns(df, run_date)
    unity_writer.write(cleaned_df, run_date)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM catalog.schema.table
# MAGIC ORDER BY file_date DESC

# COMMAND ----------

