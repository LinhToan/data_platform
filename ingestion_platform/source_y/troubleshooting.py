# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Overview
# MAGIC This notebook gets reads in the sample file in CSV format from and updates the respective delta table in Unity Catalog. The files arrive every business day at 9:00 AM CST. 
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

from data_eng_sdk import UnityWriter, filter_business_days, is_business_day, clean_columns
# from data_eng_sdk.write import UnityWriter
# from data_eng_sdk.utils import is_business_day, get_business_days, get_weekends, clean_columns
from tests.qa_checks import test_null_columns
from pyspark.sql import DataFrame
from datetime import date
import re

# COMMAND ----------

start_date = date.today()
end_date = date.today()

date_list = filter_business_days(start_date, end_date)

paths = [f"/mnt/storage_account/storage_container/{run_date:%Y/%m/%d}/*" for run_date in date_list if is_business_day(run_date)]

# COMMAND ----------

df = spark.read.csv(
    paths, 
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

cleaned_df = clean_columns(df, end_date)

# COMMAND ----------

unity_path = "catalog.schema"
unity_writer = UnityWriter(unity_path)
table_name = 'table'

unity_writer.write(df=cleaned_df, table_name=table_name, mode='daily_snapshot', day=end_date)

# COMMAND ----------

