# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Sample code below

# COMMAND ----------

from data_eng_sdk import UnityWriter, UnityReader, FeatureEngineer
from datetime import date, timedelta
import pyspark.sql.functions as F

max_date = date.today()
min_date = max_date - timedelta(days=730)

ml_path = 'machine_learning.business_prospecting'
h360_path = 'source.h360'
write_path = 'laboratory.linh'

ml_reader = UnityReader(ml_path)
h360_reader = UnityReader(h360_path)
writer = UnityWriter(write_path)
monthly_dda = FeatureEngineer()

eligible_accounts = ml_reader.read_table(table_name='inference_eligible_accounts', min_date=max_date, max_date=max_date) \
    .withColumn('date_key', F.col('date'))

dda = h360_reader.read_table(table_name='v_ods_ddahis', min_date=min_date, max_date=max_date) \
    .select('account_key', 'transaction_amount', 'transaction_date', 'utc_timestamp')

joined_df = eligible_accounts.join(dda.drop('utc_timestamp'), on='account_key', how='inner') \
    .select('customer_key', 'date_key', 'transaction_amount', 'transaction_date', 'utc_timestamp') \
    .withColumn('transaction_date', F.to_date('transaction_date'))

groupby_cols = ['customer_key', 'date_key', 'transaction_date', 'utc_timestamp']
monthly_df = monthly_dda.aggregate_monthly(joined_df, groupby_cols=groupby_cols, agg_col='transaction_amount', date_col='transaction_date', max_months_ago=24)

writer.write_table(df=monthly_df, table_name='monthly_dda_transactions', mode='overwrite', day=None, merge_cols=None)

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from data_eng_sdk import FeatureEngineer

FeatureEngineer.help()

# COMMAND ----------

