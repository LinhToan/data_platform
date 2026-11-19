# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Sample code below

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from data_eng_sdk.pipelines import DeliverySchedule
from data_eng_sdk.io import WriteUnityMode, UnityConfig

delivery_schedule = DeliverySchedule.BUSINESS_DAYS

azure_path = "/mnt/emprisedatastorage/yardi"

file_name = 'yardi_pha'

unity_config = UnityConfig(
    unity_path="laboratory.linh",
    table_name="pha",
    mode=WriteUnityMode.DAILY_SNAPSHOT
)

# COMMAND ----------

from datetime import date
from pyspark.sql import DataFrame, SparkSession
from data_eng_sdk.utils import is_business_day

def main(dates_list: list[date]) -> DataFrame:
    """
    Overview:
        Reads in the Yardi PHA CSV file and cleans the column names, such as:
        camel case to snake case, replace special characters with underscore, and convert to lower case.
    Parameters:
        start_date: Date to start reading in the CSV files.
        end_date: Date to end reading in the CSV files.
    Return:
        cleaned_df: Cleaned dataframe with all columns cleaned.
    """

    spark = SparkSession.builder.getOrCreate()

    # Example: /mnt/emprisedatastorage/yardi/2026/10/30/yardi_pha_20261031.csv
    paths = [
        f"{azure_path}/{run_date}/{file_name}_{run_date.replace("/", "")}.csv"
        for run_date in dates_list
    ]

    df = spark.read.csv(
        paths, 
        header=True,
        quote='"',        
        escape='"',       
        multiLine=True,   
        sep=','
    )

    return df

# COMMAND ----------

from datetime import date
from data_eng_sdk.io import UnityWriter
from data_eng_sdk.pipelines import IngestionPipeline

if __name__=="__main__":

    unity_writer = UnityWriter(unity_config)
    pipeline = IngestionPipeline(delivery_schedule=delivery_schedule, unity_writer=unity_writer)

    start_date = date.today()
    end_date = date.today()
    pipeline.run(start_date=start_date, end_date=end_date, ingest_fn=main)

# COMMAND ----------

from data_eng_sdk.pipelines import IngestionPipeline

IngestionPipeline.help()

# COMMAND ----------

