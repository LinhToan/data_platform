# Databricks notebook source
from datetime import date
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col
from pyspark.sql import DataFrame

# COMMAND ----------

transactions_schema = StructType(fields=[
    StructField('RecordType', StringType(), False),
    StructField('UserToken', StringType(), False),
    StructField('UAI', StringType(), True),
    StructField('CardToken', StringType(), False),
    StructField('ActingUserToken', StringType(), False),
    StructField('ProgramId', StringType(), True),
    StructField('TransactionToken', StringType(), False),
    StructField('OriginalTransactionToken', StringType(), False),
    StructField('TransDate', StringType(), True),
    StructField('TransTime', StringType(), True),
    StructField('ImpactTimestamp', StringType(), True),
    StructField('ImpactDate', StringType(), True),
    StructField('ImpactTime', StringType(), True),
    StructField('PostedDate', StringType(), True),
    StructField('PostedTime', StringType(), True),
    StructField('SettlementDate', StringType(), True),
    StructField('Network', StringType(), True),
    StructField('Subnetwork', StringType(), True),
    StructField('TransactionCurrencyCode', StringType(), True),
    StructField('TransAmount', StringType(), True),
    StructField('TranType', StringType(), True),
    StructField('TransCode', StringType(), True),
    StructField('MerchantName', StringType(), True),
    StructField('MerchantCategoryCode', StringType(), True),
    StructField('MerchantState', StringType(), True),
    StructField('MerchantCity', StringType(), True),
    StructField('MerchantStreet', StringType(), True),
    StructField('MerchantZip', StringType(), True),
    StructField('MerchantCountry', StringType(), True),
    StructField('MerchantDivisionId', StringType(), True),
    StructField('StoreNo', StringType(), True),
    StructField('MerchantId', StringType(), True),
    StructField('PosEntryCode', StringType(), True),
    StructField('TransactionAuthType', StringType(), True),
    StructField('IssuerFeeAmount', StringType(), True),
    StructField('InterchangeFeeAmount', StringType(), True),
    StructField('CrossBorderFeeAmount', StringType(), True),
    StructField('AcquirerFeeAmount', StringType(), True),
    StructField('PindebitAssocFeeAmount', StringType(), True),
    StructField('SwitchFeeAmount', StringType(), True),
    StructField('CurConvCardholderFeeAmount', StringType(), True),
    StructField('CurConvIssuerFeeAmount', StringType(), True),
    StructField('TransactionNotes', StringType(), True),
    StructField('ResponseCode', StringType(), True),
    StructField('ReferenceTransactionToken', StringType(), True),
    StructField('ReferenceTransactionDate', StringType(), True),
    StructField('MessageType', StringType(), True),
    StructField('TransentryToken', StringType(), True),
    StructField('NetworkReferenceId', StringType(), True),
    StructField('RetrievalReferenceNumber', StringType(), True),
    StructField('STAN', StringType(), True),
    StructField('ApprovalNumber', StringType(), True),
    StructField('Pan', StringType(), True),
    StructField('IsTestUser', StringType(), True)
])

# COMMAND ----------

start_date = date.today()
end_date = date.today()



# COMMAND ----------

def main(dates_list: list[date], file_type: str, schema: StructType) -> DataFrame:
    """
    Overview:
        Read in credit bank files and convert the data to a dataframe.
    Parameters:
        file_type: Type of credit bank file.
        schema: Schema for the specific credit bank file.
        day: Date of the file.
    Return:
        df (pyspark.sql.dataframe): Dataframe of the credit bank file.
    """
    
    azure_path = "/mnt/storage_account/blob_container/source_system"

    paths = [
        f"{azure_path}/{file_type}/{run_date}/{file_type[3:]}_1_5_0_*"
        for run_date in dates_list
    ]

    df = spark.read.option("skipRows", 1).csv(paths, sep=",", schema=schema) \
        .where(col('RecordType') == 'D')

    # Run QA tests before returning df

    return df

# COMMAND ----------

biz_days = get_business_days(start_date, end_date)
for day in biz_days:
    dates_list = get_weekends(day) # dates_list[-1] == day

    df = ingest_fn(dates_list) # QA tests should be done inside ingest_fn
    cleaned_df = clean_columns(df, day)

    # Pretend I'm doing try-except to log errors
    self.unity_writer.write(cleaned_df, day)
    self.azure_writer.write(cleaned_df, day)

# COMMAND ----------

    start_date = date.today()
    end_date = date.today()

    for cbf in mq_files:
        file_type = cbf['file_type']
        schema = cbf['schema']

        unity_config = UnityConfig(
            unity_path="catalog.schema",
            table_name="table",
            mode=WriteUnityMode.DAILY_SNAPSHOT
        )

        azure_config = AzureConfig(
            azure_path="/mnt/storage_account/blob_container/source_system", 
            file_name=file_type, 
            mode=AzureWriteMode.PARQUET
        )

        unity_writer = UnityWriter(unity_config)
        azure_writer = AzureWriter(azure_config)
        pipeline = IngestionPipeline(unity_writer=unity_writer, azure_writer=azure_writer)

        ingest_fn = partial(main, file_type=file_type, schema=schema)
        pipeline.run(start_date=start_date, end_date=end_date, ingest_fn=ingest_fn)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM catalog.schema.table
# MAGIC ORDER BY utc_timestamp DESC

# COMMAND ----------

