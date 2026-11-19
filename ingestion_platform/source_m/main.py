from datetime import date
from pyspark.sql.functions import col
from pyspark.sql.types import StructType
from pyspark.sql import DataFrame, SparkSession


def main(dates_list: list[date], file_type: str, schema: StructType) -> DataFrame:
    """
    Overview:
        Read in Marqeta credit bank files and convert the data to a dataframe.
    Parameters:
        file_type: Type of Marqeta credit bank file.
        schema: Schema for the specific credit bank file.
        day: Date of the file.
    Return:
        df (pyspark.sql.dataframe): Dataframe of the credit bank file.
    """

    spark = SparkSession.builder.getOrCreate()
    
    azure_path = "/mnt/storage_account/blob_container/source_system"

    paths = [
        f"{azure_path}/{file_type}/{run_date}/MQ_{file_type[3:]}_1_5_0_*"
        for run_date in dates_list
    ]

    df = spark.read.csv(paths, sep=",", schema=schema).where(col('RecordType') == 'D')

    # Run QA tests before returning df

    return df