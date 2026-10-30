from datetime import date
from pyspark.sql.functions import col
from pyspark.sql.types import StructType
from pyspark.sql import DataFrame, SparkSession


def main(dates_list: list[date], file_type: str, schema: StructType) -> DataFrame:
    """
    Overview:
        Read in raw data files and convert the data to a dataframe.
    Parameters:
        file_type: Type of raw data file.
        schema: Schema for the specific data file.
        day: Date of the file.
    Return:
        df (pyspark.sql.dataframe): Dataframe of the data file.
    """

    spark = SparkSession.builder.getOrCreate()
    
    azure_path = "/mnt/storage_account/storage_container/directory"

    paths = [
        f"{azure_path}/{file_type}/{run_date}/{file_type[3:]}_1_5_0_*"
        for run_date in dates_list
    ]

    df = spark.read.option("skipRows", 1).csv(paths, sep=",", schema=schema) \
        .where(col('RecordType') == 'D')

    # Run QA tests before returning df

    return df