from datetime import date
from pyspark.sql import DataFrame, SparkSession
from tests.qa_checks import test_null_columns
from data_eng_sdk.utils import is_business_day
from config import azure_path, file_name


def main(dates_list: list[date]) -> DataFrame:
    """
    Overview:
        Reads in the CSV file and cleans the column names, such as:
        camel case to snake case, replace special characters with underscore, and convert to lower case.
    Parameters:
        start_date: Date to start reading in the CSV files.
        end_date: Date to end reading in the CSV files.
    Return:
        cleaned_df: Cleaned dataframe with all columns cleaned.
    """

    spark = SparkSession.builder.getOrCreate()

    # Example: /mnt/stoage_account/blob_container/2026/10/30/file_name_20261031.csv
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

    qa_columns = df.columns
    qa_columns.remove('address_line_2')

    test_null_columns(df, qa_columns)

    return df