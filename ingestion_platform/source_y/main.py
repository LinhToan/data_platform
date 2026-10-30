from datetime import date
from pyspark.sql.functions import col
from pyspark.sql.types import StructType
from pyspark.sql import DataFrame, SparkSession
from tests.qa_checks import test_null_columns


def main(dates_list: list[date]) -> DataFrame:
    """
    Overview:
        Reads in the sample CSV file and runs data validation tests.
    Parameters:
        dates_list: List of dates for year/month/day file directory
    Return:
        cleaned_df: Cleaned dataframe with all columns cleaned.
    """

    spark = SparkSession.builder.getOrCreate()

    azure_path = "/mnt/storage_account/storage_container"

    paths = [f"{azure_path}/{run_date}/*" for run_date in dates_list]

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
