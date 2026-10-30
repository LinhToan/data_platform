from datetime import date
from pyspark.sql.functions import col
from pyspark.sql.types import StructType
from pyspark.sql import DataFrame, SparkSession
from tests.qa_checks import test_null_columns


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

    azure_path = "/mnt/emprisedatastorage/yardi"

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