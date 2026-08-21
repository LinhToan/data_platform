from datetime import date
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit
import re


spark = SparkSession.builder.getOrCreate()


def is_business_day(day: date) -> bool:
    """
    Overview:
        Check if a given date is a business day.
    Parameters:
        day: The date to check.
    Return:
        True if the date is a business day, False otherwise.
    """

    calendar_table = spark.sql(f"""
        SELECT is_business_day FROM systematic.calendar.calendar
        WHERE calendar_date = '{day.strftime("%Y-%m-%d")}'
    """)

    is_business_day = calendar_table.collect()[0]['is_business_day']

    return True if is_business_day == 1 else False


def get_business_days(day: date) -> list:
    """Get all dates between the given date and the previous business day. """

    calendar_table = spark.sql(f"""
        SELECT 
            DATE_ADD(calendar_date,1) AS current_business_days, 
            next_business_date
        FROM systematic.calendar.calendar
        WHERE next_business_date = '{day.strftime("%Y-%m-%d")}'
    """)

    incoming_file_dates = [row['current_business_days'] for row in calendar_table.select('current_business_days').collect()]
    formatted_dates = [date.strftime('%Y/%m/%d') for date in incoming_file_dates]

    return formatted_dates


def clean_columns(df: DataFrame, day: date) -> DataFrame:
    """
    Overview:
        Cleans column names because Unity Catalog does not like special characters.
    Parameters:
        df: Dataframe to be cleaned.
    Return:
        final_df: Cleaned dataframe with all columns converted to string.
    """

    new_columns = [re.sub(r'[^a-zA-Z0-9_]', '_', c).lower() for c in df.columns]

    cleaned_df = df.toDF(*new_columns).withColumn('utc_timestamp', lit(day))

    return cleaned_df