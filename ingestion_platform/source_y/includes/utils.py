from datetime import date
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit, col, to_date, regexp_extract, input_file_name
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
        SELECT is_business_day 
        FROM systematic.calendar.calendar
        WHERE calendar_date = '{day.strftime("%Y-%m-%d")}'
    """)

    is_business_day = calendar_table.collect()[0]['is_business_day']

    return is_business_day == 1


def get_business_days(start_date: date, end_date: date) -> list[date]:
    """Get business dates between the start_date (inclusive) and end_date (inclusive). This will typically be passed into the get_busienss_days function."""

    dates_list = (
        spark.table('systematic.calendar.calendar')
        .where(
            (col('calendar_date') >= start_date)
            & (col('calendar_date')<= end_date)
            & (col('is_business_day') == 1)
        ).select('calendar_date').rdd.flatMap(lambda x: x).collect()
    )

    return dates_list


def get_weekends(day: date) -> list[str]:
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

    def camel_to_snake(text: str) -> str:
        # Insert an underscore before any capital letter followed by a lowercase letter
        s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', text)

        # Insert an underscore between a lowercase letter/digit and a capital letter
        s2 = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()
    
        return re.sub(r'[^a-zA-Z0-9_]', '_', s2)

    new_columns = [camel_to_snake(c) for c in df.columns]
    
    cleaned_df = (
        df.toDF(*new_columns)
        .withColumn('utc_timestamp', lit(day))
        .withColumn('file_name', input_file_name())
        .withColumn(
            "file_date",
            to_date(
                regexp_extract(input_file_name(), r"/(\d{4}/\d{2}/\d{2})/", 1),
                "yyyy/MM/dd"
            )
        )
    )

    return cleaned_df