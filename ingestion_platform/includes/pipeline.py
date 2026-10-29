from datetime import date
from pyspark.sql import DataFrame
from collections.abc import Callable
from .write import UnityWriter, AzureWriter
from .utils import is_business_day, get_business_days, get_weekends, clean_columns

class IngestionPipeline:
    """Boilerplate for source data ingestion pipelines."""

    def __init__(self, azure_writer: AzureWriter, unity_writer: UnityWriter) -> None:
        self.azure_writer = azure_writer
        self.unity_writer = unity_writer

    def run(self, start_date: date, end_date: date, ingest_fn: Callable[[date, date], DataFrame]) -> None:
        """
        Overview:
            Check if today is a business day. If not, exit the pipeline.
            Otherwise, loop through the business days between start_date and end_date.
            For each day, run the ingest function, run QA, and write to Azure and Unity.
        Parameters:
            start_date: The start date of the pipeline.
            end_date: The end date of the pipeline.
            ingest_fn: The main function to ingest data for this paricular pipeline.
        Return:
            None.
        """

        if start_date == date.today() and not is_business_day(start_date):
            dbutils.notebook.exit("Today is not a business day. Exiting pipeline.")
        
        biz_days = get_business_days(start_date, end_date)
        for day in biz_days:
            dates_list = get_weekends(day) # dates_list[-1] == day       

            df = ingest_fn(dates_list) # QA tests should be done inside ingest_fn
            cleaned_df = clean_columns(df, day)

            # Pretend I'm doing try-except to log errors
            self.unity_writer.write(cleaned_df, day)
            self.azure_writer.write(cleaned_df, day)