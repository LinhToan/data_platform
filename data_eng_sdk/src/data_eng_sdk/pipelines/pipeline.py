from datetime import date
import inspect
from enum import Enum
from pyspark.sql import DataFrame
from pyspark.dbutils import DBUtils
from collections.abc import Callable
from ..io import UnityWriter, AzureWriter
from ..utils import is_business_day, filter_business_days, get_weekends, clean_columns


class DeliverySchedule(Enum):
    CALENDAR_DAYS = "calendar_days"
    BUSINESS_DAYS = "business_days"


class IngestionPipeline:
    """Boilerplate for source data ingestion pipelines."""

    def __init__(self, delivery_schedule: DeliverySchedule, unity_writer: UnityWriter, azure_writer: AzureWriter | None = None) -> None:
        self.azure_writer = azure_writer
        self.unity_writer = unity_writer
        self.delivery_schedule = delivery_schedule

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
            return
        
        biz_days = filter_business_days(start_date, end_date)
        for day in biz_days:
            if self.delivery_schedule == DeliverySchedule.CALENDAR_DAYS:
                dates_list = get_weekends(day)
            elif self.delivery_schedule == DeliverySchedule.BUSINESS_DAYS:
                dates_list = [day.strftime("%Y/%m/%d")]
            else:
                raise ValueError(f"Unsupported delivery schedule: {self.delivery_schedule}")

            df = ingest_fn(dates_list)
            cleaned_df = clean_columns(df, day)

            self.unity_writer.write(cleaned_df, day)

            if self.azure_writer:
                self.azure_writer.write(cleaned_df, day)

    @classmethod
    def help(cls):
        print(f"{cls.__name__}")
        print("=" * len(cls.__name__))
        print()
        print(inspect.getdoc(cls) or "No description available.")
        print()
        print("Methods:")

        for name, method in inspect.getmembers(cls, inspect.isfunction):
            if not name.startswith("_"):
                print(f"\n{name}()")
                print(inspect.getdoc(method) or "No documentation available.")