from pyspark.sql import SparkSession, DataFrame
from datetime import date
from pyspark.sql.functions import col
import inspect

class UnityReader:
    """Provides a read() method to read in a Unity Catalog table and filter on a date range."""

    def __init__(self, unity_path):
        self.spark = SparkSession.builder.getOrCreate()
        self.unity_path = unity_path

    ##############
    ### Public ###
    ##############
    def read(self, table_name: str, min_date: date, max_date: date) -> DataFrame:
        """
        Overview:
            Read in the given delta table in Unity Catalog and filter on the given dates.
            Typically we partition/liquid cluster by utc_timestamp, so this will make reads fast.
        Parameters:
            table_name (str): Name of the delta table in Unity Catalog.
            min_date (date): Start date to filter utc_timestamp on.
            max_date (date): End date to filter utc_timestamp on.
        Return: 
            PySpark dataframe of the given table filtered on the given dates.
        """

        full_table = f"{self.unity_path}.{table_name}"
        
        return (
            self.spark.read.table(full_table)
            .filter(col('utc_timestamp').between(min_date, max_date))
        )

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