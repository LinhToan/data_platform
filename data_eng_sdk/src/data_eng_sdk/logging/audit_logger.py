from pyspark.sql import SparkSession, DataFrame
from datetime import date, datetime
from enum import Enum
from pyspark.sql.functions import col, lit
import inspect


class WriteAuditMode(Enum):
    WRITE = "write"
    READ = "read"
    QA = "qa"


class AuditLogger:
    """
    Provides log_unity() method to log the number of new records into the given table.
    Provides log_exception() method to log any errors when trying to read in a file from Azure or write to a table in Unity or run QA tests on a dataframe.
    """

    def __init__(self, unity_path):
        self.spark = SparkSession.builder.getOrCreate()
        self.unity_path = unity_path

    def log_unity(self, table_name: str, log_table: str, run_date: date, df_count: int) -> None:
        """
        Overview:
            Log the number of new records into the given table.
        Parameters:
            table_name (str): Name of the table that was just updated.
            log_table (str): Name of the table to log number of rows inserted.
            run_date (date): Processing date.
            df_count (int): Number of rows inserted.
        Return:
            None.
        """

        full_table = f"{self.unity_path}.{table_name}"

        log_df = self.spark.createDataFrame(
            [(table_name, run_date, df_count)], 
            schema=['table_name', 'utc_timestamp', 'rows_inserted']
        )

        log_df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("replaceWhere", f"table_name = '{table_name}' AND utc_timestamp = DATE '{run_date}'") \
            .saveAsTable(f"{self.unity_path}.{log_table}")


    def log_exception(self, table_name: str, log_table: str, location: str, operation: str, exception_message: str) -> None:
        """
        Overview:
            Log the error message when trying to read in a file from Azure or write to a table in Unity.
        Parameters:
            table_name (str): Name of the table that was attempted to be updated.
            log_table (str): Name of the table to log errors.
            location (str): Location of the Unity table or Azure blob storage.
            operation (str): Operation that was attempted. Must be read, write, or qa.
            exception_message (str): Error message.
        Return:
            None.
        """

        if isinstance(operation, str):
            raw_mode = operation
            try:                
                mode = WriteAuditMode(operation.lower().strip())
            except ValueError:
                valid_modes = [m.value for m in WriteAuditMode]
                raise ValueError(f"Invalid mode '{raw_mode}'. Expected one of: {valid_modes}")

        timestamp = datetime.now()

        # Create column for error counter in case we get the same error multiple times per day
        log_df = self.spark.createDataFrame(
            [(table_name, location, operation, timestamp, exception_message)], 
            schema=['table_name', 'location', 'operation', 'timestamp', 'exception_message']
        )

        log_df.write \
            .format("delta") \
            .mode("append") \
            .saveAsTable(f"{self.unity_path}.{log_table}")

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