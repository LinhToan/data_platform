from pyspark.sql import SparkSession, DataFrame
from datetime import date
from enum import Enum
import inspect
from dataclasses import dataclass


class WriteAzureMode(Enum):
    PARQUET = "parquet"
    CSV = "csv"
    JSON = "json"


@dataclass
class AzureConfig:
    """Class for UnityWriter configurations."""
    azure_path: str
    file_name: str
    mode: WriteAzureMode


class AzureWriter:
    """Provides write() method to write PySpark dataframe to Azure blob storage, either as parquet or CSV file."""

    def __init__(self, azure_config: dict):
        """
        file_name: Name of the parquet or CSV file to be created from the given dataframe.
        mode: Either "parquet" or "csv".
        """

        self.spark = SparkSession.builder.getOrCreate()
        self.azure_path = azure_config.azure_path
        self.file_name = azure_config.file_name
        self.mode = azure_config.mode

    def write(self, df: DataFrame, day: date) -> None:
        """
        Overview:
            Write the given dataframe to the Azure blob storage.
        Parameters:
            df (DataFrame): The dataframe to be saved to Azure blob storage.
            day (date): Date of file creation in yyyy/mm/dd format. This will be used as part of the directory name.
        Return:
            None.
        """

        full_path = f"{self.azure_path}/{self.file_name}/{day.strftime("%Y/%m/%d")}"

        if mode is WriteAzureMode.PARQUET:
            df.write.mode("overwrite").parquet(full_path)
        elif mode is WriteAzureMode.CSV:
            df.coalesce(1).write.mode("overwrite").option("header", "true").csv(full_path)
        elif mode is WriteAzureMode.JSON:
            pass

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


class WriteUnityMode(Enum):
    DAILY_SNAPSHOT = "daily_snapshot"
    UPSERT = "upsert"
    OVERWRITE = "overwrite"


@dataclass
class UnityConfig:
    """Class for UnityWriter configurations."""
    unity_path: str
    table_name: str
    mode: WriteUnityMode


class UnityWriter:
    """Provides write() method to write PySpark dataframe to Unity Catalog."""

    def __init__(self, unity_config: dict):
        """
        table_name (str): Name of the table to be created or updated in Unity Catalog.
        mode (str): Either "daily_snapshot", "upsert", or "overwrite". Overwrite uses CREATE OR REPLACE; upsert uses MERGE INTO; daily_snapshot uses overwrite with replaceWhere.
        """
        
        self.spark = SparkSession.builder.getOrCreate()
        self.unity_path = unity_config.unity_path
        self.table_name = unity_config.table_name
        self.mode = unity_config.mode

    ##############
    ### Public ###
    ##############
    def write(self, df: DataFrame, day: date | None = None, merge_cols: list[str] | None = None) -> None:
        """
        Overview:
            Create or update the specified delta table in Unity Catalog, depending on the given argument for the mode parameter.
            Will raise errors if an invalid mode value is passed in, which is handled by the WriteUnityMode class.
            Also errors will be raised if day or merge_cols is missing if you pass in daily_snapshot and upsert respectively.
        Parameters:
            df (DataFrame): The dataframe to be saved to Unity Catalog.
            day (date): utc_timestamp value for daily snapshots. This is used for the replaceWhere option.
            merge_cols (list[str]): List of columns to be used for the merge condition.
        Return:
            None.
        """

        full_table = f"{self.unity_path}.{self.table_name}"

        if self.mode is WriteUnityMode.OVERWRITE:
            self._create_replace(df, full_table)

        elif self.mode is WriteUnityMode.DAILY_SNAPSHOT:
            if day is None:
                raise ValueError("day must be provided for daily_snapshot")
            self._replace_where(df, full_table, day)

        elif self.mode is WriteUnityMode.UPSERT:
            if not merge_cols:
                raise ValueError("merge_cols must be provided for upsert")
            self._merge_into(df, full_table, merge_cols)

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

    ########################
    ### Internal helpers ###
    ########################
    def _create_replace(self, df: DataFrame, full_table: str) -> None:

        df.createOrReplaceTempView('df')

        self.spark.sql(f"""
            CREATE OR REPLACE TABLE {full_table}
            USING DELTA
            CLUSTER BY (utc_timestamp)
            AS SELECT * FROM df
        """)


    def _replace_where(self, df: DataFrame, full_table: str, day: date | None = None) -> None:
        
        df.write \
            .mode('overwrite') \
            .format('delta') \
            .option("replaceWhere", f"utc_timestamp = DATE '{day}'") \
            .option("mergeSchema", "true") \
            .saveAsTable(full_table)


    def _merge_into(self, df: DataFrame, full_table: str, merge_cols: list[str] | None = None) -> None:

        df.createOrReplaceTempView('df')
        # Are we better off creating a composite key column if we need to use multiple columns to merge?
        merge_condition = " AND ".join([f"target.{c} = source.{c}" for c in merge_cols])

        self.spark.sql(f"""
            MERGE INTO {full_table} AS target
            USING df AS source
            ON {merge_condition}
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)