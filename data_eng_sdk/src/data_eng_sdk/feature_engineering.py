from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
import inspect

class FeatureEngineer:
    """
    Provide the aggregate_monthly() method to aggregate a quantity column (balance or transactions) per month for each customer.
    """

    def __init__(self):
        self.spark = SparkSession.builder.getOrCreate()

    ##############
    ### Public ###
    ##############
    def aggregate_monthly(self, df: DataFrame, groupby_cols: list, agg_col: list, date_col, max_months_ago: int) -> DataFrame:
        """
        Overview:
            Aggregate balances or transactions, depending on the dataframe, by the given groupby columns. 
            This calculates months ago from the anchor date/date key using the given date column, typically transaction_date or utc_timestamp.
        Parameters:
            df (DataFrame): PySpark dataframe to aggregate.
            groupby_cols (list): List of columns to group by.
            agg_col (list): Column to aggregate.
            date_col (str): Column to calculate months ago with the anchor date/date key.
            max_months_ago (int): Maximum months ago to aggregate, typically will be 12 or 24 months.
        Return:
            Dataframe aggregated by month for each customer, aggregates for sum, avg, max, min, and count.
        """

        groupby_cols.append('months_ago')
        monthly_df = self._create_months_ago(df, date_col, max_months_ago)

        return (
            monthly_df
            .groupBy(*groupby_cols)
            .agg(
                F.sum(agg_col).alias('sum_amount'),
                F.avg(agg_col).alias('avg_amount'),
                F.max(agg_col).alias('max_amount'),
                F.min(agg_col).alias("min_amount"),
                F.count(agg_col).alias("count_amount")
            )
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

    ########################
    ### Internal helpers ###
    ########################
    def _create_months_ago(self, df: DataFrame, date_col: str, max_months_ago: int) -> DataFrame:
        """Create a column that calculates how many months are between the anchor date and dataframe's date column."""

        return (
            df.withColumn(
                'date_diff',
                F.datediff(F.col('date_key'), F.col(date_col))
            ).withColumn(
                'months_ago',
                F.ceil(F.col('date_diff') / 30)
            ).filter(
                (F.col('months_ago') > 0) &
                (F.col('months_ago') <= max_months_ago)
            )
            .drop('date_diff')
        )