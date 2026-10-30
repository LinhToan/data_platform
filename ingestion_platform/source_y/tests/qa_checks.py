from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
from functools import reduce




def test_null_columns(df: DataFrame, qa_columns: list[str]) -> int:
    """
    Overview:
        Takes in a given dataframe and its given to check if there are any null values.
    Paramters:
        df: Dataframe to be checked.
        pk: Primary key column.
    Return:
        0 to signify success
    """
    spark = SparkSession.builder.getOrCreate()
    
    null_condition = reduce(
        lambda x, y: x | y,
        [col(c).isNull() for c in qa_columns]
    )

    has_nulls = df.filter(null_condition).limit(1).count() > 0
    
    if has_nulls > 0:
        raise Exception(f"Null values found in {qa_columns}")

    return 0