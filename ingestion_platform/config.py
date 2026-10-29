from pyspark.sql.types import StructType, StructField, StringType

file_type = "daily_file.txt"

#schema provided in MQ Credit Bank Files documentation
schema = StructType(fields=[
    StructField('Pretend we have a full schema', StringType(), False),
])