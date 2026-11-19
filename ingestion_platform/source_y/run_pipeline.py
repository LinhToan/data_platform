### Overview
# This notebook gets reads in the source data file in CSV format from and updates 
# the respective delta table in Unity Catalog. 
# The files arrive every business day evening, 
# and the corresponding GoAnywhere job runs the next morning at 9:00 AM CST. 

### Frequency
# Business days at 9:15 AM CST.

### Downstream
# TBD

### Troubleshooting
# In the event of a failure or partial run, this notebook can be re-run without issue 
# because the process is idempotent. That is, the `daily_snapshot` mode in our UnityWriter's write method 
# overwrites the given day's data or inserts new data if the day's data does not exist in the table. 
# This is similar to a SQL MERGE statement, except it updates data on a partition level instead of a 
# row level and the `replaceWhere` option that UnityWriter utlizes is atomic.

from datetime import date
from data_eng_sdk.io import UnityWriter
from data_eng_sdk.pipelines import IngestionPipeline
from config import delivery_schedule, unity_config
from main import main

if __name__=="__main__":

    unity_writer = UnityWriter(unity_config)
    pipeline = IngestionPipeline(delivery_schedule=delivery_schedule, unity_writer=unity_writer)

    start_date = date.today()
    end_date = date.today()
    pipeline.run(start_date=start_date, end_date=end_date, ingest_fn=main)