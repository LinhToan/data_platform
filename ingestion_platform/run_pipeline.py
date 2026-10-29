from datetime import date
from functools import partial
from includes.write import AzureWriter, UnityWriter
from includes.pipeline import IngestionPipeline
from config import file_type, schema
from main import main

if __name__=="__main__":
    unity_config = {
        "unity_path": "catalog.schema",
        "table_name": file_type,
        "mode": "daily_snapshot"
    }

    azure_config = {
        "azure_path": "/mnt/storage_account/storage_container/directory", 
        "file_name": file_type, 
        "mode": "parquet",
        "azure_write": True
    }

    unity_writer = UnityWriter(unity_config)
    azure_writer = AzureWriter(azure_config)
    pipeline = IngestionPipeline(unity_writer=unity_writer, azure_writer=azure_writer)

    start_date = date.today()
    end_date = date.today()
    ingest_fn = partial(main, file_type=file_type, schema=schema)
    pipeline.run(start_date=start_date, end_date=end_date, ingest_fn=ingest_fn)