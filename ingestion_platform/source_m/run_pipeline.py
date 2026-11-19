from datetime import date
from functools import partial
from data_eng_sdk.io import AzureWriter, UnityWriter, UnityConfig
from data_eng_sdk.pipelines import IngestionPipeline
from config import mq_files, delivery_schedule
from main import main


if __name__=="__main__":

    start_date = date.today()
    end_date = date.today()

    for cbf in mq_files:
        file_type = cbf['file_type']
        schema = cbf['schema']

        unity_config = UnityConfig(
            unity_path="catalog.schema",
            table_name="table",
            mode=WriteUnityMode.DAILY_SNAPSHOT
        )

        azure_config = {
            "azure_path": "/mnt/storage_account/blob_container/source_system", 
            "file_name": file_type, 
            "mode": "parquet",
            "azure_write": False
        }

        unity_writer = UnityWriter(unity_config)
        azure_writer = AzureWriter(azure_config)
        pipeline = IngestionPipeline(delivery_schedule=delivery_schedule, unity_writer=unity_writer, azure_writer=azure_writer)

        ingest_fn = partial(main, file_type=file_type, schema=schema)
        pipeline.run(start_date=start_date, end_date=end_date, ingest_fn=ingest_fn)