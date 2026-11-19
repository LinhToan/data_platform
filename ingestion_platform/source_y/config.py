from data_eng_sdk.pipelines import DeliverySchedule
from data_eng_sdk.io import WriteUnityMode, UnityConfig

delivery_schedule = DeliverySchedule.BUSINESS_DAYS

azure_path = "/mnt/storage_account/blob_container"

file_name = 'file_name'

unity_config = UnityConfig(
    unity_path="catalog.schema",
    table_name="table",
    mode=WriteUnityMode.DAILY_SNAPSHOT
)