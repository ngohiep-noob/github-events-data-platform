from airflow.models import DAG
from datetime import timedelta
from airflow.decorators import task
import pendulum as pdl
from helpers import process_url, create_table_with_type_check
import urllib
from tempfile import NamedTemporaryFile
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from pyarrow_schema import gh_events_schema
import pyarrow.parquet as pq

default_args = {
    "owner": "Ngo Hiep",
    "depends_on_past": True,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}
TZ = pdl.timezone("UTC")


@task()
def process_mini_batch(**kwargs):
    data_interval_end = kwargs["data_interval_end"].in_tz(TZ)

    print(f"Processing data for {data_interval_end.to_datetime_string()}")

    params = {"end_time": data_interval_end.to_datetime_string(), "delta_in_minutes": 5}
    url = "http://web-server:8000/download?" + urllib.parse.urlencode(params)
    print(f"Downloading data from {url}")

    rows = process_url(url)
    print(f"Processing {len(rows)} rows")

    if len(rows) == 0:
        print("No data to process")
        return

    # Write as parquet to MinIO
    # Create a temporary file
    with NamedTemporaryFile(delete=False) as tmpfile:
        table = create_table_with_type_check(rows, gh_events_schema)
        pq.write_table(table, tmpfile.name)

    # Define the S3 key and bucket
    s3_key = f"{data_interval_end.to_date_string()}/{data_interval_end.to_time_string()}.parquet"
    s3_bucket = "gharchive"

    # Upload the file to S3
    s3_hook = S3Hook(aws_conn_id="minio_conn")
    s3_hook.load_file(tmpfile.name, s3_key, s3_bucket)
    print(f"Uploaded {tmpfile.name} to s3://{s3_bucket}/{s3_key}")


with DAG(
    "gharchive_data_ingestion",
    default_args=default_args,
    description="A workflow to ingest data from GitHub Archive API and store to MinIO",
    schedule_interval="*/5 8-9 * * *",  # Every 5 minutes from 8 to 9 UTC
    start_date=pdl.datetime(2024, 12, 1, tz=TZ),
    catchup=True,
    max_active_runs=1,
) as dag:
    process_mini_batch()
