from airflow.models import DAG
from datetime import timedelta
import pendulum as pdl
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator


default_args = {
    "owner": "Ngo Hiep",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    "gharchive_data_ingestion",
    default_args=default_args,
    description="A workflow to ingest data from GitHub Archive API and store to MinIO",
    schedule_interval="* 5 * * *",  # 5AM daily
    start_date=pdl.datetime(2024, 2, 1, tz=pdl.timezone("Asia/Ho_Chi_Minh")),
    catchup=False,
) as dag:
    create_bucket = S3CreateBucketOperator(
        task_id="create_bucket", bucket_name="gharchive", aws_conn_id="minio_s3_conn"
    )
