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
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
import os

MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_ACCESS_SECRET = os.getenv("MINIO_ACCESS_SECRET")

default_args = {
    "owner": "Ngo Hiep",
    "depends_on_past": True,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}
TZ = pdl.timezone("UTC")


@task
def source_to_minio(**kwargs):
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
    s3_hook.load_file(tmpfile.name, s3_key, s3_bucket, replace=True)
    print(f"Uploaded {tmpfile.name} to s3://{s3_bucket}/{s3_key}")

    return f"{s3_bucket}/{s3_key}"  # Push location of the file in MinIO to XCom


with DAG(
    "gh_events_ingestion",
    default_args=default_args,
    description="A workflow to ingest data from GitHub Archive API and store to MinIO, then load to ClickHouse",
    schedule_interval="*/5 8-9 * * *",  # Every 5 minutes from 8 to 9 UTC
    start_date=pdl.datetime(2024, 12, 1, tz=TZ),
    end_date=pdl.datetime(2024, 12, 3, tz=TZ),
    catchup=True,
    max_active_runs=1,
) as dag:
    to_minio = source_to_minio()
    minio_to_clickhouse = ClickHouseOperator(
        task_id="minio_to_clickhouse",
        database="default",
        sql=(
            f"""
                INSERT INTO gharchive.github_events
                SELECT
                    now('Asia/Ho_Chi_Minh') synced_at,
                    event_type,
                    actor_login,
                    repo_name,
                    created_at,
                    updated_at,
                    ifNull(action, 'none') AS action,
                    comment_id,
                    body,
                    path,
                    position,
                    line,
                    ref,
                    ref_type,
                    creator_user_login,
                    number,
                    title,
                    labels,
                    state,
                    locked,
                    assignee,
                    assignees,
                    comments,
                    author_association,
                    closed_at,
                    merged_at,
                    merge_commit_sha,
                    requested_reviewers,
                    requested_teams,
                    head_ref,
                    head_sha,
                    base_ref,
                    base_sha,
                    merged,
                    mergeable,
                    rebaseable,
                    mergeable_state,
                    merged_by,
                    review_comments,
                    maintainer_can_modify,
                    commits,
                    additions,
                    deletions,
                    changed_files,
                    diff_hunk,
                    original_position,
                    commit_id,
                    original_commit_id,
                    push_size,
                    push_distinct_size,
                    member_login,
                    release_tag_name,
                    release_name,
                    review_state
                FROM s3(
                    'http://minio:9000/{{{{ ti.xcom_pull(task_ids=\"source_to_minio\") }}}}', 
                    '{MINIO_ACCESS_KEY}', 
                    '{MINIO_ACCESS_SECRET}', 
                    'Parquet'
                )
            """,
        ),
        query_id="{{ ti.dag_id }}-{{ ti.task_id }}-{{ ti.run_id }}-{{ ti.try_number }}",
        clickhouse_conn_id="clickhouse_conn",
    )

    to_minio >> minio_to_clickhouse
