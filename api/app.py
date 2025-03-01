from fastapi import FastAPI, Query, HTTPException, Response
from pydantic import BaseModel
from typing import Annotated
from datetime import datetime, timedelta
import psycopg2
from dotenv import load_dotenv
import os
import json

app = FastAPI()
load_dotenv(".env")

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST"),
    "dbname": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "port": os.getenv("POSTGRES_PORT"),
}


def query_pg(query: str, db_config: dict = DB_CONFIG):
    try:
        with psycopg2.connect(**db_config) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                result = cur.fetchall()
                table_cols = [desc[0] for desc in cur.description]
                return result, table_cols
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}")
        raise


class MicroBatch(BaseModel):
    end_time: datetime
    delta_in_minutes: int


@app.get("/")
def read_root():
    return "Hello, world!"


@app.get("/download")
def download(query: Annotated[MicroBatch, Query()]):
    """
    Download data from GitHub Archive for a given time range.
    """
    if query.delta_in_minutes <= 0:
        raise HTTPException(status_code=400, detail="Delta must be greater than 0.")
    start_of_day = query.end_time.replace(hour=0, minute=0, second=0, microsecond=0)
    start = max(
        start_of_day, query.end_time - timedelta(minutes=query.delta_in_minutes)
    )
    end = query.end_time

    table_name = end.strftime("%Y_%m_%d")

    is_table_exist, _ = query_pg(
        f"""
        SELECT 1
        FROM information_schema.tables
        WHERE table_name = '{table_name}';
        """
    )
    if not is_table_exist:
        raise HTTPException(
            status_code=404, detail=f"Data for `{table_name}` not found."
        )

    pg_query = f"""
    SELECT *
    FROM "{table_name}"
    WHERE created_at between '{start}' and '{end}'
    """

    result, table_cols = query_pg(pg_query)

    # convert result to JSON
    payload = []
    for row in result:
        payload.append(dict(zip(table_cols, row)))

    json_str = json.dumps(payload, default=str)
    resp = Response(content=json_str, media_type="application/json")
    filename = f"{end.strftime('%Y-%m-%dT%H:%M:%S')}-{query.delta_in_minutes}.json"
    resp.headers["Content-Disposition"] = f"attachment; filename={filename}"
    return resp
