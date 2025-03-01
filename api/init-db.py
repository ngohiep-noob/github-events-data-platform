import requests
import gzip
from pathlib import Path
import json
import psycopg2
from psycopg2.extras import execute_batch
from dotenv import load_dotenv
import os
import re


def run_ddl(db_config: dict, ddl_query: str):
    """
    Run a DDL script in a PostgreSQL database.

    Args:
        db_config (dict): Database connection details with keys: host, dbname, user, password, port.
        ddl_query (str): DDL script to execute.
    """

    try:
        with psycopg2.connect(**db_config) as conn:
            with conn.cursor() as cur:
                cur.execute(ddl_query)

                print("DDL script executed successfully.")
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}")
        raise


def create_table(db_config: dict, table_name: str):
    """
    Creates a table in a PostgreSQL database.

    Args:
        db_config (dict): Database connection details with keys: host, dbname, user, password, port.
    """
    create_table_query = f"""
    CREATE TABLE "{table_name}" (
        id BIGINT NOT NULL,
        type TEXT NOT NULL,
        actor JSONB NOT NULL,
        repo JSONB NOT NULL,
        payload JSONB NOT NULL,
        public BOOLEAN NOT NULL,
        created_at TIMESTAMP NOT NULL,
        org JSONB
    );
    """

    run_ddl(db_config, create_table_query)


def drop_table(db_config: dict, table_name: str):
    """
    Drops a table in a PostgreSQL database.

    Args:
        db_config (dict): Database connection details with keys: host, dbname, user, password, port.
        table_name (str): Name of the table to drop.
    """
    drop_table_query = f"""
    DROP TABLE IF EXISTS "{table_name}";
    """

    run_ddl(db_config, drop_table_query)


def read_json(file_path: Path) -> list:
    raw = []
    with open(str(file_path), "r", encoding="utf8") as file:
        # Read each line in the file
        for line in file:
            # Convert the string to a dictionary
            data = json.loads(line)
            if "org" not in data:
                data["org"] = None
            raw.append(data)
        print(f"Read {len(raw)} objects from {file_path}")
    return raw


def clean_json_data(data):
    if isinstance(data, str):
        # Remove null characters
        return re.sub(r"\u0000", "", data)
    elif isinstance(data, dict):
        # Recursively clean dictionaries
        return {k: clean_json_data(v) for k, v in data.items()}
    elif isinstance(data, list):
        # Recursively clean lists
        return [clean_json_data(item) for item in data]
    else:
        return data


def write_batch(db_config: dict, table_name: str, object_list: list):
    # Ensure the list is not empty
    if not object_list:
        raise ValueError("The object list is empty. Nothing to write.")

    # Prepare data for insertion
    columns = object_list[0].keys()
    values = []

    for obj in object_list:
        row = []
        for col in columns:
            value = clean_json_data(obj[col])
            if isinstance(value, (dict, list)):  # Convert dict or list to JSON string
                row.append(json.dumps(value))
            else:
                row.append(value)
        values.append(row)

    # Generate the SQL INSERT statement
    columns_str = ", ".join(columns)
    placeholders = ", ".join([f"%s" for _ in columns])
    insert_query = f'INSERT INTO "{table_name}" ({columns_str}) VALUES ({placeholders})'

    try:
        with psycopg2.connect(**db_config) as conn:
            with conn.cursor() as cur:
                # Use execute_batch for efficient bulk insert
                execute_batch(cur, insert_query, values, page_size=5000)

                print(f"Successfully inserted objects into {table_name}.")
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}")
        raise


if __name__ == "__main__":
    root_path = Path(__file__).resolve().parent

    data_path = root_path / "data"
    data_path.mkdir(exist_ok=True)

    load_dotenv(root_path / ".env")
    if os.getenv("INIT_DB") != "true":
        print(
            "Set the INIT_DB environment variable to 'true' to run initialization script."
        )
        exit()

    db_config = {
        "host": os.getenv("POSTGRES_HOST"),
        "dbname": os.getenv("POSTGRES_DB"),
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
        "port": os.getenv("POSTGRES_PORT"),
    }
    print(f"Database configuration: {db_config}")

    download_info = {
        "2024-12-01": (8, 9),
        "2024-12-02": (8, 9),
        "2024-12-03": (8, 9),
    }
    print(f"Downloading data from GitHub Archive...\n{download_info}")

    for date, hour_range in download_info.items():
        hours = [f"{hour}" for hour in range(hour_range[0], hour_range[1] + 1)]
        table_name = date.replace("-", "_")
        drop_table(db_config, table_name=table_name)
        create_table(db_config, table_name=table_name)

        for hour in hours:
            time_id = f"{date}-{hour}"
            url = f"https://data.gharchive.org/{time_id}.json.gz"
            print(f"Downloading {url}...")
            response = requests.get(url, stream=True)

            dest_file = data_path / f"{time_id}.json"

            # Save the .gz file
            with open(f"{str(dest_file)}.gz", "wb") as handle:
                for data in response.iter_content(chunk_size=1024):
                    handle.write(data)
            print(f"Downloaded {str(dest_file)}.gz")

            # Unzip the file
            print(f"Unzipping {str(dest_file)}.gz...")
            with gzip.open(f"{str(dest_file)}.gz", "rb") as f:
                content = f.read()
                with open(dest_file, "wb") as handle:
                    handle.write(content)

            # Read the JSON file
            objects = read_json(dest_file)

            # Write the objects to the database
            print(
                f"Writing {len(objects)} objects to the database table {table_name}..."
            )
            write_batch(db_config, table_name=table_name, object_list=objects)

            # remove the .gz file
            (data_path / f"{time_id}.json.gz").unlink()

            # remove the .json file
            (data_path / f"{time_id}.json").unlink()

    print("Initialization script completed.")
