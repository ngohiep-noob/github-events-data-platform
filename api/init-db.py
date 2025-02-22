from tqdm import tqdm
import requests
import gzip
from pathlib import Path
import json
import psycopg2
from psycopg2.extras import execute_batch

root_path = Path(__file__).resolve().parent

data_path = root_path / "data"
data_path.mkdir(exist_ok=True)

download_info = {
    "2024-12-01": (7, 17),
    "2024-12-02": (7, 17),
    "2024-12-03": (7, 17),
}


def create_table(db_config: dict, table_name: str):
    """
    Creates a table in a PostgreSQL database.

    Args:
        db_config (dict): Database connection details with keys: host, dbname, user, password, port.
    """
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS "{table_name}" (
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

    conn = psycopg2.connect(**db_config)
    try:
        cur = conn.cursor()
        cur.execute(create_table_query)
        conn.commit()
        print(f"Table {table_name} created successfully.")
        cur.close()
        conn.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}")
        if conn:
            conn.rollback()
        raise


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


def write_objects_to_postgres(db_config: dict, table_name: str, object_list: list):
    # Ensure the list is not empty
    if not object_list:
        raise ValueError("The object list is empty. Nothing to write.")

    # Prepare data for insertion
    columns = object_list[0].keys()
    values = []

    for obj in object_list:
        row = []
        for col in columns:
            value = obj[col]
            if isinstance(value, (dict, list)):  # Convert dict or list to JSON string
                row.append(json.dumps(value))
            else:
                row.append(value)
        values.append(row)

    # Generate the SQL INSERT statement
    columns_str = ", ".join(columns)
    placeholders = ", ".join([f"%s" for _ in columns])
    insert_query = f'INSERT INTO "{table_name}" ({columns_str}) VALUES ({placeholders})'

    conn = psycopg2.connect(**db_config)
    try:
        cur = conn.cursor()
        # Use execute_batch for efficient bulk insert
        execute_batch(cur, insert_query, values, page_size=10000)
        conn.commit()

        cur.close()
        conn.close()
        print(f"Successfully inserted objects into {table_name}.")
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}")
        if conn:
            conn.rollback()
        raise


if __name__ == "__main__":
    db_config = {
        "host": "localhost",
        "dbname": "gharchive",
        "user": "gharchive",
        "password": "password",
        "port": 5432,
    }

    for date, hour_range in download_info.items():
        hours = [f"{hour}" for hour in range(hour_range[0], hour_range[1] + 1)]
        table_name = date.replace("-", "_")
        create_table(db_config, table_name=table_name)

        for hour in hours:
            time_id = f"{date}-{hour}"
            url = f"https://data.gharchive.org/{time_id}.json.gz"
            print(f"Downloading {url}...")
            response = requests.get(url, stream=True)

            dest_file = data_path / f"{time_id}.json"

            # Save the .gz file
            with open(f"{str(dest_file)}.gz", "wb") as handle:
                for data in tqdm(response.iter_content(chunk_size=1024), unit="kB"):
                    handle.write(data)

            # Unzip the file
            with gzip.open(f"{str(dest_file)}.gz", "rb") as f:
                content = f.read()
                with open(dest_file, "wb") as handle:
                    handle.write(content)

            # Read the JSON file
            objects = read_json(dest_file)

            # Write the objects to the database
            write_objects_to_postgres(
                db_config, table_name=table_name, object_list=objects
            )

            # remove the .gz file
            (data_path / f"{time_id}.json.gz").unlink()

            # remove the .json file
            (data_path / f"{time_id}.json").unlink()
