import gzip
import json
from typing import List, Any, Dict, Optional
import re
import requests
from io import BytesIO
import pyarrow as pa
import pendulum as pdl


def get_nested(obj: Dict, *keys, default: Any = None) -> Any:
    """Safely access nested dictionary values with a default fallback."""
    current = obj
    for key in keys:
        if isinstance(current, dict) and key in current:
            current = current[key]
        else:
            return default
    return current if current is not None else default


def extract_filename_date(file_path: str) -> Optional[str]:
    """Extract date pattern from filename."""
    date_match = re.search(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}-\d+", file_path)
    return date_match.group(0) if date_match else None


def extract_row_data(data: Dict) -> List[Any]:
    """Extract fields from a single JSON object matching the bash script's jq query."""
    return [
        data.get("type"),
        get_nested(data, "actor", "login")
        or get_nested(data, "actor_attributes", "login")
        or (data.get("actor") if isinstance(data.get("actor"), str) else None),
        get_nested(data, "repo", "name")
        or (
            f"{get_nested(data, 'repository', 'owner')}/{get_nested(data, 'repository', 'name')}"
            if get_nested(data, "repository", "owner")
            and get_nested(data, "repository", "name")
            else None
        ),
        data.get("created_at"),
        get_nested(data, "payload", "updated_at")
        or get_nested(data, "payload", "comment", "updated_at")
        or get_nested(data, "payload", "issue", "updated_at")
        or get_nested(data, "payload", "pull_request", "updated_at"),
        get_nested(data, "payload", "action"),
        get_nested(data, "payload", "comment", "id"),
        get_nested(data, "payload", "review", "body")
        or get_nested(data, "payload", "comment", "body")
        or get_nested(data, "payload", "issue", "body")
        or get_nested(data, "payload", "pull_request", "body")
        or get_nested(data, "payload", "release", "body"),
        get_nested(data, "payload", "comment", "path"),
        get_nested(data, "payload", "comment", "position"),
        get_nested(data, "payload", "comment", "line"),
        get_nested(data, "payload", "ref"),
        get_nested(data, "payload", "ref_type"),
        get_nested(data, "payload", "comment", "user", "login")
        or get_nested(data, "payload", "issue", "user", "login")
        or get_nested(data, "payload", "pull_request", "user", "login"),
        get_nested(data, "payload", "issue", "number")
        or get_nested(data, "payload", "pull_request", "number")
        or get_nested(data, "payload", "number"),
        get_nested(data, "payload", "issue", "title")
        or get_nested(data, "payload", "pull_request", "title"),
        [
            label.get("name")
            for label in (
                get_nested(data, "payload", "issue", "labels")
                or get_nested(data, "payload", "pull_request", "labels")
                or []
            )
        ],
        get_nested(data, "payload", "issue", "state")
        or get_nested(data, "payload", "pull_request", "state"),
        get_nested(data, "payload", "issue", "locked")
        or get_nested(data, "payload", "pull_request", "locked"),
        get_nested(data, "payload", "issue", "assignee", "login")
        or get_nested(data, "payload", "pull_request", "assignee", "login"),
        [
            assignee.get("login")
            for assignee in (
                get_nested(data, "payload", "issue", "assignees")
                or get_nested(data, "payload", "pull_request", "assignees")
                or []
            )
        ],
        get_nested(data, "payload", "issue", "comments")
        or get_nested(data, "payload", "pull_request", "comments"),
        get_nested(data, "payload", "review", "author_association")
        or get_nested(data, "payload", "issue", "author_association")
        or get_nested(data, "payload", "pull_request", "author_association"),
        get_nested(data, "payload", "issue", "closed_at")
        or get_nested(data, "payload", "pull_request", "closed_at"),
        get_nested(data, "payload", "pull_request", "merged_at"),
        get_nested(data, "payload", "pull_request", "merge_commit_sha"),
        [
            reviewer.get("login")
            for reviewer in get_nested(
                data, "payload", "pull_request", "requested_reviewers"
            )
            or []
        ],
        [
            team.get("name")
            for team in get_nested(data, "payload", "pull_request", "requested_teams")
            or []
        ],
        get_nested(data, "payload", "pull_request", "head", "ref"),
        get_nested(data, "payload", "pull_request", "head", "sha"),
        get_nested(data, "payload", "pull_request", "base", "ref"),
        get_nested(data, "payload", "pull_request", "base", "sha"),
        get_nested(data, "payload", "pull_request", "merged"),
        get_nested(data, "payload", "pull_request", "mergeable"),
        get_nested(data, "payload", "pull_request", "rebaseable"),
        get_nested(data, "payload", "pull_request", "mergeable_state"),
        get_nested(data, "payload", "pull_request", "merged_by", "login"),
        get_nested(data, "payload", "pull_request", "review_comments"),
        get_nested(data, "payload", "pull_request", "maintainer_can_modify"),
        get_nested(data, "payload", "pull_request", "commits"),
        get_nested(data, "payload", "pull_request", "additions"),
        get_nested(data, "payload", "pull_request", "deletions"),
        get_nested(data, "payload", "pull_request", "changed_files"),
        get_nested(data, "payload", "comment", "diff_hunk"),
        get_nested(data, "payload", "comment", "original_position"),
        get_nested(data, "payload", "comment", "commit_id"),
        get_nested(data, "payload", "comment", "original_commit_id"),
        get_nested(data, "payload", "size"),
        get_nested(data, "payload", "distinct_size"),
        get_nested(data, "payload", "member", "login")
        or get_nested(data, "payload", "member"),
        get_nested(data, "payload", "release", "tag_name"),
        get_nested(data, "payload", "release", "name"),
        get_nested(data, "payload", "review", "state"),
    ]


def process_url(file_url: str) -> List[List[Any]]:
    """Process a single .json.gz file from a URL on-the-fly."""
    rows = []
    try:
        # Stream the file from the URL
        with requests.get(file_url, stream=True) as response:
            response.raise_for_status()  # Raise exception for bad status codes

            # Decompress the gzipped content in memory
            with gzip.GzipFile(fileobj=BytesIO(response.raw.read())) as gz:
                data = json.loads(gz.read().decode("utf-8"))
                # Extract rows from the JSON data
                for item in data:
                    rows.append(extract_row_data(item))
    except requests.RequestException as e:
        raise Exception(f"Error downloading {file_url}: {str(e)}")
    except Exception as e:
        raise Exception(f"Error processing {file_url}: {str(e)}")
    return rows


def create_table_with_type_check(rows, schema):
    data_dicts = []

    def check_value(value, field_name, field_type, row_idx, path=""):
        """Recursively check a value against its expected PyArrow type."""
        current_path = f"{field_name}{path}"

        # Handle lists
        if pa.types.is_list(field_type) and isinstance(value, list):
            if isinstance(value, type(None)) or value == []:  # valid nullable list
                return

            item_type = field_type.value_type
            for i, item in enumerate(value):
                check_value(item, field_name, item_type, row_idx, f"[{i}]")
            return

        # Handle dictionaries
        if pa.types.is_dictionary(field_type):
            if pa.types.is_string(field_type.value_type) and not isinstance(
                value, (str, type(None))
            ):
                print(
                    f"Row {row_idx}, Field '{current_path}': Expected string, got '{value}'"
                )
            return

        if (
            (pa.types.is_integer(field_type) and isinstance(value, (int, type(None))))
            or (pa.types.is_boolean(field_type) and value in (True, False, type(None)))
            or (pa.types.is_string(field_type) and isinstance(value, (str, type(None))))
            or (
                pa.types.is_timestamp(field_type)
                and isinstance(value, (str, type(None)))
            )
        ):  # conditions for valid types
            return

        # Catch unexpected types
        if value is not None and not pa.types.is_null(field_type):
            expected_category = (
                "integer"
                if pa.types.is_integer(field_type)
                else "string" if pa.types.is_string(field_type) else str(field_type)
            )
            print(
                f"Row {row_idx}, Field '{current_path}': Expected {expected_category}, got {type(value).__name__} '{value}'"
            )

    for i, row in enumerate(rows):
        row_dict = dict(zip(schema.names, row))
        for field_name, value in row_dict.items():
            field_type = schema.field(field_name).type
            check_value(value, field_name, field_type, i)

            if pa.types.is_timestamp(field_type) and isinstance(
                value, str
            ):  # convert datetime string to timestamp
                row_dict[field_name] = pdl.parse(value).int_timestamp

            if pa.types.is_integer(field_type) and isinstance(value, bool):
                row_dict[field_name] = int(value)
        data_dicts.append(row_dict)

    return pa.Table.from_pylist(data_dicts, schema=schema)
