import boto3
import io
import json
import pandas as pd
from botocore.exceptions import ClientError
import pyarrow as pa
import pyarrow.parquet as pq

from schema_registry import SCHEMAS

s3 = boto3.client('s3')

# CSV Functions
# def read_csv_from_s3(bucket, key):
#     obj = s3.get_object(Bucket=bucket, Key=key)
#     # Extract module name
#     file_name = key.split("/")[-1].lower()
#     module_name = file_name.split("-")[0]
#
#     # Validate schema
#     expected_headers = SCHEMAS.get(module_name)
#     return pd.read_csv(io.BytesIO(obj['Body'].read()), usecols=expected_headers, on_bad_lines="skip", low_memory=False, index_col=False)

def read_csv_from_s3(bucket, key):
    obj = s3.get_object(Bucket=bucket, Key=key)

    # Extract module name
    file_name = key.split("/")[-1].lower()
    module_name = file_name.split("-")[0]

    # Validate schema
    expected_headers = SCHEMAS.get(module_name)

    # Step 1: Load entire file first (no usecols here)
    df = pd.read_csv(io.BytesIO(obj['Body'].read()), on_bad_lines="skip", low_memory=False, index_col=False)

    # Step 2: Keep only columns that exist in both
    if expected_headers:
        available_cols = [col for col in expected_headers if col in df.columns]
        missing_cols = [col for col in expected_headers if col not in df.columns]

        if missing_cols:
            print(f"⚠️ Missing columns in {key}: {missing_cols}")

        df = df[available_cols]  # safely select existing columns

    return df

def read_parquet_from_s3(bucket: str, key: str, engine: str = 'pyarrow') -> pd.DataFrame:
    """
    Read a Parquet file from S3 and return it as a pandas DataFrame.

    Parameters:
        bucket (str): S3 bucket name.
        key (str): Full S3 key path to the parquet file.
        engine (str): Parquet engine to use ('pyarrow' or 'fastparquet').

    Returns:
        pd.DataFrame: The loaded dataframe.
    """
    response = s3.get_object(Bucket=bucket, Key=key)
    body = response["Body"].read()

    df = pd.read_parquet(io.BytesIO(body), engine=engine)
    return df

# def write_csv_to_s3(df, bucket, key):
#     buffer = io.StringIO()
#     df.to_csv(buffer, index=False)
#     s3.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())

def write_parquet_to_s3(df, bucket, key, module_name):

    if df is None:
        raise ValueError("❌ DataFrame (df) passed to write_parquet_to_s3() is None!")

    table = pa.Table.from_pandas(df)
    # print(f"table: {table}")
    # Write the Parquet data to an in-memory buffer
    buffer = io.BytesIO()
    pq.write_table(table, buffer, compression='snappy')

    # Reset buffer position to start
    buffer.seek(0)

    # Upload to S3
    s3.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())

# JSON Functions
def read_json_from_s3(bucket, key):
    obj = s3.get_object(Bucket=bucket, Key=key)
    return json.loads(obj['Body'].read().decode('utf-8'))

def write_json_to_s3(bucket, key, json_data):
    buffer = io.StringIO()
    buffer.write(json.dumps(json_data, indent=2))
    s3.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())

def list_objects_by_suffix(bucket, prefix, suffix):
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
    return [obj["Key"] for page in pages for obj in page.get("Contents", []) if obj["Key"].endswith(suffix)]

def append_json_log_to_s3(bucket, key, log_entry):
    try:
        # Try reading existing log file
        response = s3.get_object(Bucket=bucket, Key=key)
        existing_data = json.loads(response["Body"].read().decode("utf-8"))
        if not isinstance(existing_data, list):
            existing_data = [existing_data]
    except ClientError as e:
        if e.response['Error']['Code'] == "NoSuchKey":
            existing_data = []
        else:
            raise

    # Append new entry and write back
    existing_data.append(log_entry)
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(existing_data, indent=2).encode("utf-8"),
        ContentType="application/json"
    )
