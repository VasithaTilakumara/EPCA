import boto3
import io
import json
import pandas as pd

s3 = boto3.client('s3')

# CSV Functions
def read_csv_from_s3(bucket, key):
    obj = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(io.BytesIO(obj['Body'].read()), on_bad_lines="skip", low_memory=False, index_col=False)

def write_csv_to_s3(df, bucket, key):
    buffer = io.StringIO()
    df.to_csv(buffer, index=False)
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
