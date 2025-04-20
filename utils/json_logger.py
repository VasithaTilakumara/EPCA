import json
from datetime import datetime
from utils.s3_handler import read_json_from_s3, write_json_to_s3

def load_existing_log(bucket, key):
    try:
        return read_json_from_s3(bucket, key)
    except Exception as e:
        print(f"⚠️ Could not load log file {key}: {str(e)}")
        return []

def write_json_log(bucket, key, log_entry):
    log_data = load_existing_log(bucket, key)
    log_data.append(log_entry)
    write_json_to_s3(bucket, key, log_data)