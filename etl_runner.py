# import json
# import os
#
# import boto3
# from config import PROCESSOR_MAPPING
# from processors.process_file import process_file
# from utils.s3_handler import list_objects_by_suffix
# from utils.json_logger import write_json_log
#
# # S3 Bucket
# BUCKET_NAME = "s40334577-epca-bucket"
#
# # S3 Client
# s3 = boto3.client("s3")
#
# TRACKING_FILE = "processed_files.json"
#
# # Load tracking
# if os.path.exists(TRACKING_FILE):
#     with open(TRACKING_FILE, "r") as f:
#         processed_files = json.load(f).get("processed_files", [])
# else:
#     processed_files = []
#
# def get_top_level_folders(bucket):
#     """Fetch top-level folders from the root of the S3 bucket"""
#     result = s3.list_objects_v2(Bucket=bucket, Delimiter='/')
#     return [prefix['Prefix'].strip("/") for prefix in result.get('CommonPrefixes', [])]
#
# def get_processor(folder_name):
#     """Return processor class based on folder name"""
#     return PROCESSOR_MAPPING.get(folder_name)
#
# def run_etl_on_folder(folder):
#     input_prefix = f"{folder}/"
#     output_prefix = f"processed/{folder}/"
#     log_prefix = f"processed/logs/{folder}/"
#     log_key = f"{log_prefix}log.json"  # 🔑 This is the single JSON file for this folder
#
#     # print(f"input_prefix: {input_prefix}")
#     # print(f"output_prefix: {output_prefix}")
#     # print(f"log_prefix: {log_prefix}")
#     # print(f"log_key: {log_key}")
#     # Get all CSV files
#     csv_files = list_objects_by_suffix(BUCKET_NAME, input_prefix, '.csv')
#     # print(f"Found {len(csv_files)} files in {input_prefix}")
#     print(f"folder: {folder}")
#     processor_class = get_processor(folder)
#     print("Processing files: ", processor_class)
#     if not processor_class:
#         write_json_log(BUCKET_NAME, log_prefix, {"level": "warning", "message": f"No processor found for folder: {folder}"})
#         return
#     #
#     for key in csv_files:
#         if key not in processed_files:  # <-- Only process new files
#             process_file(processor_class, BUCKET_NAME, key, output_prefix, log_prefix)
#             processed_files.append(key)  # <-- Track it after processing
#         else:
#             print(f"⏩ Skipping already processed file: {key}")
#
# def main():
#     folders = get_top_level_folders(BUCKET_NAME)
#     print(folders)
#     for folder in folders:
#         if "_input" not in folder and ("auxiliary" in folder): #or "drive1" in folder or "drive2" in folder
#             run_etl_on_folder(folder)
#
#     # Save updated processed files
#     with open(TRACKING_FILE, "w") as f:
#         json.dump({"processed_files": processed_files}, f, indent=2)
#
#
#
# if __name__ == "__main__":
#     main()

import boto3
import json
import os

from config import PROCESSOR_MAPPING
from processors.process_file import process_file
from utils.s3_handler import list_objects_by_suffix
from utils.json_logger import write_json_log
from utils.partition_register import register_partition_for_file, extract_session_from_key, \
    extract_session_from_filename

# --- Config ---
BUCKET_NAME = "s40334577-epca-bucket"
DATABASE_NAME = "s40334577-epca-db"
TRACKING_FILE = "utils/processed_files.json"

# --- Setup S3 client ---
s3 = boto3.client("s3")

# --- Load processed files tracker ---
if os.path.exists(TRACKING_FILE):
    with open(TRACKING_FILE, "r") as f:
        processed_files = json.load(f)
else:
    processed_files = []

def save_tracking():
    """Save processed files list."""
    with open(TRACKING_FILE, "w") as f:
        json.dump(processed_files, f, indent=2)

def get_top_level_folders(bucket):
    """Fetch top-level folders from the root of the S3 bucket."""
    result = s3.list_objects_v2(Bucket=bucket, Delimiter='/')
    return [prefix['Prefix'].strip("/") for prefix in result.get('CommonPrefixes', [])]

def get_processor(folder_name):
    """Return processor class based on folder name."""
    return PROCESSOR_MAPPING.get(folder_name)

def run_etl_on_folder(folder):
    """ETL process for a given folder."""
    input_prefix = f"{folder}/"
    # output_prefix = f"processed/{folder}/"
    log_prefix = f"processed/logs/{folder}/"
    log_key = f"{log_prefix}log.json"

    csv_files = list_objects_by_suffix(BUCKET_NAME, input_prefix, '.csv')

    print(f"📂 Folder: {folder}")
    processor_class = get_processor(folder)
    print(f"🔍 Processor: {processor_class}")

    if not processor_class:
        write_json_log(BUCKET_NAME, log_key, {"level": "warning", "message": f"No processor found for folder: {folder}"})
        return

    for key in csv_files:
        filename = key.split("/")[-1]
        session_name = extract_session_from_filename(filename)
        # print(f"session name: {session_name}")
        output_prefix = f"processed/{folder}/session={session_name}/"
        # log_prefix = f"processed/logs/{folder}/session={session_name}/"
        # print(f"output prefix: {output_prefix}")
        if key in processed_files:
            print(f"⏩ Skipping already processed file: {key}")
            continue
        # print(f"key: {key}")
        try:
            output_key = process_file(processor_class, BUCKET_NAME, key, session_name,output_prefix, log_key)

            # Register new partition
            session_folder = extract_session_from_key(output_key)
            # table_name = folder.replace("_input", "").lower()
            register_partition_for_file(BUCKET_NAME, DATABASE_NAME, folder, session_name)

            # ✅ Successfully processed
            # processed_files.append(key)
            # save_tracking()

        except Exception as e:
            print(f"❌ Error processing {key}: {str(e)}")

def main():
    folders = get_top_level_folders(BUCKET_NAME)
    print(f"Found folders: {folders}")
    for folder in folders:
        if "_input" not in folder and ("vehicle" in folder):
            run_etl_on_folder(folder)

if __name__ == "__main__":
    main()
