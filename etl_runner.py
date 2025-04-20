import boto3
from config import PROCESSOR_MAPPING
from processors.process_file import process_file
from utils.s3_handler import list_objects_by_suffix
from utils.json_logger import write_json_log

# S3 Bucket
BUCKET_NAME = "s4033457-data-pipeline-bucket"

# S3 Client
s3 = boto3.client("s3")

def get_top_level_folders(bucket):
    """Fetch top-level folders from the root of the S3 bucket"""
    result = s3.list_objects_v2(Bucket=bucket, Delimiter='/')
    return [prefix['Prefix'].strip("/") for prefix in result.get('CommonPrefixes', [])]

def get_processor(folder_name):
    """Return processor class based on folder name"""
    return PROCESSOR_MAPPING.get(folder_name)

def run_etl_on_folder(folder):
    input_prefix = f"{folder}/"
    output_prefix = f"processed/{folder}/"
    log_prefix = f"processed/logs/{folder}/"
    log_key = f"{log_prefix}log.json"  # 🔑 This is the single JSON file for this folder

    # print(f"input_prefix: {input_prefix}")
    # print(f"output_prefix: {output_prefix}")
    # print(f"log_prefix: {log_prefix}")
    # print(f"log_key: {log_key}")
    # Get all CSV files
    csv_files = list_objects_by_suffix(BUCKET_NAME, input_prefix, '.csv')
    # print(f"Found {len(csv_files)} files in {input_prefix}")
    print(f"folder: {folder}")
    processor_class = get_processor(folder)
    print("Processing files: ", processor_class)
    if not processor_class:
        write_json_log(BUCKET_NAME, log_key, {"level": "warning", "message": f"No processor found for folder: {folder}"})
        return
    #
    for key in csv_files:
        process_file(processor_class, BUCKET_NAME, key, output_prefix, log_key)

def main():
    folders = get_top_level_folders(BUCKET_NAME)
    # print(folders)
    for folder in folders:
        run_etl_on_folder(folder)

if __name__ == "__main__":
    main()
