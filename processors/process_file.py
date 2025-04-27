import pandas as pd
from datetime import datetime
from utils.s3_handler import read_csv_from_s3, write_parquet_to_s3, write_json_to_s3, append_json_log_to_s3
from processors.auxiliary import AuxiliaryProcessor
from processors.vehicle import VehicleProcessor
from processors.battery1 import Battery1Processor
from pathlib import Path

# # Add more processors as needed
# def get_processor(subfolder, df):
#     if 'auxiliary' in subfolder.lower():
#         return AuxiliaryProcessor(df)
#     elif 'vehicle' in subfolder.lower():
#         return VehicleProcessor(df)
#     elif 'battery1' in subfolder.lower():
#         return Battery1Processor(df)
#     else:
#         return None

def process_file(processor_class,bucket, key, session_name,output_prefix, log_prefix):
    try:
        df = read_csv_from_s3(bucket, key)
        # print(f"info:{df}")
        original_count = len(df)
        # print(f"Original count: {original_count}")
        # Determine which processor to use
        # subfolder = output_prefix.split('/')[1] if '/' in key else ''
        subfolder = Path(output_prefix).parent.name
        processor = processor_class()
        # print(processor)

        if processor is None:
            print(f"No processor found for subfolder: {subfolder}. Skipping file: {key}")
            return

        cleaned_df = processor.clean_data(df,key)
        cleaned_df['session'] = session_name
        cleaned_count = len(cleaned_df)
        print(f"Processed {cleaned_count} records")
        # Save cleaned file
        filename = key.split("/")[-1]
        parquet_filename = filename.replace(".csv", ".parquet")
        output_key = f"{output_prefix}{parquet_filename}"
        print(f"Writing {cleaned_count} records to {parquet_filename}")
        write_parquet_to_s3(cleaned_df, bucket, output_key)

        # Save log
        log_data = {
            "timestamp": datetime.now().isoformat(),
            "file": key,
            "folder": subfolder,
            "raw_rows": original_count,
            "cleaned_rows": cleaned_count,
            "output_key": output_key
        }
        log_key = f"{log_prefix}{subfolder}.json"
        print(f"Writing log to {log_key}")
        # # write_json_to_s3(bucket, log_key,log_data)
        # append_json_log_to_s3(bucket, log_key, log_data)


    except Exception as e:
        print(f"Error processing file {key}: {str(e)}")
