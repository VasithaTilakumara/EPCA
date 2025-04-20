from utils.cleaning_utils import *
from utils.s3_handler import read_csv_from_s3, write_csv_to_s3, write_json_to_s3
from datetime import datetime
from schema_registry import SCHEMAS

import logging
import os

# Create logs directory if not exist
os.makedirs("logs", exist_ok=True)

# Configure logging
logging.basicConfig(
    filename="logs/error_log.txt",         # Path to your local log file
    filemode="a",                          # Append mode
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO                     # Could also be DEBUG or ERROR
)

class BaseProcessor:

    def clean_data(self, df,input_key):
        # Extract module name
        file_name = input_key.split("/")[-1].lower()
        module_name = file_name.split("-")[0]

        # Validate schema
        expected_headers = SCHEMAS.get(module_name)
        if expected_headers:
            actual_headers = list(df.columns)
            if not compare_headers(expected_headers, actual_headers):
                raise ValueError(
                    f"Header mismatch for {module_name}.\nExpected: {expected_headers}\nFound: {actual_headers}",
                    logging.error(f"Header mismatch for {module_name}.\nExpected: {expected_headers}\nFound: {actual_headers}", exc_info=True))


        df = standardize_headers(df)
        df = drop_empty_rows_cols(df)
        df = drop_duplicate_rows(df)
        df = remove_null_timestamps(df)
        df = parse_and_set_session_column(df)
        df = convert_numeric_columns(df)
        return df




