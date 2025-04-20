from utils.cleaning_utils import *
from utils.s3_handler import read_csv_from_s3, write_csv_to_s3, write_json_to_s3
from datetime import datetime
from schema_registry import SCHEMAS


class BaseProcessor:
    def process_file(self, bucket_name, input_key, output_prefix, log_path):
        df = read_csv_from_s3(bucket_name, input_key)
        original_count = len(df)

        # Extract module name
        file_name = input_key.split("/")[-1].lower()
        module_name = file_name.split("-")[0]

        # Validate schema
        expected_headers = SCHEMAS.get(module_name)
        if expected_headers:
            actual_headers = list(df.columns)
            if not compare_headers(expected_headers, actual_headers):
                raise ValueError(
                    f"Header mismatch for {module_name}.\nExpected: {expected_headers}\nFound: {actual_headers}")

        df = self.clean_data(df)
        cleaned_count = len(df)


        output_key = f"{output_prefix}{file_name}"
        write_csv_to_s3(df, bucket_name, output_key)

        ## Prepare log entry
        log_entry = {
            "file": input_key,
            "raw_rows_loaded": original_count,
            "rows_after_cleaning": cleaned_count,
            "saved_to": output_key,
            "processed_at": datetime.now().isoformat()
        }

        write_json_to_s3(bucket_name, log_path, log_entry)

    def clean_data(self, df):
        df = standardize_headers(df)
        df = drop_empty_rows_cols(df)
        df = drop_duplicate_rows(df)
        df = remove_null_timestamps(df)
        df = parse_and_set_session_column(df)
        df = convert_numeric_columns(df)
        return df
