import boto3
import pandas as pd
import io
from utils.cleaning_utils import *

from utils.s3_handler import read_csv_from_s3, read_parquet_from_s3

# # --- AWS S3 Configuration ---
BUCKET_NAME = "s40334577-epca-bucket"
file_path = "processed/auxiliary/Auxiliary-2025-2-15-14-47-15.parquet"
# processed_file_path = "processed/vehicle/Vehicle.parquet"
#
# # Initialize S3 client
s3 = boto3.client("s3")
#
# file = read_csv_from_s3(BUCKET_NAME,file_path)
processed = read_parquet_from_s3(BUCKET_NAME,file_path)
print(processed.dtypes)
# df = pd.read_parquet('Vehicle.parquet')
# print(df.dtypes)