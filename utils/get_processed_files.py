# import boto3
# import pandas as pd
# import io
#
#
# # --- AWS S3 Configuration ---
# BUCKET_NAME = "s40334577-epca-bucket"
# PREFIXES = [
#     "processed/battery1", "processed/battery1_input", "processed/chargecontroller1",
#     "processed/chiller1", "processed/chiller1_Input", "processed/ccl1", "processed/ccl1_input",
#     "processed/dcdc", "processed/drive1", "processed/drive2", "processed/vehicle", "processed/auxiliary",
#     "processed/auxiliary_input", "processed/drive1_input", "processed/drive2_input"
# ]
#
# # Initialize S3 client
# s3 = boto3.client("s3")
#
# def list_parquet_files(bucket, prefix):
#     response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
#     return [obj["Key"] for obj in response.get("Contents", []) if obj["Key"].endswith(".parquet")]
#
# def read_parquet_from_s3(bucket, key):
#     obj = s3.get_object(Bucket=bucket, Key=key)
#     return pd.read_parquet(io.BytesIO(obj["Body"].read()))
#
# # --- Read One File per Folder and Display Schema ---
# for prefix in PREFIXES:
#     print(f"\n🔍 Inspecting: {prefix}")
#     parquet_files = list_parquet_files(BUCKET_NAME, prefix)
#     if not parquet_files:
#         print("⚠️ No Parquet files found.")
#         continue
#
#     try:
#         # if prefix == "processed/vehicle":
#         df = read_parquet_from_s3(BUCKET_NAME, parquet_files[0])
#         # type = df.dtypes
#         print(type)
#         filename = prefix.split("/")[-1]
#         with open(f"processed_dtypes/{filename}.txt", "w", encoding="utf-8") as f:
#             for col, dtype in df.dtypes.items():
#                 f.write(f"\"{col}\": \"{dtype}\",\n")
#     except Exception as e:
#         print(f"❌ Failed to read {parquet_files[0]}: {e}")


import boto3
import pandas as pd
import io
import random

from utils.s3_handler import read_parquet_from_s3

# --- AWS S3 Configuration ---
BUCKET_NAME = "s40334577-epca-bucket" # ✅ Update this
FOLDERS = [
    "processed/battery1", "processed/battery1_input", "processed/chargecontroller1",
    "processed/chiller1", "processed/chiller1_Input", "processed/ccl1", "processed/ccl1_input",
    "processed/dcdc", "processed/drive1", "processed/drive2", "processed/vehicle", "processed/auxiliary",
    "processed/auxiliary_input", "processed/drive1_input", "processed/drive2_input"
]

# --- AWS S3 client
s3 = boto3.client('s3')


def list_files_by_extension(bucket, prefix, extension=".parquet"):
    """List files under a folder with given extension (.csv or .parquet)"""
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    return [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith(extension)]


def sample_files(file_keys, n_samples=5):
    """Randomly sample up to n files"""
    return random.sample(file_keys, min(len(file_keys), n_samples))


# def read_csv_from_s3(bucket, key):
# #     """Read single CSV from S3"""
# #     obj = s3.get_object(Bucket=bucket, Key=key)
# #     return pd.read_csv(io.BytesIO(obj['Body'].read()), on_bad_lines="skip", low_memory=False)


# --- MAIN
all_dataframes = []
schema_summary = {}

for folder in FOLDERS:
    print(f"\n📂 Sampling from folder: {folder}")
    print()
    csv_files = list_files_by_extension(BUCKET_NAME, folder)
    print(csv_files)
    if not csv_files:
        print("⚠️ No CSV files found.")
        continue

    sampled_files = sample_files(csv_files, n_samples=5)
    print(f"Selected {len(sampled_files)} files: {sampled_files}")

    folder_dfs = []
    for key in sampled_files:
        try:
            df = read_parquet_from_s3(BUCKET_NAME, key)
            df['source_file'] = key  # Optionally, tag source file
            folder_dfs.append(df)
        except Exception as e:
            print(f"❌ Failed to read {key}: {e}")

    if folder_dfs:
        combined_folder_df = pd.concat(folder_dfs, ignore_index=True)
        all_dataframes.append((folder, combined_folder_df))

        # --- 📋 Capture dtypes into dictionary
        dtype_dict = {col: str(dtype) for col, dtype in combined_folder_df.dtypes.items()}
        schema_summary[folder] = dtype_dict

# --- 📈 Analyze Sampled Data
# for folder, df in all_dataframes:
#     print(f"\n--- 📊 Folder: {folder} ---")
#     print("🔹 Shape:", df.shape)
#     print("🔹 Columns:", df.columns.tolist())
#     print("🔹 Data Types:")
#     print(df.dtypes)
#     print("🔹 Missing Values:")
#     print(df.isnull().sum())


# --- 🔥 Final Schema Summary
print("\n======= 📜 DATA TYPE SUMMARY =======\n")
for folder, schema in schema_summary.items():
    print(f"\n📂 Folder: {folder}")
    for col, dtype in schema.items():
        print(f"  {col}: {dtype}")

# (Optional) Save the schema_summary to a local JSON file if you want
import json
with open("processed_files_scheme.json", "w") as f:
    json.dump(schema_summary, f, indent=4)
    print("\n✅ Saved schema summary to 'sampled_schema_summary.json'")
