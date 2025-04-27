import boto3
import pandas as pd
import io
import random

# --- AWS S3 Configuration ---
BUCKET_NAME = "s40334577-epca-bucket" # ✅ Update this
FOLDERS = [
    "auxiliary",
    "battery1",
    "vehicle",
    "drive1",
    "drive2",
    "chiller1",
    "ccl1",
    "chargecontroller1", "dcdc"
]

# --- AWS S3 client
s3 = boto3.client('s3')


def list_files_by_extension(bucket, prefix, extension=".csv"):
    """List files under a folder with given extension (.csv or .parquet)"""
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    return [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith(extension)]


def sample_files(file_keys, n_samples=100):
    """Randomly sample up to n files"""
    return random.sample(file_keys, min(len(file_keys), n_samples))


def read_csv_from_s3(bucket, key):
    """Read single CSV from S3"""
    obj = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(io.BytesIO(obj['Body'].read()), on_bad_lines="skip", low_memory=False)


# --- MAIN
all_dataframes = []
schema_summary = {}

for folder in FOLDERS:
    print(f"\n📂 Sampling from folder: {folder}")
    csv_files = list_files_by_extension(BUCKET_NAME, folder)

    if not csv_files:
        print("⚠️ No CSV files found.")
        continue

    sampled_files = sample_files(csv_files, n_samples=5)
    print(f"Selected {len(sampled_files)} files: {sampled_files}")

    folder_dfs = []
    for key in sampled_files:
        try:
            df = read_csv_from_s3(BUCKET_NAME, key)
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
with open("sampled_schema_summary.json", "w") as f:
    json.dump(schema_summary, f, indent=4)
    print("\n✅ Saved schema summary to 'sampled_schema_summary.json'")