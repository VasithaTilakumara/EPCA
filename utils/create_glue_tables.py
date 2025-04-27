import json

import boto3

# --- Configuration ---
bucket_name = "s40334577-epca-bucket"
database_name = "s40334577-epca-db"
s3_base_path = f"s3://{bucket_name}/processed/"

# --- Type Mapping from Pandas to Glue ---
type_mapping = {
    "int64": "int",
    "float64": "double",
    "object": "string",
    "datetime64[ns]": "timestamp"
}

# --- Load schema definitions ---
from schema_registry import SCHEMAS

# Load your JSON schema once
with open("utils/processed_files_scheme.json", "r") as f:
    SCHEMA_TYPES = json.load(f)

# --- Convert Pandas-style schema to Glue format ---
def convert_schema(pandas_schema: dict) -> list:
    glue_columns = []
    for col, dtype in pandas_schema.items():
        glue_type = type_mapping.get(dtype, "string")  # Default to string if unknown type
        glue_columns.append({"Name": col, "Type": glue_type})
    return glue_columns

# --- Create Glue client ---
glue = boto3.client("glue")

# --- Ensure Database Exists ---
def create_database_if_not_exists(db_name):
    try:
        glue.get_database(Name=db_name)
        print(f"✅ Database '{db_name}' already exists.")
    except glue.exceptions.EntityNotFoundException:
        glue.create_database(DatabaseInput={"Name": db_name})
        print(f"📁 Created database: {db_name}")

# --- Create Glue Table from Schema ---
def create_glue_table(table_name, schema_dict):
    s3_location = f"{s3_base_path}{table_name.replace('_processed', '')}/"
    columns = convert_schema(schema_dict)
    print(columns)
    try:
        glue.create_table(
            DatabaseName=database_name,
            TableInput={
                'Name': table_name,
                'StorageDescriptor': {
                    'Columns': columns,
                    'Location': s3_location,
                    'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
                    'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
                    'SerdeInfo': {
                        'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
                        'Parameters': {'serialization.format': '1'}
                    }
                },
                'TableType': 'EXTERNAL_TABLE',
                'Parameters': {'classification': 'parquet'}
            }
        )
        print(f"✅ Table created: {table_name}")
    except glue.exceptions.AlreadyExistsException:
        print(f"⚠️ Table already exists: {table_name}")

# --- Main Execution ---
def setup_all_tables():
    create_database_if_not_exists(database_name)

    for table_name, schema in SCHEMAS.items():
        print(f"table:{table_name}")
        print(f"schema:{schema}")
        # module_schema = SCHEMA_TYPES[f"processed/{table_name}"]
        # print(f"module_schema:{module_schema}")
        if "_processed" in table_name:
            create_glue_table(table_name, schema)



# Run
if __name__ == "__main__":
    setup_all_tables()
