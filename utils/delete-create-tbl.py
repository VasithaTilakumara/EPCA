import boto3
from botocore.exceptions import ClientError

# Define your parameters
glue = boto3.client('glue')
bucket_name = "s40334577-epca-bucket"
database_name = 's40334577-epca-db'
table_name = 'vehicle'
s3_location = f"s3://{bucket_name}/processed/{table_name}/"

# Example schema
columns = [
  {"Name": "datetime", "Type": "timestamp"},
  {"Name": "plc_hb", "Type": "double"},
  {"Name": "24volt", "Type": "int"},
  {"Name": "vcumode", "Type": "double"},
  {"Name": "vehstate", "Type": "double"},
  {"Name": "whlspd", "Type": "double"},
  {"Name": "uptime", "Type": "string"},
  {"Name": "downtime", "Type": "string"},
  {"Name": "gearchngetime", "Type": "string"},
  {"Name": "uptime_ms", "Type": "int"},
  {"Name": "downtime_ms", "Type": "int"},
  {"Name": "gearchngetime_ms", "Type": "int"},
  {"Name": "bodypos", "Type": "double"},
  {"Name": "throttle", "Type": "double"},
  {"Name": "bodyup", "Type": "double"},
  {"Name": "brakeairpres", "Type": "double"},
  {"Name": "brakeoiltemp", "Type": "double"},
  {"Name": "downshifton", "Type": "double"},
  {"Name": "hoistposave", "Type": "double"},
  {"Name": "keyaccessvolt", "Type": "double"},
  {"Name": "ignition", "Type": "double"},
  {"Name": "parkbrakeleft", "Type": "double"},
  {"Name": "parkbrakeright", "Type": "double"},
  {"Name": "retarder", "Type": "double"},
  {"Name": "flstrut", "Type": "double"},
  {"Name": "frstrut", "Type": "double"},
  {"Name": "rlstrut", "Type": "double"},
  {"Name": "rrstrut", "Type": "double"},
  {"Name": "gearreq", "Type": "double"},
  {"Name": "gearact", "Type": "double"},
  {"Name": "upshifton", "Type": "double"},
  {"Name": "wspeedleft", "Type": "double"},
  {"Name": "wspeedright", "Type": "double"},
  {"Name": "pdpposconclos", "Type": "double"},
  {"Name": "pdpnegconclos", "Type": "double"},
  {"Name": "pdpchilconclos", "Type": "double"},
  {"Name": "pdpdrive1contclos", "Type": "double"},
  {"Name": "pdpdrive2contclos", "Type": "double"},
  {"Name": "pdpaux1contclos", "Type": "double"},
  {"Name": "chillconn", "Type": "double"},
  {"Name": "driveconn", "Type": "double"},
  {"Name": "auxconn", "Type": "double"},
  {"Name": "pdpposconn", "Type": "double"},
  {"Name": "pdpnegconn", "Type": "double"},
  {"Name": "pdpdriveconn", "Type": "double"},
  {"Name": "pdpauxconn", "Type": "double"},
  {"Name": "pdpchillconn", "Type": "double"},
  {"Name": "pdpdcdcconn", "Type": "double"},
  {"Name": "pressval", "Type": "double"},
  {"Name": "tempval", "Type": "double"},
  {"Name": "lvvolt", "Type": "double"},
  {"Name": "procestemp", "Type": "double"},
  {"Name": "pcbtemp", "Type": "double"},
  {"Name": "vibval", "Type": "double"},
  {"Name": "g1outvolt", "Type": "double"},
  {"Name": "g2outvolt", "Type": "double"},
  {"Name": "g3goutvolt", "Type": "double"},
  {"Name": "transdrainon", "Type": "double"},
  {"Name": "batton", "Type": "double"},
  {"Name": "dcdcon", "Type": "double"},
  {"Name": "driveon", "Type": "double"},
  {"Name": "auxon", "Type": "double"},
  {"Name": "incli_x_raw", "Type": "double"},
  {"Name": "incli_y_raw", "Type": "double"},
  {"Name": "incli_x", "Type": "double"},
  {"Name": "incli_y", "Type": "double"},
  {"Name": "can0errtx", "Type": "double"},
  {"Name": "can0errrx", "Type": "double"},
  {"Name": "can0busload", "Type": "double"},
  {"Name": "can0busstate", "Type": "double"},
  {"Name": "can0messcount", "Type": "double"},
  {"Name": "can1errtx", "Type": "double"},
  {"Name": "can1errrx", "Type": "double"},
  {"Name": "can1busload", "Type": "double"},
  {"Name": "can1busstate", "Type": "double"},
  {"Name": "can1messcount", "Type": "double"},
  {"Name": "can2errtx", "Type": "double"},
  {"Name": "can2errrx", "Type": "double"},
  {"Name": "can2busload", "Type": "double"},
  {"Name": "can2busstate", "Type": "double"},
  {"Name": "can2messcount", "Type": "double"},
  {"Name": "can3errtx", "Type": "double"},
  {"Name": "can3errrx", "Type": "double"},
  {"Name": "can3busload", "Type": "double"},
  {"Name": "can3busstate", "Type": "double"},
  {"Name": "can3messcount", "Type": "double"},
  {"Name": "rpmbefore", "Type": "double"},
  {"Name": "rpmafter", "Type": "double"},
  {"Name": "gearchanged", "Type": "double"},
  {"Name": "rpminit", "Type": "double"},
  {"Name": "source_file", "Type": "string"},
  {"Name": "shutdownsafe", "Type": "int"},
  {"Name": "fault", "Type": "string"}
]

partition_keys = [
    {"Name": "session", "Type": "string"}
]

def delete_table_if_exists():
    try:
        glue.delete_table(DatabaseName=database_name, Name=table_name)
        print(f"Deleted table: {table_name}")
    except ClientError as e:
        if e.response['Error']['Code'] == 'EntityNotFoundException':
            print(f"Table {table_name} does not exist, nothing to delete.")
        else:
            raise

def create_table():
    response = glue.create_table(
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
                    'Parameters': {}
                }
            },
            'PartitionKeys': partition_keys,
            'TableType': 'EXTERNAL_TABLE',
            'Parameters': {
                'classification': 'parquet',
                'compressionType': 'none',
                'typeOfData': 'file'
            }
        }
    )
    print(f"Created table: {table_name}")

# Run the operations
delete_table_if_exists()
create_table()
