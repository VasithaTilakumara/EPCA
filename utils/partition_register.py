from pyathena import connect

def extract_session_from_filename(filename):
    # try:
        parts = filename.split("-")
        print(f"parts: {parts}")
        # Expected: ['Auxiliary', '2024', '8', '28', '14', '12', '58']
        if len(parts) >= 7:
            year, month, day, hour, minute, second = parts[1:7]
            # year, month, day = parts[1:3]
            # Ensure all values are 2-digit padded
            # session = f"{year}-{int(month):02d}-{int(day):02d}-{int(hour):02d}-{int(minute):02d}-{int(second):02d}"
            session = f"{year}-{int(month):02d}-{int(day):02d}"
            return session
        # else:
        #     # Fallback: If structure is weird, return a generic session
        #     from datetime import datetime
        #     return datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    # except Exception:
        # from datetime import datetime
        # return datetime.now().strftime("%Y-%m-%d-%H-%M-%S")


def extract_session_from_key(key):
    """Extract session from output key."""
    try:
        parts = key.split("/")
        session_folder = [p for p in parts if p.startswith("session=")]
        if session_folder:
            return session_folder[0].replace("session=", "")
        else:
            return None
    except Exception:
        return None

def register_partition_for_file(bucket, database, table, session):
    """Register a new partition in Athena after processing a file."""
    if not session:
        print("⚠️ No session extracted; skipping partition registration.")
        return

    print(f"🔄 Registering new partition for table: {table}, session: {session}...")

    conn = connect(
        s3_staging_dir=f"s3://{bucket}/athena-query-results/",
        region_name="ap-southeast-2",
        schema_name=database
    )

    cursor = conn.cursor()
    location = f"s3://{bucket}/processed/{table}/session={session}/"

    query = f"""
    ALTER TABLE `{database}`.`{table}`
    ADD IF NOT EXISTS PARTITION (session = '{session}')
    LOCATION '{location}'
    """

    try:
        cursor.execute(query)
        print(f"✅ Partition added: {table}, session={session}")
    except Exception as e:
        print(f"❌ Failed to register partition for {table}, session={session}: {e}")
    finally:
        cursor.close()
        conn.close()
