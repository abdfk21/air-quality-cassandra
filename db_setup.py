"""
db_setup.py
-----------
Initializes Apache Cassandra keyspace and table schema for Big Data Analytics,
and loads the generated time-series air quality telemetry dataset.

Key Design Decisions:
- Keyspace: bda_air_quality (SimpleStrategy, RF=1 for local development)
- Primary Key: (city, recorded_at, station_id)
  * Partition Key: city (localizes all telemetry for a city on the same Cassandra node)
  * Clustering Keys: recorded_at DESC, station_id ASC (on-disk sorting: newest first,
    station_id breaks timestamp collisions)
"""

import argparse
import csv
import datetime
import os
import sys
import time
from typing import Optional

# Python 3.12+ compatibility fix for cassandra-driver asyncore dependency
try:
    import asyncore
except ImportError:
    try:
        import pyasyncore as asyncore
        sys.modules['asyncore'] = asyncore
    except ImportError:
        pass

try:
    from cassandra.cluster import Cluster, Session
    from cassandra.auth import PlainTextAuthProvider
    from cassandra.concurrent import execute_concurrent_with_args
    from cassandra.query import BatchStatement, ConsistencyLevel
except ImportError as e:
    print(f"[!] Missing required package or dependency: {e}")
    print("    Please run: pip install cassandra-driver pyasyncore pyasynchat")
    sys.exit(1)



KEYSPACE_NAME = "bda_air_quality"
TABLE_NAME = "city_air_quality"


def get_cassandra_connection(
    host: str = "127.0.0.1",
    port: int = 9042,
    keyspace: Optional[str] = None,
    astra_bundle: Optional[str] = None,
    astra_token: Optional[str] = None,
    max_retries: int = 15,
    retry_interval: int = 4,
) -> tuple[Cluster, Session]:
    """Establishes connection to local Apache Cassandra or DataStax Astra DB with retry logic."""
    if astra_bundle and astra_token:
        print(f"[*] Connecting to DataStax Astra DB using secure bundle: {astra_bundle}...")
        cloud_config = {'secure_connect_bundle': astra_bundle}
        auth_provider = PlainTextAuthProvider('token', astra_token)
        cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
        session = cluster.connect(keyspace)
        return cluster, session

    print(f"[*] Connecting to local Apache Cassandra at {host}:{port}...")
    for attempt in range(1, max_retries + 1):
        try:
            cluster = Cluster([host], port=port, connect_timeout=10)
            session = cluster.connect()
            if keyspace:
                session.set_keyspace(keyspace)
            print(f"[+] Successfully connected to Cassandra cluster: '{cluster.metadata.cluster_name}'")
            return cluster, session
        except Exception as e:
            if attempt < max_retries:
                print(f"[~] Cassandra is starting up (Attempt {attempt}/{max_retries}). Retrying in {retry_interval}s... ({e})")
                time.sleep(retry_interval)
            else:
                print(f"[!] Failed to connect to Cassandra after {max_retries} attempts.")
                print("    Ensure Cassandra is running: 'docker compose up -d' or check container logs.")
                raise e


def create_schema(session: Session, is_astra: bool = False) -> None:
    """Creates the keyspace and wide-column table optimized for time-series range queries."""
    if not is_astra:
        print(f"[*] Creating Keyspace '{KEYSPACE_NAME}' if not exists...")
        keyspace_cql = f"""
        CREATE KEYSPACE IF NOT EXISTS {KEYSPACE_NAME}
        WITH replication = {{
            'class': 'SimpleStrategy',
            'replication_factor': 1
        }};
        """
        try:
            session.execute(keyspace_cql)
        except Exception as e:
            print(f"[~] Note: Could not execute CREATE KEYSPACE ({e}). Proceeding...")
    else:
        print(f"[*] Using managed Astra DB Keyspace '{KEYSPACE_NAME}'...")

    try:
        session.set_keyspace(KEYSPACE_NAME)
    except Exception as e:
        print(f"[~] Keyspace notice: {e}")

    print(f"[*] Creating Table '{TABLE_NAME}' with Time-Series Clustering...")
    table_cql = f"""
    CREATE TABLE IF NOT EXISTS {KEYSPACE_NAME}.{TABLE_NAME} (
        city text,
        recorded_at timestamp,
        station_id text,
        pm2_5 double,
        pm10 double,
        no2 double,
        co double,
        so2 double,
        aqi int,
        spike_alert boolean,
        PRIMARY KEY (city, recorded_at, station_id)
    ) WITH CLUSTERING ORDER BY (recorded_at DESC, station_id ASC);
    """
    session.execute(table_cql)
    print(f"[+] Schema initialized successfully: {KEYSPACE_NAME}.{TABLE_NAME}")



def parse_timestamp(ts_str: str) -> datetime.datetime:
    """Parses standard ISO/SQL timestamp string into Python datetime."""
    return datetime.datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")


def populate_data_from_csv(session: Session, csv_path: str = "air_quality_data.csv", batch_size: int = 400, concurrency: int = 25) -> None:
    """
    Reads records from CSV and inserts them concurrently into Cassandra using Prepared Statements.
    Utilizes cassandra.concurrent.execute_concurrent_with_args for optimal throughput without
    causing coordinator node heap pressure (anti-pattern of monolithic multi-partition batches).
    """
    if not os.path.exists(csv_path):
        print(f"[!] CSV dataset file '{csv_path}' not found.")
        print(f"    Please run 'python generate_dataset.py' first!")
        sys.exit(1)

    print(f"[*] Preparing CQL insert statement...")
    insert_cql = f"""
    INSERT INTO {KEYSPACE_NAME}.{TABLE_NAME} (
        city, recorded_at, station_id, pm2_5, pm10, no2, co, so2, aqi, spike_alert
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    """
    prepared = session.prepare(insert_cql)
    prepared.consistency_level = ConsistencyLevel.ONE

    print(f"[*] Reading telemetry records from '{csv_path}'...")
    all_parameters = []
    with open(csv_path, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            all_parameters.append((
                row["city"],
                parse_timestamp(row["recorded_at"]),
                row["station_id"],
                float(row["pm2_5"]),
                float(row["pm10"]),
                float(row["no2"]),
                float(row["co"]),
                float(row["so2"]),
                int(row["aqi"]),
                row["spike_alert"].strip().lower() in ("true", "1", "yes"),
            ))

    total_records = len(all_parameters)
    print(f"[*] Ingesting {total_records:,} records into Cassandra in chunks of {batch_size} (concurrency={concurrency})...")

    start_time = time.time()
    inserted_count = 0

    for i in range(0, total_records, batch_size):
        chunk = all_parameters[i:i + batch_size]
        results = execute_concurrent_with_args(session, prepared, chunk, concurrency=concurrency)

        
        # Verify success
        for success, result_or_exc in results:
            if not success:
                print(f"[!] Failed to insert record: {result_or_exc}")
            else:
                inserted_count += 1

        sys.stdout.write(f"\r    Progress: {inserted_count:,}/{total_records:,} rows inserted ({(inserted_count / total_records) * 100:.1f}%)")
        sys.stdout.flush()

    elapsed = time.time() - start_time
    throughput = inserted_count / elapsed if elapsed > 0 else 0
    print(f"\n[+] Data seeding complete: {inserted_count:,} records inserted in {elapsed:.2f}s ({throughput:.1f} writes/sec).")


def main():
    parser = argparse.ArgumentParser(description="Set up Cassandra schema and seed air quality dataset.")
    parser.add_argument("--host", default="127.0.0.1", help="Cassandra contact point (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=9042, help="Cassandra native transport port (default: 9042)")
    parser.add_argument("--csv", default="air_quality_data.csv", help="Path to input CSV file")
    parser.add_argument("--astra-bundle", default=os.getenv("ASTRA_BUNDLE"), help="Path to Astra DB secure connect zip")
    parser.add_argument("--astra-token", default=os.getenv("ASTRA_TOKEN"), help="Astra DB client application token")
    args = parser.parse_args()

    is_astra = bool(args.astra_bundle and args.astra_token)
    cluster, session = get_cassandra_connection(
        host=args.host,
        port=args.port,
        keyspace=KEYSPACE_NAME if is_astra else None,
        astra_bundle=args.astra_bundle,
        astra_token=args.astra_token,
    )

    try:
        create_schema(session, is_astra=is_astra)
        populate_data_from_csv(session, csv_path=args.csv, concurrency=20 if is_astra else 50)


    finally:
        session.shutdown()
        cluster.shutdown()
        print("[*] Connection closed gracefully.")


if __name__ == "__main__":
    main()
