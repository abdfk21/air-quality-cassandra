"""
queries.py
----------
Reusable Cassandra CQL query functions for Big Data Analytics on
urban air quality telemetry (aligned with UN SDG 11).

Demonstrates:
1. Latest readings lookup (Leverages clustering key DESC order).
2. Time-window slice query (Clustering column range scan).
3. Pollution spike identification (In-partition filtering).
4. Aggregate summary metrics (CQL aggregates + daily aggregations).
"""

import time
import datetime
from typing import Optional, Tuple, Dict, Any, List
import pandas as pd

KEYSPACE_NAME = "bda_air_quality"
TABLE_NAME = "city_air_quality"


def time_query(func):
    """Decorator to measure execution time of CQL queries in milliseconds."""
    def wrapper(*args, **kwargs):
        start = time.perf_counter()
        result, cql_stmt = func(*args, **kwargs)
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        return result, cql_stmt, elapsed_ms
    return wrapper


@time_query
def get_latest_readings(session, city: str, limit: int = 20) -> Tuple[pd.DataFrame, str]:
    """
    Query 1: Retrieve the latest N readings for a selected city.
    
    Why Cassandra is fast here:
    Because the table is clustered by (recorded_at DESC), Cassandra locates
    the partition for 'city' and reads the top rows sequentially from the top
    of the SSTable/Memtable on disk. No sorting in memory is required!
    """
    cql = f"""
    SELECT city, recorded_at, station_id, pm2_5, pm10, no2, co, so2, aqi, spike_alert
    FROM {KEYSPACE_NAME}.{TABLE_NAME}
    WHERE city = %s
    LIMIT %s;
    """
    cql_display = f"SELECT * FROM {KEYSPACE_NAME}.{TABLE_NAME} WHERE city = '{city}' LIMIT {limit};"
    rows = session.execute(cql, (city, limit))
    df = pd.DataFrame(list(rows))
    return df, cql_display


@time_query
def get_readings_by_time_window(
    session,
    city: str,
    start_time: datetime.datetime,
    end_time: datetime.datetime
) -> Tuple[pd.DataFrame, str]:
    """
    Query 2: Retrieve historical readings within a specific time window.
    
    Why Cassandra is fast here:
    Cassandra performs a contiguous range scan on the clustering column
    'recorded_at' within the single node partition.
    """
    cql = f"""
    SELECT city, recorded_at, station_id, pm2_5, pm10, no2, co, so2, aqi, spike_alert
    FROM {KEYSPACE_NAME}.{TABLE_NAME}
    WHERE city = %s
      AND recorded_at >= %s
      AND recorded_at <= %s;
    """
    cql_display = (
        f"SELECT * FROM {KEYSPACE_NAME}.{TABLE_NAME}\n"
        f"WHERE city = '{city}'\n"
        f"  AND recorded_at >= '{start_time.strftime('%Y-%m-%d %H:%M:%S')}'\n"
        f"  AND recorded_at <= '{end_time.strftime('%Y-%m-%d %H:%M:%S')}';"
    )
    rows = session.execute(cql, (city, start_time, end_time))
    df = pd.DataFrame(list(rows))
    return df, cql_display


@time_query
def get_pollution_spikes(
    session,
    city: str,
    start_time: Optional[datetime.datetime] = None,
    end_time: Optional[datetime.datetime] = None,
    aqi_threshold: int = 300,
    pm25_threshold: float = 250.0
) -> Tuple[pd.DataFrame, str]:
    """
    Query 3: Identify dangerous pollution spikes.
    
    Demonstrates:
    In Cassandra, filtering on non-clustering columns within a known partition
    (ALLOW FILTERING on a single partition) is safe because it does NOT trigger
    a full cluster scan. Alternatively, filtering can be verified against
    the pre-computed boolean column 'spike_alert'.
    """
    if start_time and end_time:
        cql = f"""
        SELECT city, recorded_at, station_id, pm2_5, pm10, no2, co, so2, aqi, spike_alert
        FROM {KEYSPACE_NAME}.{TABLE_NAME}
        WHERE city = %s
          AND recorded_at >= %s
          AND recorded_at <= %s
          AND spike_alert = true
        ALLOW FILTERING;
        """
        cql_display = (
            f"SELECT * FROM {KEYSPACE_NAME}.{TABLE_NAME}\n"
            f"WHERE city = '{city}'\n"
            f"  AND recorded_at >= '{start_time.strftime('%Y-%m-%d %H:%M:%S')}'\n"
            f"  AND recorded_at <= '{end_time.strftime('%Y-%m-%d %H:%M:%S')}'\n"
            f"  AND spike_alert = true\n"
            f"ALLOW FILTERING;"
        )
        rows = session.execute(cql, (city, start_time, end_time))
    else:
        cql = f"""
        SELECT city, recorded_at, station_id, pm2_5, pm10, no2, co, so2, aqi, spike_alert
        FROM {KEYSPACE_NAME}.{TABLE_NAME}
        WHERE city = %s
          AND spike_alert = true
        ALLOW FILTERING;
        """
        cql_display = (
            f"SELECT * FROM {KEYSPACE_NAME}.{TABLE_NAME}\n"
            f"WHERE city = '{city}'\n"
            f"  AND spike_alert = true\n"
            f"ALLOW FILTERING;"
        )
        rows = session.execute(cql, (city,))

    df = pd.DataFrame(list(rows))
    return df, cql_display


@time_query
def get_cql_aggregate_metrics(
    session,
    city: str,
    start_time: Optional[datetime.datetime] = None,
    end_time: Optional[datetime.datetime] = None
) -> Tuple[Dict[str, Any], str]:
    """
    Query 4A: Native Cassandra aggregate functions (min, max, avg, count).
    Cassandra supports built-in aggregation over a single partition.
    """
    if start_time and end_time:
        cql = f"""
        SELECT count(*), min(pm2_5) as min_pm25, max(pm2_5) as max_pm25, avg(pm2_5) as avg_pm25,
               min(pm10) as min_pm10, max(pm10) as max_pm10, avg(pm10) as avg_pm10,
               min(aqi) as min_aqi, max(aqi) as max_aqi, avg(aqi) as avg_aqi
        FROM {KEYSPACE_NAME}.{TABLE_NAME}
        WHERE city = %s
          AND recorded_at >= %s
          AND recorded_at <= %s;
        """
        cql_display = (
            f"SELECT count(*), min(pm2_5), max(pm2_5), avg(pm2_5), min(aqi), max(aqi), avg(aqi)\n"
            f"FROM {KEYSPACE_NAME}.{TABLE_NAME}\n"
            f"WHERE city = '{city}'\n"
            f"  AND recorded_at >= '{start_time.strftime('%Y-%m-%d %H:%M:%S')}'\n"
            f"  AND recorded_at <= '{end_time.strftime('%Y-%m-%d %H:%M:%S')}';"
        )
        row = session.execute(cql, (city, start_time, end_time)).one()
    else:
        cql = f"""
        SELECT count(*), min(pm2_5) as min_pm25, max(pm2_5) as max_pm25, avg(pm2_5) as avg_pm25,
               min(pm10) as min_pm10, max(pm10) as max_pm10, avg(pm10) as avg_pm10,
               min(aqi) as min_aqi, max(aqi) as max_aqi, avg(aqi) as avg_aqi
        FROM {KEYSPACE_NAME}.{TABLE_NAME}
        WHERE city = %s;
        """
        cql_display = (
            f"SELECT count(*), min(pm2_5), max(pm2_5), avg(pm2_5), min(aqi), max(aqi), avg(aqi)\n"
            f"FROM {KEYSPACE_NAME}.{TABLE_NAME}\n"
            f"WHERE city = '{city}';"
        )
        row = session.execute(cql, (city,)).one()

    stats = dict(row._asdict()) if row else {}
    return stats, cql_display


def calculate_daily_summary_stats(df: pd.DataFrame) -> pd.DataFrame:
    """
    Query 4B: Computes daily high, low, average and spike alert counts
    from partition time-series data.
    """
    if df.empty or "recorded_at" not in df.columns:
        return pd.DataFrame()

    temp_df = df.copy()
    temp_df["date"] = pd.to_datetime(temp_df["recorded_at"]).dt.date

    daily = temp_df.groupby("date").agg(
        readings_count=("aqi", "count"),
        avg_aqi=("aqi", lambda x: round(x.mean(), 1)),
        min_aqi=("aqi", "min"),
        max_aqi=("aqi", "max"),
        avg_pm25=("pm2_5", lambda x: round(x.mean(), 2)),
        max_pm25=("pm2_5", "max"),
        avg_pm10=("pm10", lambda x: round(x.mean(), 2)),
        max_pm10=("pm10", "max"),
        spike_alerts=("spike_alert", lambda x: int(x.sum())),
    ).reset_index().sort_values(by="date", ascending=False)

    return daily


def get_available_cities(session) -> List[str]:
    """Retrieves distinct cities available in the keyspace."""
    try:
        cql = f"SELECT DISTINCT city FROM {KEYSPACE_NAME}.{TABLE_NAME};"
        rows = session.execute(cql)
        cities = [r.city for r in rows if r.city]
        return sorted(cities) if cities else ["Delhi", "Mumbai", "Bengaluru", "Hyderabad", "Kolkata"]
    except Exception:
        return ["Delhi", "Mumbai", "Bengaluru", "Hyderabad", "Kolkata"]


if __name__ == "__main__":
    print("=" * 70)
    print("  Apache Cassandra Analytical Query Library (queries.py)")
    print("=" * 70)
    print("This module provides reusable CQL query functions:")
    print("  1. get_latest_readings(session, city, limit=20)")
    print("  2. get_readings_by_time_window(session, city, start_time, end_time)")
    print("  3. get_pollution_spikes(session, city, start_time, end_time, ...)")
    print("  4. get_cql_aggregate_metrics(session, city, ...)")
    print("  5. calculate_daily_summary_stats(df)")
    print("\nTo run interactive queries on Cassandra, launch the Streamlit app:")
    print("  streamlit run app.py")
    print("=" * 70)

