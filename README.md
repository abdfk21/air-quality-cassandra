# 🌍 Time-Series Air Quality and Pollution Spike Analysis Using Apache Cassandra

[![UN SDG 11](https://img.shields.io/badge/UN%20SDG-11%3A%20Sustainable%20Cities-orange.svg)](https://sdgs.un.org/goals/goal11)
[![Cassandra](https://img.shields.io/badge/Database-Apache%20Cassandra%204.x-1f5582.svg)](https://cassandra.apache.org/)
[![Python](https://img.shields.io/badge/Python-3.9%20%7C%203.10%20%7C%203.11%20%7C%203.12%20%7C%203.13-blue.svg)](https://www.python.org/)
[![Streamlit](https://img.shields.io/badge/UI-Streamlit-red.svg)](https://streamlit.io/)
[![Plotly](https://img.shields.io/badge/Visuals-Plotly%20Express-purple.svg)](https://plotly.com/)

A Big Data Analytics (BDA) mini-project showcasing high-velocity time-series IoT sensor data ingestion, wide-column partitioned storage, range querying, and episodic pollution spike analysis using **Apache Cassandra** and an interactive **Streamlit** dashboard.

---

## 📌 1. Project Overview & Alignment with UN SDG 11

Urban air pollution is one of the most pressing environmental health challenges of the 21st century. Exposure to fine particulate matter ($PM_{2.5}$ and $PM_{10}$) leads to cardiovascular diseases, respiratory illness, and increased mortality.

This project directly aligns with **United Nations Sustainable Development Goal 11: Sustainable Cities and Communities**:
* **Target 11.6**: *"By 2030, reduce the adverse per capita environmental impact of cities, including by paying special attention to air quality and municipal and other waste management."*
* **Indicator 11.6.2**: *"Annual mean levels of fine particulate matter (e.g. $PM_{2.5}$ and $PM_{10}$) in cities (population weighted)."*

### Big Data Analytics (BDA) Relevance
1. **High Velocity**: Environmental monitoring stations emit telemetry every few seconds/minutes across hundreds of urban sensors.
2. **Write-Heavy Ingestion**: IoT databases must absorb continuous writes without lock contention or performance degradation.
3. **Time-Series Querying**: Dashboards demand fast time-slice lookups (e.g., *"show latest 20 readings"* or *"show pollution levels between 8 AM and 11 AM"*).

---

## 🏗️ 2. Architectural Comparison: Why Apache Cassandra?

| Dimension | Apache Cassandra (Our Choice) | Relational DB (MySQL / PostgreSQL) | Document Store (MongoDB) | Hadoop / HDFS / MapReduce |
| :--- | :--- | :--- | :--- | :--- |
| **Write Architecture** | **LSM-Tree** (Append-only CommitLog + Memtable + immutable SSTables) | B-Tree index updates in place (random disk I/O, lock contention) | B-Trees (WiredTiger) with document-level locking | Batch-oriented append; high latency for streaming point-writes |
| **Write Throughput** | **Hundreds of thousands of writes/sec** per node | Bottlenecks at high concurrency | High, but degrades under heavy indexing | High, but designed for large files, not individual sensor pings |
| **High Availability** | **Masterless (Peer-to-Peer)**; zero single point of failure | Active-Passive / Replication lag | Primary-Replica failover downtime | NameNode master failover complexity |
| **Time-Series Lookups** | **On-disk sorted clustering keys** ($O(1)$ seek to latest rows) | Requires index scans and explicit `ORDER BY` memory sorting | Requires in-memory sorting of document arrays | Slow (scans entire partitions or runs batch jobs) |
| **CAP Theorem** | **AP System** (High Availability & Partition Tolerance with Tunable Consistency) | **CA / CP System** (Sacrifices availability during network partitions) | **CP System** (Unavailable during primary elections) | Batch file system |

---

## 📐 3. Cassandra Data Modeling & Schema Design

In Apache Cassandra, data modeling follows the **Query-First Principle**: you design tables specifically around the queries your application will run.

### Keyspace Definition
```sql
CREATE KEYSPACE IF NOT EXISTS bda_air_quality
WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
};
```

### Table Schema: `city_air_quality`
```sql
CREATE TABLE IF NOT EXISTS bda_air_quality.city_air_quality (
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
```

### Anatomical Breakdown of the Primary Key
$$\text{PRIMARY KEY} = (\underbrace{\text{city}}_{\text{Partition Key}}, \; \underbrace{\text{recorded\_at}, \; \text{station\_id}}_{\text{Clustering Keys}})$$

1. **Partition Key (`city`)**:
   - Determines which cluster node holds the data using **Murmur3Partitioner** token hashing.
   - Collocates all observations for a given city on the same node, eliminating distributed network shuffles when filtering by city.
2. **Clustering Key 1 (`recorded_at DESC`)**:
   - Controls physical on-disk sort order inside the node's **SSTable**.
   - Because it is sorted `DESC`, the most recent readings are positioned at the very front of the partition on disk. Querying `LIMIT 20` performs a sequential disk read with **zero RAM sorting**.
3. **Clustering Key 2 (`station_id ASC`)**:
   - Disambiguates multiple sensor stations in the same city reporting telemetry at the exact same second, preventing row overwrite collisions.

---

## 📂 4. Project Repository Structure

```
BDA_project/
│
├── docker-compose.yml       # Cassandra 4.1 single-node container configuration
├── generate_dataset.py      # Synthetic time-series generator with realistic episodic spikes
├── db_setup.py              # Keyspace/table creation & concurrent prepared statement ingestion
├── queries.py               # Reusable CQL query library with execution-time metrics
├── app.py                   # Streamlit analytics dashboard with Plotly & CQL explorer
├── requirements.txt         # Python project dependencies
├── air_quality_data.csv     # Generated multi-city telemetry dataset (28,000+ rows)
└── README.md                # Comprehensive documentation, architecture & Viva Q&A
```

---

## 🚀 5. Quick-Start Step-by-Step Guide

### Step 1: Start Cassandra (Local Docker)
Make sure Docker Desktop is running, then execute:
```bash
docker compose up -d
```
Verify the container is healthy:
```bash
docker ps
```
*(Cassandra usually takes 20–45 seconds to initialize gossip protocols and open port 9042).*

### Step 2: Set Up Python Virtual Environment & Dependencies
```bash
# Create virtual environment
python -m venv .venv

# Activate virtual environment
# Windows (PowerShell):
.\.venv\Scripts\Activate.ps1
# Linux / macOS:
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### Step 3: Generate the Synthetic Air Quality Dataset
Generates 180 days of 2-hourly observations across 5 cities (Delhi, Mumbai, Bengaluru, Hyderabad, Kolkata) with realistic diurnal curves and episodic pollution spikes:
```bash
python generate_dataset.py
```
*Output: `air_quality_data.csv` with ~28,000+ records and ~3,800+ spike alerts.*

### Step 4: Initialize Schema and Ingest into Cassandra
Creates the keyspace, table, and executes concurrent prepared statements:
```bash
python db_setup.py
```
*(The script includes an automatic connection retry loop with progress reporting).*

### Step 5: Launch the Streamlit Dashboard
```bash
streamlit run app.py
```
Open your browser at `http://localhost:8501`.

---

## ☁️ 6. Alternative: Connecting to DataStax Astra DB (Free Cloud Cassandra)

If you do not want to run Docker locally, you can use **DataStax Astra DB** (a free-tier, serverless managed Cassandra):

1. Sign up for free at [astra.datastax.com](https://astra.datastax.com).
2. Create a Database named `bda_air_quality` with keyspace `bda_air_quality`.
3. In **Database Settings**, generate an **Application Token** (Role: *Database Administrator*). Copy the token string (starts with `AstraCS:...`).
4. In **Connect** -> **Python**, download the **Secure Connect Bundle** zip file (e.g. `secure-connect-bda-air-quality.zip`).
5. Seed the database using:
   ```bash
   python db_setup.py --astra-bundle path/to/secure-connect.zip --astra-token "AstraCS:..."
   ```

---

## 📊 7. Dashboard Features

1. **Top KPI Summary Cards**:
   - Current AQI with color-coded safety badges (Good, Satisfactory, Moderate, Poor, Very Poor, Severe).
   - Peak $PM_{2.5}$ and $PM_{10}$ recorded.
   - Total Spike Alert instances.
   - Active sensor station count.
   - **Cassandra Query Latency Tracker** (displaying sub-10ms query speeds).
2. **Interactive Plotly Visualizations**:
   - Continuous time-series line chart for $PM_{2.5}$ & $PM_{10}$ with a hazardous alert threshold line ($250\,\mu\text{g/m}^3$).
   - Gaseous co-pollutant trends ($NO_2$, $SO_2$, and $CO$).
3. **Spike Alert Incident Room**:
   - Scatter plot sizing bubbles by AQI severity.
   - Station-level breakdown of spike occurrences.
   - Detailed incident audit table.
4. **Daily Aggregate Rollups**:
   - Aggregated daily high, low, and average concentrations for municipal urban planning.
5. **Executed CQL & Raw Resultset**:
   - Displays the exact CQL query generated with live parameters.
   - Live execution time in milliseconds.
   - One-click CSV export button.
6. **Cassandra Architecture Deep-Dive Tab**:
   - Detailed breakdown of wide-column mechanics, LSM-trees, and UN SDG 11.6 alignment.

---

## 🎓 8. Sample Viva Voce / Exam Q&A

### Q1: What is the difference between a Partition Key and a Clustering Key in Apache Cassandra?
**Answer:**
* **Partition Key**: Determines **which physical node** in the Cassandra cluster stores the data. Cassandra hashes the partition key using the `Murmur3Partitioner` algorithm to generate a 64-bit token that maps to a specific node's token range.
* **Clustering Key**: Determines **how the data is physically ordered on disk** within that partition's SSTable. In our project, `city` is the partition key (collocating all sensor data for a city on one node), while `recorded_at DESC` and `station_id ASC` are clustering keys (sorting readings chronologically, newest first).

---

### Q2: Why is Apache Cassandra superior to MongoDB or Relational DBs for IoT time-series telemetry?
**Answer:**
IoT sensors produce continuous, high-volume append operations.
1. **LSM-Tree Storage Engine**: Cassandra never modifies data in-place on disk. Writes are appended sequentially to a `CommitLog` (for durability) and stored in an in-memory `Memtable`. This turns random disk I/O into sequential writes, giving Cassandra near-zero lock contention and throughput of 100,000+ writes/second per node.
2. **Relational DBs (B-Trees)**: RDBMS require updating B-Tree leaf nodes in place and acquiring row/table locks, causing severe disk thrashing and deadlocks under streaming sensor loads.
3. **MongoDB**: While fast, MongoDB uses document-level locking and updates B-Tree structures in WiredTiger. Large time-series document arrays can trigger document relocation and memory fragmentation.

---

### Q3: Why is `CLUSTERING ORDER BY (recorded_at DESC)` critical for time-series sensor dashboards?
**Answer:**
Dashboards predominantly query recent readings (e.g. *"What is the current air quality in Delhi?"*). By specifying `CLUSTERING ORDER BY (recorded_at DESC)`, Cassandra stores the newest records at the physical head of the partition on disk. When executing `SELECT * FROM city_air_quality WHERE city = 'Delhi' LIMIT 20;`, Cassandra reads the first 20 records sequentially without scanning or sorting the remaining 20,000+ historical rows in memory ($O(1)$ disk seek).

---

### Q4: What does "Query-First Data Modeling" mean in NoSQL?
**Answer:**
In relational databases, you normalize data first (Third Normal Form) and write arbitrary SQL `JOIN` queries later. In Cassandra, joins and table-wide scans do not exist because data is distributed across multiple network nodes. Therefore, you must first list the **exact application queries** you need to answer, and design each table's primary key specifically to satisfy that single query pattern in a single partition seek.

---

### Q5: What is `ALLOW FILTERING` in CQL, and why is it usually an anti-pattern, but acceptable in our specific query?
**Answer:**
`ALLOW FILTERING` instructs Cassandra to execute a query even if it requires filtering on a column that is not part of the primary key.
* **Why it's dangerous at scale**: If executed across the entire cluster without a partition key, Cassandra must perform a **full cluster scan**, reading every SSTable on every node, causing timeouts.
* **Why it is safe in our spike query**: We restrict the query with `WHERE city = 'Delhi' AND recorded_at >= ? AND recorded_at <= ?`. Because the partition key and clustering range are strictly bounded, Cassandra only filters rows within that single node's memory partition, avoiding a cluster-wide broadcast.

---

### Q6: Where does Apache Cassandra sit on the CAP Theorem?
**Answer:**
Cassandra is classified as an **AP (Availability and Partition Tolerance)** system. It has a masterless, decentralized architecture where every node is identical (peer-to-peer gossip protocol). If network partitions occur, nodes continue accepting reads and writes. Cassandra offers **Tunable Consistency**, allowing developers to choose consistency per query (e.g. `ConsistencyLevel.ONE`, `QUORUM`, or `ALL`).

---

### Q7: Explain the Write Path of Apache Cassandra.
**Answer:**
When a write request arrives at a coordinator node:
1. It hashes the partition key and forwards the write to the replica nodes.
2. Each replica writes sequentially to the on-disk **CommitLog** (crash recovery log).
3. The data is written to an in-memory sorted buffer called the **Memtable**.
4. Once written to CommitLog and Memtable, the write is acknowledged as successful (sub-millisecond latency).
5. When the Memtable fills up, it is flushed sequentially to disk as an immutable **SSTable** (Sorted String Table).
6. Periodic background **Compaction** merges SSTables and discards tombstoned (deleted) data.

---

### Q8: Why did we use concurrent prepared statements rather than one massive `BATCH` statement in `db_setup.py`?
**Answer:**
In Cassandra, `BATCH` statements are intended for keeping multiple tables in sync for a single partition (atomicity), **not** for bulk loading. Sending a monolithic batch containing thousands of rows spanning different partitions forces the coordinator node to track and coordinate all mutations in memory, leading to severe garbage collection pauses and `BatchStatement too large` exceptions. Using `cassandra.concurrent.execute_concurrent_with_args` distributes the requests asynchronously across cluster connections without overwhelming the coordinator.

---

### Q9: How does this project directly support UN Sustainable Development Goal 11?
**Answer:**
Target 11.6 and Indicator 11.6.2 mandate tracking urban particulate matter ($PM_{2.5}$ and $PM_{10}$). Relational databases struggle to scale as municipal IoT sensor density grows. This project demonstrates a production-grade Big Data architecture capable of ingesting high-frequency telemetry from thousands of city wards in real time, detecting dangerous episodic spikes (e.g. AQI > 300), and alerting public health authorities before severe respiratory damage occurs.

---

### Q10: How does Cassandra break ties when multiple sensors report at the exact same timestamp?
**Answer:**
If the primary key were only `(city, recorded_at)`, two different stations (e.g., `DEL_ITO` and `DEL_ANAND_VIHAR`) reporting at `2026-10-04 10:00:00` would overwrite each other's data because Cassandra uses the primary key for upsert semantics. By adding `station_id` as the second clustering column (`PRIMARY KEY (city, recorded_at, station_id)`), each sensor reading forms a uniquely identifiable row.

---

## 📜 License
This project is licensed under the Apache 2.0 License — suitable for academic, educational, and open-source demonstration purposes.
