

# DuckDB Joins

This project demonstrates the use of DuckDB to perform various types of joins on a large dataset. The dataset used in this project is a CSV file containing information about railway services in the Netherlands for March 2025.

## Getting Started

To get started with this project, you need to have DuckDB installed. You can use the DuckDB shell to execute the SQL commands provided in this README.

### Prerequisites

- DuckDB (https://duckdb.org/)

### Installation

1. Download and install DuckDB from the official website: https://duckdb.org/

2. Launch the DuckDB shell.

or try online
https://shell.duckdb.org/

## Dataset

The dataset used in this project is a CSV file containing information about railway services in the Netherlands for March 2025. The CSV file is available at the following URL:

```
https://blobs.duckdb.org/nl-railway/services-2025-03.csv.gz
```

## Creating Tables

First, create a table `tab1` by reading the CSV file:

```sql
CREATE TABLE tab1 AS
    FROM read_csv([
        'https://blobs.duckdb.org/nl-railway/services-2025-03.csv.gz'
    ]);
```

Verify the number of rows in `tab1`:

```sql
SELECT COUNT(*) FROM tab1;
```
```
┌─────────┐
│ Count   │
╞═════════╡
│ 1902543 │
└─────────┘
```
Create additional tables `tab2`, `tab3`, `tab4`, and `tab5` by copying the data from `tab1`:

```sql
CREATE TABLE tab2 AS SELECT * FROM tab1;
CREATE TABLE tab3 AS SELECT * FROM tab1;
CREATE TABLE tab4 AS SELECT * FROM tab1;
CREATE TABLE tab5 AS SELECT * FROM tab1;
```

Verify the tables have been created:

```sql
SHOW TABLES;
```
```
┌──────┐
│ name │
╞══════╡
│ tab1 │
│ tab2 │
│ tab3 │
│ tab4 │
│ tab5 │
└──────┘
```

## Performing Joins

### Positional Join

Create a table `join_all` by performing a positional join on the tables:

```sql
CREATE TABLE join_all AS 
  FROM tab1
  POSITIONAL JOIN tab2
  POSITIONAL JOIN tab3
  POSITIONAL JOIN tab4
  POSITIONAL JOIN tab5;
```

Verify the number of rows in `join_all`:

```sql
SELECT COUNT(*) FROM join_all;
```
```
┌─────────┐
│ Count   │
╞═════════╡
│ 1902543 │
└─────────┘ 
```

```sql
SELECT count(*) AS column_count
   FROM duckdb_columns()
     WHERE table_name = 'join_all';
```
 ```
┌──────────────┐
│ column_count │
╞══════════════╡
│           85 │
└──────────────┘
```

## Profiling and Analysis

Use the `EXPLAIN ANALYZE` command to profile and analyze the query execution:

```sql
EXPLAIN ANALYZE
  FROM tab1
  POSITIONAL JOIN tab2
  POSITIONAL JOIN tab3
  POSITIONAL JOIN tab4
  POSITIONAL JOIN tab5;
```
 
```
┌─────────────────────────────────────┐
│┌───────────────────────────────────┐│
││    Query Profiling Information    ││
│└───────────────────────────────────┘│
└─────────────────────────────────────┘
explain analyze from tab1   positional join tab2   positional join tab3   positional join tab4   positional join tab5    ;
┌────────────────────────────────────────────────┐
│┌──────────────────────────────────────────────┐│
││              Total Time: 0.532s              ││
│└──────────────────────────────────────────────┘│
└────────────────────────────────────────────────┘
┌───────────────────────────┐
│           QUERY           │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│      EXPLAIN_ANALYZE      │
│    ────────────────────   │
│           0 rows          │
│          (0.00s)          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│      POSITIONAL_SCAN      │
│    ────────────────────   │
│       1,902,543 rows      ├──────────────┬────────────────────────────┬────────────────────────────┬────────────────────────────┐
│          (0.50s)          │              │                            │                            │                            │
└─────────────┬─────────────┘              │                            │                            │                            │
┌─────────────┴─────────────┐┌─────────────┴─────────────┐┌─────────────┴─────────────┐┌─────────────┴─────────────┐┌─────────────┴─────────────┐
│         TABLE_SCAN        ││         TABLE_SCAN        ││         TABLE_SCAN        ││         TABLE_SCAN        ││         TABLE_SCAN        │
│    ────────────────────   ││    ────────────────────   ││    ────────────────────   ││    ────────────────────   ││    ────────────────────   │
│        Table: tab1        ││        Table: tab2        ││        Table: tab3        ││        Table: tab4        ││        Table: tab5        │
│   Type: Sequential Scan   ││   Type: Sequential Scan   ││   Type: Sequential Scan   ││   Type: Sequential Scan   ││   Type: Sequential Scan   │
│                           ││                           ││                           ││                           ││                           │
│        Projections:       ││        Projections:       ││        Projections:       ││        Projections:       ││        Projections:       │
│       Service:RDT-ID      ││       Service:RDT-ID      ││       Service:RDT-ID      ││       Service:RDT-ID      ││       Service:RDT-ID      │
│        Service:Date       ││        Service:Date       ││        Service:Date       ││        Service:Date       ││        Service:Date       │
│        Service:Type       ││        Service:Type       ││        Service:Type       ││        Service:Type       ││        Service:Type       │
│      Service:Company      ││      Service:Company      ││      Service:Company      ││      Service:Company      ││      Service:Company      │
│    Service:Train number   ││    Service:Train number   ││    Service:Train number   ││    Service:Train number   ││    Service:Train number   │
│     Service:Completely    ││     Service:Completely    ││     Service:Completely    ││     Service:Completely    ││     Service:Completely    │
│          cancelled        ││          cancelled        ││          cancelled        ││          cancelled        ││          cancelled        │
│  Service:Partly cancelled ││  Service:Partly cancelled ││  Service:Partly cancelled ││  Service:Partly cancelled ││  Service:Partly cancelled │
│   Service:Maximum delay   ││   Service:Maximum delay   ││   Service:Maximum delay   ││   Service:Maximum delay   ││   Service:Maximum delay   │
│        Stop:RDT-ID        ││        Stop:RDT-ID        ││        Stop:RDT-ID        ││        Stop:RDT-ID        ││        Stop:RDT-ID        │
│     Stop:Station code     ││     Stop:Station code     ││     Stop:Station code     ││     Stop:Station code     ││     Stop:Station code     │
│     Stop:Station name     ││     Stop:Station name     ││     Stop:Station name     ││     Stop:Station name     ││     Stop:Station name     │
│     Stop:Arrival time     ││     Stop:Arrival time     ││     Stop:Arrival time     ││     Stop:Arrival time     ││     Stop:Arrival time     │
│     Stop:Arrival delay    ││     Stop:Arrival delay    ││     Stop:Arrival delay    ││     Stop:Arrival delay    ││     Stop:Arrival delay    │
│   Stop:Arrival cancelled  ││   Stop:Arrival cancelled  ││   Stop:Arrival cancelled  ││   Stop:Arrival cancelled  ││   Stop:Arrival cancelled  │
│    Stop:Departure time    ││    Stop:Departure time    ││    Stop:Departure time    ││    Stop:Departure time    ││    Stop:Departure time    │
│    Stop:Departure delay   ││    Stop:Departure delay   ││    Stop:Departure delay   ││    Stop:Departure delay   ││    Stop:Departure delay   │
│  Stop:Departure cancelled ││  Stop:Departure cancelled ││  Stop:Departure cancelled ││  Stop:Departure cancelled ││  Stop:Departure cancelled │
│                           ││                           ││                           ││                           ││                           │
│           0 rows          ││           0 rows          ││           0 rows          ││           0 rows          ││           0 rows          │
│          (0.00s)          ││          (0.00s)          ││          (0.00s)          ││          (0.00s)          ││          (0.00s)          │
└───────────────────────────┘└───────────────────────────┘└───────────────────────────┘└───────────────────────────┘└───────────────────────────┘
```

```sql
FROM duckdb_memory();
```
```
┌─────────────────────┬────────────────────┬─────────────────────────┐
│ tag                 ┆ memory_usage_bytes ┆ temporary_storage_bytes │
╞═════════════════════╪════════════════════╪═════════════════════════╡
│ BASE_TABLE          ┆                  0 ┆                       0 │
│ HASH_TABLE          ┆                  0 ┆                       0 │
│ PARQUET_READER      ┆                  0 ┆                       0 │
│ CSV_READER          ┆                  0 ┆                       0 │
│ ORDER_BY            ┆                  0 ┆                       0 │
│ ART_INDEX           ┆                  0 ┆                       0 │
│ COLUMN_DATA         ┆                  0 ┆                       0 │
│ METADATA            ┆                  0 ┆                       0 │
│ OVERFLOW_STRINGS    ┆                  0 ┆                       0 │
│ IN_MEMORY_TABLE     ┆         3190804480 ┆                       0 │
│ ALLOCATOR           ┆                  0 ┆                       0 │
│ EXTENSION           ┆                  0 ┆                       0 │
│ TRANSACTION         ┆                  0 ┆                       0 │
│ EXTERNAL_FILE_CACHE ┆                  0 ┆                       0 │
└─────────────────────┴────────────────────┴─────────────────────────┘
```
### Natural Join

Attempt to perform a natural join on the tables (note: this may result in an out-of-memory error):

```sql
EXPLAIN ANALYZE 
  FROM tab1
  NATURAL JOIN tab2
  NATURAL JOIN tab3
  NATURAL JOIN tab4
  NATURAL JOIN tab5;
```

If you encounter an out-of-memory error, consider the following solutions:

- Launch the database with a persistent storage back-end.
- Set a temporary directory: `SET temp_directory='/path/to/tmp.tmp'`
- Reduce the number of threads: `SET threads=X`
- Disable insertion-order preservation: `SET preserve_insertion_order=false`
- Increase the memory limit: `SET memory_limit='...GB'`

## Summary

This project demonstrates how to use DuckDB to perform various types of joins on a large dataset. 
By following the steps outlined in this README, you can replicate the process and experiment with different join types and optimization techniques.

Run the complete experiment locally (https://raw.githubusercontent.com/teoria/duckdb_heavy_joins/refs/heads/main/positional_join.sql)

---

This README provides a comprehensive guide to setting up and running the project, including the necessary SQL commands and troubleshooting tips.
 

