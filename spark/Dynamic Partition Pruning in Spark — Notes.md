# Dynamic Partition Pruning in Spark — Notes

Sep 29, 2026 · @Sunil Patil

## Overview

Dynamic Partition Pruning (DPP) lets Spark skip partitions of a large, partitioned table using filter values that are only known at runtime — typically values produced by filtering the other side of a join.

- **Prerequisite:** understand partitioning — data is written to disk in one folder per value of the partition column (e.g. `listen_date=2023-06-04/`).
- **Goal of pruning:** read fewer files, so both scan time and downstream processing drop.
- **Two flavours:** *static* pruning (filter value known when the query is written) and *dynamic* pruning (filter values discovered while the query runs).
- **Running example:** a music app with a `listening_activity` dataset (partitioned by `listen_date`) and a `songs` dataset (not partitioned).

## 1. Static partition pruning

When you filter directly on the partition column with a literal value, Spark reads only the matching partition folder.

**Dataset — listening activity** (partitioned on disk by `listen_date`):

| Column | Meaning |
| --- | --- |
| `song_id` | Song that was played |
| `listen_duration` | How long it was played (seconds) |
| `listen_time` | Timestamp of the play |
| `listen_date` | Date of the play — **partition column** |

**Example:**

```python
listening_df = spark.read.parquet("listening_activity")
listening_df.filter(col("listen_date") == "2023-06-04")
```

**What Spark does:**

1. Recognises that the filter is on the partition column.
2. Checks each partition folder: is this `listen_date=2023-06-04`?
3. Reads only the matching folder and skips every other partition.

**Result:** only a fraction of the data is scanned instead of the full dataset. The filter value is known at *query-writing / planning time* — that is what makes it **static**.

## 2. The problem: a join that scans everything

**Business question:** analyze users' listening behavior *on the release date* of each song, for songs released **after 2019-12-31**.

**Datasets:**

| Dataset | Key columns | Stored as |
| --- | --- | --- |
| `listening_activity` | `song_id`, `listen_duration`, `listen_time`, `listen_date` | Parquet, partitioned by `listen_date` (dates in 2019 and 2023) |
| `songs` | `song_id`, `release_date`, `title` | Single CSV, not partitioned |

**Join condition:**

- `listening.song_id == songs.song_id`
- `listening.listen_date == songs.release_date`

**Naive execution (no DPP):**

1. Full scan of `listening_activity` — every `listen_date` partition.
2. Full scan of `songs`.
3. Filter `songs` to `release_date > 2019-12-31` — the 2019 release dates drop out.
4. Join the filtered songs with the full listening data.

**The problem:** the join only needs listening rows whose `listen_date` equals one of the surviving release dates. Every other partition is read and then thrown away. With 3–4 years of daily partitions, that is a huge amount of wasted I/O.

## 3. How Dynamic Partition Pruning solves it

Spark reads the small `songs` side first, filters it, and passes the surviving release dates to the scan of `listening_activity` as a partition filter — so only those `listen_date` partitions are read.

&#91;embedded content: DPP flow · songs side prunes the listening scan\]

The release dates that survive the filter become a runtime partition filter on the big table; the broadcast songs then join with the pruned listening data.

**Step by step:**

1. **Scan `songs`** — a full scan, but it is a small table.
2. **Filter** to `release_date > 2019-12-31` — e.g. only three release dates remain.
3. **Re-read the question:** listen date must equal release date, so only listening data on *those three dates* matters.
4. **Pass those dates to Spark** as a filter on `listen_date` (the partition column).
5. **Selective scan of `listening_activity`** — only the matching partitions ("green") are read; all others ("red") are skipped.
6. **Join** the filtered songs with the pruned listening data.

**Why "dynamic":** the filter values (release dates) are not known when the query is written. They are only known at runtime, after part of the query (scan + filter of `songs`) has executed.

**Benefit:** with hundreds of partitions but only a few relevant ones, scan time and processing time both drop sharply.

## 4. Static vs dynamic pruning

| Aspect | Static partition pruning | Dynamic partition pruning |
| --- | --- | --- |
| When filter value is known | At query-writing / planning time | At runtime, after part of the query runs |
| Typical query | `df.filter(listen_date == '2023-06-04')` | Join where one side's filtered values decide the other side's partitions |
| Source of filter values | A literal in the query | Results of scanning + filtering the other (usually smaller) table |
| Plan evidence | Partition filter with a literal value | `dynamicpruningexpression(...)` in `PartitionFilters` |
| Requirement | Filter on the partition column | Join key on the big side must be its partition column |

## 5. When DPP works — and when it doesn't

DPP is **enabled by default** in Spark, but it does not always kick in automatically.

**Why it worked in the example:**

- The join matched `songs.release_date` with `listening_activity.listen_date`.
- `listen_date` is the **partition column** of the big table.
- So the dates collected from `songs` (via a broadcast exchange) could be used directly to pick partition folders.

**When it will NOT work:**

- **Big table not partitioned** — there are no partition folders to skip, so Spark scans the whole file anyway.
- **Join on a non-partition column** — e.g. joining on `song_id` or `artist_id` when `listening_activity` is partitioned only by `listen_date`. The runtime values cannot map to partitions, so a full scan happens.

**Rule of thumb:** for DPP, one side must be partitioned, and the join key on that side must be its partition column. The other side supplies the runtime filter values.

## 6. Code walkthrough (PySpark)

The plan shows a `dynamicpruningexpression` on the listening scan, and the SQL DAG shows the broadcast exchange being **reused** — the songs data is not read twice.

**Code (reconstructed from the video):**

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

spark = SparkSession.builder.appName("dpp-demo").getOrCreate()

# 1. Big table: partitioned by listen_date
listening_df = spark.read.parquet("listening_activity")   # song_id, listen_time, listen_duration, listen_date

# 2. Small table: single CSV, not partitioned
songs_df = spark.read.csv("songs.csv", header=True, inferSchema=True)

# release_date is a datetime -> keep it as release_datetime, derive a pure date
songs_df = (songs_df
    .withColumnRenamed("release_date", "release_datetime")
    .withColumn("release_date", to_date(col("release_datetime"))))

# 3. Filter songs released after 2019-12-31
filtered_songs = songs_df.filter(col("release_date") > "2019-12-31")

# 4. Join on song_id AND listen_date == release_date
result = listening_df.join(
    filtered_songs,
    (listening_df.song_id == filtered_songs.song_id) &
    (listening_df.listen_date == filtered_songs.release_date))

result.explain()   # inspect the physical plan
result.show()      # trigger execution, then check the Spark UI SQL tab
```

**Data preparation note:** `release_date` in the CSV is a datetime, while `listen_date` is a date. Convert with `to_date` so the join condition compares like with like.

**Physical plan — what to look for (`explain()`):**

1. `FileScan csv` (songs) → `Filter` → `Project` (select relevant columns).
2. `BroadcastExchange` — the small filtered songs DataFrame is broadcast.
3. `BroadcastHashJoin` between songs and listening activity.
4. `FileScan csv` for songs: `PartitionFilters: []` — empty, because a single CSV has no partitions.
5. `FileScan parquet` for listening activity: `PartitionFilters: [dynamicpruningexpression(...)]` — **this is DPP**; the partitions to read are decided at runtime.
6. The plan text can make it look like songs is read a second time to compute the pruning values — the DAG shows otherwise.

**Spark UI → SQL tab (DAG):**

- Scan CSV (songs) → Filter (`> 2019-12-31`) → BroadcastExchange.
- The exchange is shown as **ReusedExchange** — the same broadcast result feeds the pruning filter; songs is not scanned again.
- The listening activity scan reads only relevant partitions; the demo showed a **dynamic partition pruning time of \~104 ms**.
- Then the broadcast hash join runs between the two DataFrames.

**Config (on by default):** `spark.sql.optimizer.dynamicPartitionPruning.enabled = true`.

## 7. Quick revision

- **Partition pruning** = reading only the partition folders a query needs.
- **Static:** the filter literal on the partition column is known upfront → Spark skips folders at planning time.
- **Dynamic:** the filter values come from another table at runtime (usually the small, filtered side of a join) → Spark prunes the big table's partitions on the fly.
- **Mechanism:** small side scanned + filtered → broadcast → values reused as `dynamicpruningexpression` on the big side's partition column.
- **Must have:** a partitioned big table, joined on its partition column.
- **Won't help:** unpartitioned data, or joins on non-partition columns (e.g. `song_id`, `artist_id`).
- **How to verify:** `explain()` → look for `dynamicpruningexpression` in `PartitionFilters`; Spark UI SQL tab → `ReusedExchange` and "dynamic partition pruning time".
- **Payoff:** biggest when there are hundreds of partitions but only a few are relevant.
