# Spark Partitioning — Detailed Notes

Sep 29, 2026 · @Sunil Patil

## 1. What partitioning is

Partitioning splits a large dataset into smaller, manageable chunks so Spark can skip data it doesn't need and process the rest in parallel.

**The bookshelf analogy**

- **Unorganized shelf:** books placed in no order. To find one book you scan every book — slow and inefficient.
- **Sectioned shelf:** books grouped by author, spine colour or any other attribute. You go straight to the right section, so the search space shrinks.
- Dividing the shelf into sections = **partitioning** the shelf. Spark does the same with rows of data.

**Two problems partitioning solves**

1. **Faster reads / filtering** — Spark reads only the partitions that match a filter (partition pruning) instead of scanning everything.
2. **Parallelism and resource utilization** — the right number of partitions keeps every CPU core busy (Section 3).

## 2. Hands-on: partitionBy on a Spotify dataset

The demo writes a mock Spotify listening-activity dataset partitioned by date, so all rows for one day land in one folder.

**Dataset:** each row is a song play — song, time it was listened to, and duration (seconds listened).

**Step 1 — Prepare the partition column**

1. Rename `listen_date` to `listen_time` (it holds a timestamp, not a date).
2. Derive a pure date column `listen_date` with `to_date()` from `pyspark.sql.functions`, passing the format the timestamp uses.
3. Goal: every play on, say, 27 June goes into the 27 June partition.

```python
from pyspark.sql.functions import to_date, col

df = df.withColumnRenamed("listen_date", "listen_time")
df = df.withColumn("listen_date", to_date(col("listen_time"), "yyyy-MM-dd HH:mm:ss"))
```

**Step 2 — Write with partitionBy**

```python
df.write.partitionBy("listen_date").mode("overwrite").parquet("listening_activity_partitioned")
```

**Result on disk:** one sub-folder per distinct date, each holding only that date's rows:

```
listening_activity_partitioned/
    listen_date=2023-04-25/part-00000-....parquet
    listen_date=2023-06-27/part-00000-....parquet
    ...
```

A query filtering on `listen_date` now reads only the matching folder(s).

The exact timestamp format and output format (Parquet shown) are assumptions; match them to your data.

## 3. Parallelism and resource utilization

The right number of partitions keeps all cores busy without drowning Spark in tiny files.

**Resource utilization** = what share of your cluster's CPU cores and memory is actually working. If only 20% is busy and 80% sits idle, you are paying for a cluster you aren't using.

**How work is scheduled**

- An **executor** holds one or more **cores**.
- Each **core processes one partition** at a time.
- Partitions queue up and are handed to cores as they free up.

**Example setup:** 3 executors × 1 core = 3 cores.

| Scenario | What happens | Outcome |
| --- | --- | --- |
| 5 balanced partitions | 3 run at once on cores 1–3; the remaining 2 run as cores free up | All cores used — good |
| 1 large partition | One core does all the heavy lifting; the other 2 sit idle | Slow job, poor utilization |
| 50 tiny partitions | High parallelism, but many small files | Small-file problem: time lost in I/O and task overhead |

**Key point:** aim for an *optimal* partition count — enough to use every core, not so many that per-file overhead dominates.

## 4. Choosing the partition column

Pick a low-to-medium cardinality column that you filter on often.

**Criterion 1 — Cardinality** (number of unique values in the column). Example: an e-commerce transactions dataset.

| Column | Cardinality | Partition on it? | Why |
| --- | --- | --- | --- |
| `customer_id` | Very high (one per customer) | No | Creates a huge number of tiny partitions; Spark still scans many of them, so the search space barely shrinks |
| `state` | Low–medium | Yes | Each partition holds a good chunk of rows; a filter like `state = 'MH'` reads one folder |
| A column with 1 value | Extremely low | No | Only one partition — Spark scans the whole dataset anyway |

**Criterion 2 — Filter conditions.** If queries frequently filter on a column (e.g. `WHERE listen_date = ...`), partitioning on it lets Spark jump straight to the matching partition, making queries faster.

**Rule of thumb:** low-to-medium cardinality + frequently used in filters = good partition column.

## 5. Multi-level partitioning

Passing several columns to `partitionBy` creates nested folders, and the column order sets the nesting order.

**Date, then hour** (saved as `partition2`):

```python
df.write.partitionBy("listen_date", "listen_hour").parquet("partition2")
```

```
partition2/
    listen_date=2023-04-25/
        listen_hour=10/part-00000-....parquet
```

**Hour, then date** (saved as `partition3`):

```python
df.write.partitionBy("listen_hour", "listen_date").parquet("partition3")
```

```
partition3/
    listen_hour=10/
        listen_date=2023-04-25/part-00000-....parquet
```

**Watch out:** order matters. Put the column you filter on most (usually the coarser one) first.

## 6. Controlling files per partition: repartition vs coalesce

Call `repartition(n)` before `partitionBy` to get up to n files per partition folder; `coalesce(n)` can't increase the count.

By default the `listen_date=2023-04-25` folder had just one data file (`part-00000`) plus a `.crc` checksum file.

**repartition — works**

```python
df.repartition(3).write.partitionBy("listen_date").parquet("partition4")
# each date folder -> part-00000, part-00001, part-00002 (+ .crc files)

df.repartition(6).write.partitionBy("listen_date").parquet("partition4")
# each date folder -> 6 part files
```

**coalesce — no effect here**

```python
df.coalesce(3).write.partitionBy("listen_date").parquet("partition5")
# still 1 file per folder; coalesce(6) -> still 1
```

**Why:** `coalesce` avoids a full shuffle, so it can only merge existing partitions, never split them. The DataFrame started with 1 partition, so it stays at 1. `repartition` does a full shuffle and can raise or lower the count.

|  | `repartition(n)` | `coalesce(n)` |
| --- | --- | --- |
| Shuffle | Full shuffle | No full shuffle |
| Can increase partitions | Yes | No |
| Can decrease partitions | Yes | Yes (cheaper) |
| Output balance | Evenly sized | Can be uneven |
| Use when | You need more partitions or even sizes | You only need fewer partitions, cheaply |

## 7. Read-time partitioning: spark.sql.files.maxPartitionBytes

`spark.sql.files.maxPartitionBytes` caps how many bytes go into one partition when Spark reads files, so it controls the partition count at read time.

**How it works:** Spark splits input files into chunks no larger than this value. Example: max = 128 MB and file = 512 MB → 512 / 128 = **4 partitions**. (Spark's default is 128 MB.)

**Demo**

1. Read the Spotify listening CSV with default settings and check the count:

```python
df = spark.read.csv("data/partitioning/raw/spotify_listening.csv", header=True)
df.rdd.getNumPartitions()   # -> 1 (whole file read as one partition)
```

2. Check the file size: `du -h spotify_listening...` → **448 KB**.
3. Set the max partition size to 1,000 bytes (\~1 KB) and re-read:

```python
spark.conf.set("spark.sql.files.maxPartitionBytes", "1000")
df = spark.read.csv("data/partitioning/raw/spotify_listening.csv", header=True)
df.rdd.getNumPartitions()   # -> 457
```

**Expected vs actual:** 448 KB / 1 KB ≈ 448 partitions expected; 457 observed. The value is a *maximum*, so some partitions come out smaller than 1,000 bytes, and the count lands close to, not exactly on, the estimate.

File path and filename above are illustrative; the video's exact path was only partly shown.

## 8. Quick reference

| Goal | API / setting | Note |
| --- | --- | --- |
| Organize output into folders by column | `df.write.partitionBy("col")` | One folder per distinct value |
| Nested folders | `partitionBy("a", "b")` | Order of columns = folder nesting order |
| More / even files per partition folder | `df.repartition(n)` before write | Full shuffle; can increase or decrease |
| Fewer files, cheaply | `df.coalesce(n)` | No full shuffle; can only decrease |
| Control partition size at read time | `spark.sql.files.maxPartitionBytes` | Default 128 MB; a max, not an exact size |
| Check partition count | `df.rdd.getNumPartitions()` |  |

**Remember**

- Partition on **low-to-medium cardinality** columns you **filter on often**.
- Avoid very high cardinality (e.g. `customer_id`) and single-value columns.
- Too few partitions → idle cores; too many → small-file problem. Aim for the balance.
