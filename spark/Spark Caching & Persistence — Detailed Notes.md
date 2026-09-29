# Spark Caching & Persistence — Detailed Notes

Sep 29, 2026 · @Sunil Patil

## 1. Overview

Caching stores a DataFrame's computed result in memory, on disk, or both, so Spark doesn't recompute it every time it's reused.

- **Goal:** avoid repeating the same transformations again and again.
- **When to use:** a DataFrame is reused by several downstream operations (multiple actions or multiple derived DataFrames).
- **Why it matters:** without caching, every action re-runs the full lineage from the source file, wasting CPU, I/O and time.

## 2. Setup and what the Spark UI shows

The demo uses a SparkSession and a customer Parquet dataset (customer ID plus details such as city, age and birthday).

- **Job 1 — reading the Parquet file:** reading is *not* an action, yet a job appears. It only reads **metadata** (column names, schema), which is why its input size is **0**.
- **Job 2 — `show()` (showString):** this is a real action. It reads the actual data, with an input of about **157.2 KB**.

## 3. The experiment: one base, two children

One base DataFrame (`df_base`) is built once and reused by two child DataFrames (`df1`, `df2`) to test whether Spark recomputes the base.

**Building `df_base`**

1. Filter rows where `city == 'Boston'`.
2. Add `customer_group` by age: kid, young, mid, old (using `when/otherwise`).
3. Select the relevant columns.

**Building `df1` and `df2` on top of `df_base`**

- `df1` = `df_base` + `test_column_1` + `birth_year` (split `birthday`, take the year part).
- `df2` = `df_base` + `test_column_2` + `birth_month` (split `birthday`, take the item at index 0).

The code below is a reconstruction of the demo; the age cut-offs are illustrative, as the video doesn't state them.

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("caching-demo").getOrCreate()
df = spark.read.parquet("customers.parquet")

df_base = (
    df.filter(F.col("city") == "Boston")
      .withColumn("customer_group",
          F.when(F.col("age") < 13, "kid")
           .when(F.col("age") < 30, "young")
           .when(F.col("age") < 55, "mid")
           .otherwise("old"))
      .select("customer_id", "name", "age", "birthday", "city", "customer_group")
)
df_base.show()

df1 = (df_base
       .withColumn("test_column_1", F.lit(1))
       .withColumn("birth_year", F.split("birthday", "/").getItem(1)))
df1.show()

df2 = (df_base
       .withColumn("test_column_2", F.lit(2))
       .withColumn("birth_month", F.split("birthday", "/").getItem(0)))
df2.show()

df1.explain()
df2.explain()
```

## 4. Without caching: the base is rebuilt every time

Both query plans start from the Parquet scan and redo every `df_base` step, so `df_base` is effectively computed twice.

**Plan for `df1`**

1. Scan Parquet file
2. ColumnarToRow
3. Filter (`city = Boston`) — a `df_base` step
4. Project: create `customer_group` (CASE WHEN) — a `df_base` step
5. Project: create `birth_year` and `test_column_1` — the only new work

**Plan for `df2`**

1. Scan Parquet file
2. ColumnarToRow
3. Filter (`city = Boston`) — repeated
4. Project: create `customer_group` — repeated from scratch
5. Project: create `birth_month` and `test_column_2`

**Spark UI confirms it:** each job scans the Parquet file again, applies the filter again, then adds its own columns. Calling `df_base.show()` earlier did **not** save `df_base` anywhere.

## 5. Why it happens: lazy evaluation and the DAG

Spark is lazy: transformations only build a plan, and nothing runs until an action is called.

- **Transformations** (`filter`, `withColumn`, `select`) are recorded, not executed.
- **Actions** (`show`, `count`, `collect`, `write`) trigger a job.
- The recorded steps form a **lineage graph / DAG** (Directed Acyclic Graph).
- On each action, Spark executes the DAG **from the source** (reading the file) to the end.
- So `df1.show()` runs *read → df\_base steps → df1 columns*, and `df2.show()` runs *read → df\_base steps → df2 columns*. The `df_base` part is repeated.

**Fix:** cache `df_base`. The first action materialises it into storage; later actions on `df1` and `df2` read the cached `df_base` and only add their own columns.

&#91;embedded content: lineage without vs with cache\]

Without a cache, the read and base steps (highlighted) run again for df2; with a cache, both children start from the stored df\_base.

## 6. With caching: an in-memory table scan replaces the rebuild

After adding `df_base.cache()`, the Parquet scan shows **0** in the child jobs and is replaced by an **InMemoryTableScan**.

```python
df_base = df_base.cache()   # lazy: marks it for caching
df_base.show()              # first action materialises the cache
df1.show()                  # reads df_base from cache
df2.show()                  # reads df_base from cache
```

**What changes in the Spark UI DAG**

- Scan Parquet: all metrics **0**, so the file isn't read again.
- **InMemoryTableScan** reads the cached `df_base`.
- Only the child's own work runs: `birth_month` + `test_column_2` for `df2`, `birth_year` + `test_column_1` for `df1`.

**What changes in the query plan**

- The plan starts with `InMemoryTableScan`, and `customer_group` already exists as a column.
- An `InMemoryRelation` block describes how the cached table was built. It's a summary, not re-executed work.
- No `CASE WHEN` appears for `customer_group`: it is referenced, not recreated.

**Takeaway:** if a DataFrame feeds several downstream operations, cache it; otherwise the whole lineage re-runs and consumes more resources.

## 7. Storage levels

The storage level decides where the cached data lives (memory, disk, or both), whether it is serialized, and how many replicas are kept.

**Key terms**

- **Deserialized:** stored as JVM objects. Faster to read, uses more space.
- **Serialized:** stored as bytes. More compact, but costs CPU to convert back when read.
- **Replication (1x, 2x, 3x):** copies on different executors for fault tolerance (resilience).

**To change the level, unpersist first:** a DataFrame must be uncached before it can be cached at another level.

```python
from pyspark import StorageLevel

df_base.unpersist()
df_base.persist(StorageLevel.MEMORY_ONLY)
df2.show()   # action materialises the cache; check the Storage tab
```

**Demo results (Spark UI → Storage tab)**

| Storage level | Where | Format | Replicas | Size in memory | Size on disk |
| --- | --- | --- | --- | --- | --- |
| `MEMORY_AND_DISK` (default) | Memory + disk | Deserialized | 1x | — | — |
| `MEMORY_ONLY` | Memory | Deserialized | 1x | 19.6 KB | 0 |
| `MEMORY_ONLY_2` | Memory | Deserialized | 2x | 19.6 KB | 0 |
| `DISK_ONLY` | Disk | Serialized | 1x | 0 | 19.6 KB |
| `DISK_ONLY_3` | Disk | Serialized | 3x | 0 | 19.6 KB |
| `MEMORY_AND_DISK_2` | Memory + disk | Serialized | 2x | 19.6 KB | 0 |

**Why is disk size 0 for `MEMORY_AND_DISK_2`?** Memory was enough to hold the data. Spark spills to disk only when memory runs out.

The suffix `_2` / `_3` means the number of replicas.

## 8. cache() vs persist()

`cache()` and `persist()` do the same thing; the only difference is that `persist()` lets you choose the storage level.

|  | `cache()` | `persist()` |
| --- | --- | --- |
| Storage level argument | Not allowed | Optional, e.g. `StorageLevel.DISK_ONLY` |
| Level used | Default (`MEMORY_AND_DISK`) | Whatever you pass (default if none) |
| Equivalent to | `df.persist(StorageLevel.MEMORY_AND_DISK)` | — |

- Both are **lazy**: the data is stored only when the first action runs.
- `unpersist()` removes the cached data. Call it before switching levels or when the DataFrame is no longer needed, to free memory.

## 9. Choosing a storage level: space vs CPU

The core trade-off: deserialized data uses more space but little CPU; serialized data saves space but costs CPU to read.

| Level | Space used | CPU time | In memory | On disk | Serialized |
| --- | --- | --- | --- | --- | --- |
| `MEMORY_ONLY` | High | Low | Yes | No | No |
| `MEMORY_ONLY_SER` | Low | High | Yes | No | Yes |
| `MEMORY_AND_DISK` | High | Medium | Some | Some | Some |
| `MEMORY_AND_DISK_SER` | Low | High | Some | Some | Yes |
| `DISK_ONLY` | Low | High | No | Yes | Yes |

The video walks through the first two rows; the remaining rows follow the same commonly shared chart.

- **`MEMORY_ONLY`:** data kept as JVM objects, so no conversion is needed → fast but memory-hungry.
- **`MEMORY_ONLY_SER`:** data compacted into bytes → saves memory, but CPU cycles are spent deserializing on every read.
- `_SER` levels are available in Scala/Java; in PySpark, `StorageLevel` exposes the non-`_SER` names.

## 10. Quick revision

1. Caching saves a DataFrame in memory, on disk, or both, to avoid recomputation.
2. Spark is lazy: every action re-runs the full lineage (DAG) from the source unless something is cached.
3. Reading Parquet triggers a small metadata-only job (input size 0).
4. Without cache: each child's plan shows Scan Parquet → Filter → CASE WHEN again.
5. With cache: Spark UI shows **InMemoryTableScan**, and Parquet scan metrics drop to 0.
6. `cache()` = `persist()` with the default level `MEMORY_AND_DISK`.
7. Use `unpersist()` before changing the storage level, and to free memory.
8. `_2` / `_3` suffix = replication factor for fault tolerance.
9. Deserialized = more space, less CPU. Serialized = less space, more CPU.
10. `MEMORY_AND_DISK` spills to disk only when memory isn't enough.
11. Cache a DataFrame when it's reused multiple times downstream; don't cache one-time-use data.
