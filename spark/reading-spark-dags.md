# Reading Spark DAGs: Study Notes

Source: "Master Reading Spark DAGs" (YouTube, https://www.youtube.com/watch?v=O_45zAz1OGk). Companion to [reading-spark-query-plans.md](reading-spark-query-plans.md), which is worth reading first. These notes follow the video's order and clean up transcription errors. Numbers (partitions, rows, files) come from the video's datasets and will differ on your data; UI snippets are simplified.

## Contents

1. Where to look in the Spark UI
2. Reading files
3. Narrow transformations
4. Wide transformation: sort merge join
5. Wide transformation: broadcast join
6. Group by with `count` / `sum`
7. Group by with `countDistinct`
8. Cheat sheet: DAG node glossary
9. Key takeaways

---

## 1. Where to look in the Spark UI

| Tab | What it tells you |
|---|---|
| **Jobs** | One entry per job. A job is (normally) triggered by an action. |
| **Stages** | Stages inside each job. Stage boundaries = shuffles. Shows input size, shuffle read/write, task count. |
| **SQL / DataFrame** | The DAG (visual physical plan) for each query, with runtime metrics per node (rows, files, batches, partitions). |

Mental model:

```
Action ──► Job(s) ──► Stages (split at every shuffle) ──► Tasks (1 task per partition)
```

- **# tasks in a stage = # partitions** that stage processes.
- **# stages = # shuffles + 1** (for a single linear query).

## 2. Reading files

### Setup

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("spark-dags").getOrCreate()

df_transactions = spark.read.parquet("data/transactions")
df_customers    = spark.read.parquet("data/customers")

df_transactions.rdd.getNumPartitions()   # 13 in the video
df_transactions.show(5)
```

### Why do I see two jobs when I only called one action?

| Job | Triggered by | Has input? | Why |
|---|---|---|---|
| `parquet at ...` | `spark.read.parquet(...)` | No | Spark reads **metadata only**: schema, column types, file list, sizes, partition info. It needs this to plan/optimize. |
| `showString at ...` | `.show()` | Yes | The real action, actually reads data. |

- No shuffle → each job has **one stage**.
- Reading is not an action, but a metadata job still shows up. Don't be confused by it.

### DAG for read + show

```
Scan parquet           number of files read: 163    (transactions)
   │                   number of output rows: 4,096
   ▼
ColumnarToRow          number of input batches: 1
   │
   ▼
CollectLimit 6
```

- **Scan parquet**: files read = number of `part-*` files in the folder (transactions had `part-00000` … `part-00162` = 163 files; customers had 1 file).
- **ColumnarToRow**: Parquet is columnar; downstream operators work on rows, so Spark converts.
- **CollectLimit 6**: `show(5)` fetches 5 rows + 1 extra (used by `show` to know whether there are more rows to report).
- **Batches**: a batch is a group of rows (default 4,096 rows for the vectorized Parquet reader). It is **not** the same as a partition. Since `show(5)` only needs 5 rows, Spark reads just **one batch** instead of scanning everything. Hence "output rows: 4,096".

## 3. Narrow transformations

Narrow = no shuffle; each output partition depends on one input partition.

```python
df_narrow = (
    df_customers
    .filter(F.col("city") == "boston")
    .withColumn("first_name", F.split("name", " ").getItem(0))
    .withColumn("last_name",  F.split("name", " ").getItem(1))
    .withColumn("age", F.col("age") + F.lit(5))
    .select("cust_id", "first_name", "last_name", "age", "gender", "birthday")
)

df_narrow.write.format("noop").mode("overwrite").save()
```

### The `noop` trick

- `show()` only reads one batch, so it doesn't reflect a full run.
- Writing forces a **full read** of the dataset.
- `format("noop")` = *no operation*: it **simulates** the write (executes the full plan) without writing anything. Great for benchmarking and testing.

### Jobs / stages

- 1 job (`save`), 1 stage (no shuffle), reads full input.

### DAG

```
Scan parquet        files read: 1, output rows: 5,000 (the whole dataset)
   ▼
ColumnarToRow
   ▼
Filter              (city = 'boston')
   ▼
Project             split(name)[0], split(name)[1], age + 5, selected columns
   ▼
WriteToDataSourceV2 / OverwriteByExpression (noop)
```

- There is **no separate node** for each `withColumn`. All column additions, modifications and the final `select` are **collapsed into one `Project`** node.
- Think of `Project` like a SQL `SELECT` list: it can hold plain columns *and* expressions (e.g. `CAST`, `split`, `+ 5`).

## 4. Wide transformation: sort merge join

The customers table is tiny (KB), so Spark would broadcast it by default. Disable that to force a sort merge join:

```python
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

df_joined = df_transactions.join(df_customers, on="cust_id", how="inner")
df_joined.write.format("noop").mode("overwrite").save()
```

### Why 3 jobs for 1 action?

| Job | Tasks | What it does |
|---|---|---|
| 1 | 13 | Read transactions (13 partitions) + shuffle write |
| 2 | 1 | Read customers (1 partition) + shuffle write |
| 3 | — | Shuffle read both sides + sort merge join + write |

With AQE, Spark runs each shuffle "map" side as its own job/stage, collects runtime stats, and then plans the rest. That's why one action produces several jobs.

Tip: match task counts to partition counts (13 ↔ transactions, 1 ↔ customers) to identify which job is which.

### DAG

```
 Scan parquet (transactions)       Scan parquet (customers)
   ~39M rows                          │
   ▼                                  ▼
 ColumnarToRow                      ColumnarToRow
   ▼                                  ▼
 Filter isnotnull(cust_id)          Filter isnotnull(cust_id)
   ▼                                  ▼
 Exchange hashpartitioning(cust_id, 200)   Exchange hashpartitioning(cust_id, 200)
   ▼                                  ▼
 AQEShuffleRead (coalesced/skew)    AQEShuffleRead (coalesced)
   ▼                                  ▼
 Sort (cust_id)                     Sort (cust_id)
          └──────────┬───────────────────┘
                     ▼
              SortMergeJoin (inner, cust_id)
                     ▼
                  Project
                     ▼
     AdaptiveSparkPlan isFinalPlan=true
                     ▼
               Write (noop)
```

Node by node:

- **Filter `isnotnull(cust_id)`**: added by Spark, not by you. An inner join can never match on a null key, so Spark drops nulls early (optimization + correctness).
- **Exchange**: the shuffle. Both sides are hash-partitioned on the join key into `spark.sql.shuffle.partitions` = **200** (default) partitions so that matching keys land in the same partition.
- **AQEShuffleRead**: Adaptive Query Execution (Spark 3.0+) reads runtime stats (partition sizes, row counts) and adjusts the plan:
  - **Coalescing**: many of the 200 partitions were empty/tiny → merged down to **24** partitions.
  - **Skew handling**: one partition was much bigger than the rest (skewed) → split into **12** pieces. So on the transactions side it went 200 → 36 (after splitting) → 24 read partitions.
- **Sort**: both sides sorted on the join key (required by sort merge join).
- **SortMergeJoin**: merges the two sorted streams.
- **Project**: final output columns of the join.

### `isFinalPlan=false` vs `true`

| Where | Value | Why |
|---|---|---|
| `df.explain()` | `false` | Query hasn't run; AQE hasn't seen runtime stats yet, so the plan may still change. |
| Spark UI SQL tab (after run) | `true` | Execution happened; this is the plan AQE actually chose. |

## 5. Wide transformation: broadcast join

```python
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10 * 1024 * 1024)  # 10 MB

df_bcast = df_transactions.join(F.broadcast(df_customers), on="cust_id", how="inner")
df_bcast.write.format("noop").mode("overwrite").save()
```

Requirement: one side must be **small** (under the threshold, or explicitly hinted with `F.broadcast`).

### Jobs / stages

| Job | What it does |
|---|---|
| 1 | Read the small table (customers), collect it to the driver |
| 2 | Broadcast it to executors, scan the big table and join |

### DAG

```
 Scan parquet (transactions)          Scan parquet (customers)
   ▼                                     ▼
 ColumnarToRow                         ColumnarToRow
   ▼                                     ▼
 Filter isnotnull(cust_id)             Filter isnotnull(cust_id)
   │                                     ▼
   │                                BroadcastExchange
   └──────────────┬──────────────────────┘
                  ▼
     BroadcastHashJoin (inner, cust_id = cust_id)
                  ▼
               Project
```

- **No `Exchange` (shuffle) on the big table.** That's the whole point: the big side stays where it is, and each executor gets a full copy of the small side.
- **BroadcastExchange** replaces the shuffle for the small side.
- No `Sort` nodes: a hash join doesn't need sorted input.

### Sort merge vs broadcast at a glance

| | Sort merge join | Broadcast hash join |
|---|---|---|
| Shuffle | Both sides | None (small side broadcast) |
| Sort | Both sides | None |
| Key DAG nodes | `Exchange`, `AQEShuffleRead`, `Sort`, `SortMergeJoin` | `BroadcastExchange`, `BroadcastHashJoin` |
| When | Both sides large | One side small enough to fit in memory |

## 6. Group by with `count` / `sum`

```python
df_count = df_transactions.groupBy("city").count()
df_count.show()

df_sum = df_transactions.groupBy("city").agg(F.sum("amt").alias("total_amt"))
df_sum.show()
```

### Jobs / stages

- 2 jobs: (1) read 13 partitions + shuffle write, (2) shuffle read + final aggregation + output.

### DAG (count and sum look the same, just a different function)

```
Scan parquet
   ▼
ColumnarToRow
   ▼
HashAggregate   keys=[city], functions=[partial_count(1)]     ← local, per partition
   ▼
Exchange hashpartitioning(city, 200)                          ← shuffle
   ▼
AQEShuffleRead  (coalesced 200 → 1)
   ▼
HashAggregate   keys=[city], functions=[count(1)]             ← final, after shuffle
```

### Worked example

Three partitions before any work:

```
P1: A, A, B        P2: A, B, C        P3: A
```

**Step 1: partial `HashAggregate`** (local count in each partition, no data moved):

```
P1: A=2, B=1       P2: A=1, B=1, C=1  P3: A=1
```

**Step 2: `Exchange` on `city`**: same key goes to the same partition:

```
Pa: A=2, A=1, A=1      Pb: B=1, B=1      Pc: C=1
```

AQE sees most of the 200 shuffle partitions are empty and coalesces to 1.

**Step 3: final `HashAggregate`**: combine partial results:

```
A=4, B=2, C=1
```

Why partial first? Pre-aggregating locally means only small `(key, partial_count)` pairs go over the network instead of every raw row (same idea as a map-side combiner).

`sum` works the same: `partial_sum` → `Exchange(city)` → `sum`.

## 7. Group by with `countDistinct`

Question: in how many distinct cities has each customer transacted?

```python
df_cd = df_transactions.groupBy("cust_id").agg(F.countDistinct("city").alias("n_cities"))
df_cd.show()
```

This DAG is different: **2 exchanges** and **4 hash aggregates**.

```
Scan parquet
   ▼
ColumnarToRow
   ▼
HashAggregate  keys=[cust_id, city], functions=[]                ① local distinct
   ▼
Exchange hashpartitioning(cust_id, city, 200)                    shuffle #1
   ▼
AQEShuffleRead (coalesced → 1)
   ▼
HashAggregate  keys=[cust_id, city], functions=[]                ② global distinct
   ▼
HashAggregate  keys=[cust_id], functions=[partial_count(city)]   ③ partial count
   ▼
Exchange hashpartitioning(cust_id, 200)                          shuffle #2
   ▼
AQEShuffleRead
   ▼
HashAggregate  keys=[cust_id], functions=[count(distinct city)]  ④ final count
```

**Reading tip:** a `HashAggregate` with **grouping keys but an empty function list** is how Spark expresses `DISTINCT`. Here it groups on `(cust_id, city)`, even though you only grouped by `cust_id`, because it is de-duplicating the pairs first.

### Worked example

Rows are `(customer, city)`:

```
P1: (A,1) (A,1) (B,1)      P2: (A,1) (B,2) (B,1)      P3: (B,2) (B,1)
```

**① Local distinct on `(cust_id, city)`**: duplicates add nothing to a distinct count, and dropping them shrinks the shuffle:

```
P1: (A,1) (B,1)            P2: (A,1) (B,2) (B,1)      P3: (B,2) (B,1)
```

**Shuffle #1 on `(cust_id, city)`**: identical pairs land together:

```
(A,1) (A,1)        (B,1) (B,1) (B,1)        (B,2) (B,2)
```

**② Distinct again**: the same pair can arrive from several partitions, so de-dup once more:

```
(A,1)              (B,1)                    (B,2)
```

**③ Partial count per `cust_id`** (local):

```
A=1                B=1                      B=1
```

**Shuffle #2 on `cust_id`**:

```
A: [1]             B: [1, 1]
```

**④ Final count**:

```
A → 1 city,  B → 2 cities
```

### Stages

- 2 shuffles → **3 stages**, and 2 shuffle writes in the Stages tab.

### count/sum vs countDistinct

| | `count` / `sum` | `countDistinct` |
|---|---|---|
| Exchanges | 1 (on group key) | 2 (on `(key, col)`, then on key) |
| HashAggregates | 2 (partial, final) | 4 (distinct, distinct, partial count, final count) |
| Stages | 2 | 3 |
| Cost | Cheaper | More expensive: extra shuffle |

## 8. Cheat sheet: DAG node glossary

| Node | Meaning | What to check |
|---|---|---|
| `Scan parquet` | Reads files | files read, output rows, size; pushed filters |
| `ColumnarToRow` | Columnar batches → rows | input batches |
| `Filter` | Row filter (yours or Spark-added, e.g. `isnotnull` before joins) | output rows vs input |
| `Project` | Select list: columns + expressions (all `withColumn`s end up here) | — |
| `Exchange hashpartitioning(k, n)` | **Shuffle** on key `k` into `n` partitions | shuffle bytes, partition count |
| `AQEShuffleRead` | AQE post-shuffle tweaks | coalesced partitions, skewed partitions split |
| `Sort` | Sort on join key | spill size |
| `SortMergeJoin` | Shuffle + sort + merge join | join type, keys |
| `BroadcastExchange` | Ships small table to all executors | data size (must be small) |
| `BroadcastHashJoin` | Join with no shuffle on big side | build side |
| `HashAggregate` (partial_*) | Local pre-aggregation | — |
| `HashAggregate` (final) | Aggregation after shuffle | — |
| `HashAggregate` (functions=[]) | A `DISTINCT` | — |
| `CollectLimit n` | `show()` / `limit` | n = rows + 1 for `show` |
| `AdaptiveSparkPlan isFinalPlan` | AQE wrapper | `false` in `explain()`, `true` after execution |

## 9. Key takeaways

1. **Actions trigger jobs**, but you may see extra jobs: a metadata job on read, and separate jobs per shuffle map side under AQE.
2. **Stages split at shuffles**: stages = shuffles + 1; tasks = partitions.
3. `show()` reads only a **batch** (≈4,096 rows); use `write.format("noop")` to benchmark the full plan.
4. **Narrow transformations collapse** into `Filter` + a single `Project`; no shuffle, one stage.
5. **Sort merge join** = `Exchange` + `Sort` on both sides; look at `AQEShuffleRead` for coalescing and skew splits.
6. **Broadcast join** removes the shuffle on the large side; look for `BroadcastExchange` + `BroadcastHashJoin`.
7. **Group by** = partial aggregate → shuffle → final aggregate.
8. **countDistinct** costs an extra shuffle: distinct on `(key, col)` first, then count by key.
9. Real jobs are combinations of these patterns; learn to spot each building block in the DAG.

### Interview quick-fire

- *Why does reading a file create a job?* Spark reads metadata (schema, file list, sizes) to plan the query.
- *Why `CollectLimit 6` for `show(5)`?* It fetches one extra row to know if there's more data.
- *Where did my `withColumn`s go in the DAG?* Merged into one `Project`.
- *Why is there a `Filter isnotnull` I didn't write?* Spark adds it before inner joins; null keys can never match.
- *Why 200 shuffle partitions?* Default `spark.sql.shuffle.partitions`; AQE coalesces empty ones at runtime.
- *How many stages for countDistinct?* 3, because it does 2 shuffles.
