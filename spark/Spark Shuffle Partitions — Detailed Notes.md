# Spark Shuffle Partitions — Detailed Notes

Sep 29, 2026 · @Sunil Patil

Source: [Shuffle Partition Spark Optimization: 10x Faster! (YouTube)](https://www.youtube.com/watch?v=q1LtBU_ca20)

## 1. What is shuffling?

Shuffling is how Spark moves related data that is spread across different nodes so it ends up together. Shuffle partitions are the partitions created at the end of that move.

- **When it happens:** during **wide transformations**, where one output partition needs data from many input partitions. Examples are `groupBy`, `join`, `distinct`, `orderBy` and aggregations by key.
- **Narrow transformations** (`filter`, `select`, `withColumn`, `map`) do not shuffle, because each output partition depends on only one input partition.
- **Why it matters:** a shuffle writes data to disk, sends it over the network and reads it back. That makes it one of the most expensive steps in a Spark job.
- **Where you see it:** in the Spark UI, look at the **Shuffle Write** / **Shuffle Read** columns on the Stages tab. You need these numbers to size shuffle partitions.

## 2. Worked example: total sales per store

To total sales per store, Spark first has to get every row for a store into the same partition. That step is the shuffle.

**Dataset:** `store_id` and `sale_amount`. Dates are ignored here.

```python
from pyspark.sql import functions as F

total_sales = df.groupBy("store_id").agg(F.sum("sale_amount").alias("total_sales"))
```

**How it runs:**

1. **Read:** files load into partitions P1–P4. Each partition holds a mix of stores. For example, P1 has rows for S1, S3, S2 and S4.
2. **Shuffle:** Spark moves the rows so each store sits in one partition. After the shuffle, P1 holds only S1, P2 only S2, P3 only S3 and P4 only S4.
3. **Group and aggregate:** each partition adds up `sale_amount` for its own store. No other data is needed.

| Partition | Before shuffle | After shuffle |
| --- | --- | --- |
| P1 | S1, S3, S2, S4 | S1 only |
| P2 | Mixed stores | S2 only |
| P3 | Mixed stores | S3 only |
| P4 | Mixed stores | S4 only |

The partitions after the shuffle (P1–P4 in the right-hand column) are the **shuffle partitions**.

## 3. Why the shuffle partition count matters

The number of shuffle partitions caps how many cores can work on a shuffle stage at the same time. If it is wrong, a big cluster sits mostly idle.

**Key rule:** one partition is processed by one core at a time.

**Example:** a 1,000-core cluster with the default setting of 200 shuffle partitions.

- A `groupBy` or `join` produces 200 shuffle partitions, so only **200 cores** work.
- The other **800 cores sit idle** for that stage.

**What this costs you:**

1. **Slow jobs:** each busy core handles a bigger slice of data, so the stage takes longer.
2. **Wasted cluster:** you pay for 1,000 cores and use 20% of them.

The same problem runs the other way. Too many tiny partitions add scheduling and task overhead. The aim is to match the partition count to both the **data size** and the **number of cores**.

## 4. The setting and the tuning formula

You set the shuffle partition count with `spark.sql.shuffle.partitions`, which defaults to **200**. Size it from the shuffle data volume and your core count.

```python
spark.conf.set("spark.sql.shuffle.partitions", 1500)
```

**Size of each partition:**

```latex
\text{Size per shuffle partition} = \frac{\text{Total shuffle data}}{\text{Number of shuffle partitions}}
```

**Partition count for a target size:**

```latex
\text{Number of shuffle partitions} = \frac{\text{Total shuffle data}}{\text{Target partition size}}
```

**Inputs:**

- **Total shuffle data:** read the Shuffle Write size from the Spark UI Stages tab.
- **Total cores:** executors × cores per executor.
- **Target partition size:** the video gives **1–200 MB** per shuffle partition. A common rule of thumb is to aim for around **100–200 MB** when the data is large.

## 5. Scenario 1: each partition holds too much data

Fix: raise the partition count from 200 to **1,500**. Each partition then holds about 200 MB instead of 1.5 GB.

| Given | Value |
| --- | --- |
| Executors | 5 |
| Cores per executor | 4 |
| Total cores | 5 × 4 = **20** |
| Shuffle data (Shuffle Write) | **300 GB** |
| `spark.sql.shuffle.partitions` | 200 (default) |

**Diagnosis:** 300 GB ÷ 200 = **1.5 GB per partition**. That is far above the 1–200 MB range. Each core has to chew through a huge slice, which risks spills to disk, memory pressure and slow tasks.

**Fix:** 300 GB ÷ 200 MB = **1,500 partitions**.

```python
spark.conf.set("spark.sql.shuffle.partitions", 1500)
```

**Result:** each task now handles about 200 MB, a workload one core handles comfortably. With 20 cores, the 1,500 tasks run in waves of 20 at a time.

Note: the transcript says "300 MB" once, but the math (1.5 GB per partition) only works for 300 GB.

## 6. Scenario 2: each partition holds too little data

Fix: cut the partition count from 200 to **12**, one per core. Every core gets about 4.2 MB and none sits idle.

| Given | Value |
| --- | --- |
| Executors | 3 |
| Cores per executor | 4 |
| Total cores | 3 × 4 = **12** |
| Shuffle data (Shuffle Write) | **50 MB** |
| `spark.sql.shuffle.partitions` | 200 (default) |

**Diagnosis:** 50 MB ÷ 200 = **250 KB per partition**. That is far too small. Spark spends more time scheduling 200 tiny tasks than it spends processing data.

**Two options:**

| Option | How | Partitions | Data per partition | Cores used | Trade-off |
| --- | --- | --- | --- | --- | --- |
| A: size-based | 50 MB ÷ 10 MB target | 5 | 10 MB | 5 of 12 | Good partition size, but 7 cores sit idle |
| B: core-based (preferred) | 50 MB ÷ 12 cores | 12 | \~4.2 MB | 12 of 12 | Every core works, so the job finishes faster |

```python
spark.conf.set("spark.sql.shuffle.partitions", 12)
```

**Takeaway:** for small data, match the partition count to the core count, or a small multiple of it. Then every core has work and each task stays light.

## 7. When tuning isn't enough: data skew

If a job is still slow after you tune the partition count, check for **data skew**. The partition count can't fix skew on its own.

**What skew looks like:** a few keys own most of the rows. Picture a join or `groupBy` where one store ID or customer ID has millions of rows. All rows for a key go to the same partition, so a few partitions get huge. A handful of cores grind through them while the rest finish early and sit idle.

**How to spot it:** on a stage in the Spark UI, most tasks finish fast and one or two run far longer. Their Shuffle Read size is also much larger than the median.

**Fixes:**

- **AQE (Adaptive Query Execution):** Spark re-plans at runtime using real shuffle statistics. It can merge small partitions and split skewed ones in joins. It is on by default in Spark 3.2+.

  ```python
  spark.conf.set("spark.sql.adaptive.enabled", "true")
  spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
  spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
  ```
- **Salting:** add a random suffix to hot keys, such as `S1_0` to `S1_9`. This spreads a key's rows across several partitions. Aggregate on the salted key first, then aggregate again on the original key.

## 8. Quick reference

| Situation | Symptom | What to do |
| --- | --- | --- |
| Large shuffle (e.g., 300 GB, 200 partitions) | \~1.5 GB per partition, spills, slow tasks | Raise partitions: data ÷ \~200 MB (1,500) |
| Small shuffle (e.g., 50 MB, 200 partitions) | \~250 KB per partition, overhead dominates | Lower partitions to about the core count (12) |
| Partitions < cores (e.g., 200 on 1,000 cores) | Most cores idle | Raise partitions to at least the core count |
| A few tasks run far longer than the rest | Data skew | Enable AQE skew join, or salt hot keys |

**Checklist:**

1. Read the **Shuffle Write** size in the Spark UI.
2. Work out **total cores**: executors × cores per executor.
3. Compute **data ÷ partitions** and compare it to the 1–200 MB target.
4. Set `spark.sql.shuffle.partitions` so partitions are well sized **and** every core has work.
5. Still slow? Check for **skew**, then use AQE or salting.
