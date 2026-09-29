# Data Skew in Apache Spark — Detailed Notes

Sep 29, 2026 · @Sunil Patil

## 1. What is data skew

Data skew means your data is **unevenly partitioned**: some partitions hold far more data than others. Almost everyone writing Spark jobs runs into it.

- Spark processes data partition by partition, with one task per partition on one core.
- If one partition is much bigger, the task working on it runs much longer than the rest.
- The whole stage cannot finish until that slowest task finishes, so one big partition holds up the entire job.

## 2. How to spot skew in the Spark UI

The Spark UI gives you three clear signs of skew.

| Where to look | What you see | What it means |
| --- | --- | --- |
| Job / stage progress | The job gets stuck on the very last task. In the video's example, the last task started at about minute 12 and ran until about 1.5 hours. | One partition holds far more data than the others. |
| Stage → Event Timeline | Time runs along the x-axis and each row is a partition. Most tasks finish early, but one has a very long green bar (executor computing time). | That one partition is the skewed one. |
| Stage → Summary Metrics for tasks | The minimum task duration is 5 seconds; the maximum is 31 minutes. | A huge min–max gap means some partitions hold much more data. |

**Tip:** In Summary Metrics, compare the median with the max for Duration, Shuffle Read Size and Records. If the max is many times larger than the median, you have skew.

## 3. How skew wastes resources: the 5-core example

With skew, most cores sit idle while one core does all the remaining work, and you still pay for every core.

**Setup:**

- 1 executor with 5 cores and 10 GB of RAM, so each core gets about 2 GB.
- The data is split into 5 partitions, P1 to P5. P3 is the largest and P2 is the smallest.
- Each core gets one partition: Core 0 → P1, Core 1 → P2, Core 2 → P3, Core 3 → P4, Core 4 → P5.

**What happens:**

1. Cores 0, 1, 3 and 4 finish their partitions quickly.
2. Core 2 is still working through the large P3.
3. For that gap, 4 of the 5 cores are idle while one core works alone.
4. Result: uneven use of resources, and you pay for cores that do nothing.

**Ideal scenario:** The data is spread evenly across partitions. Every core finishes at about the same time and no core sits idle.

## 4. Operations that cause skew

Skew usually appears after a shuffle. Rows with the same key go to the same partition, so a very common key makes one very large partition.

### a) Aggregation (groupBy)

- **Example:** A transactions dataset. Goal: count transactions per country.
- **Code idea:** `df.groupBy("country").count()`
- **Problem:** Country C4 has far more transactions than the others. Its partition is skewed, so the core that processes it takes much longer.

### b) Join

- **Example:** Join `order_line` with `products` on `product_id` to get order lines with product details.
- **Check:** Count rows per join key. Product P2 has far more rows than the others.
- **Problem:** The join is skewed at P2. The core that processes P2's partition takes much longer than the others.

**Rule of thumb:** Before a groupBy or join, count rows per key (`df.groupBy(key).count().orderBy(desc("count"))`). A few keys with huge counts mean skew is coming.

## 5. Why data skew is bad

Skew costs you time, money and stability.

1. **Slow jobs and developer time.** Jobs take longer, and you spend time debugging and fixing them.
2. **Idle, uneven resources.** One core is fully used while the others sit idle after they finish, and you still pay for all of them.
3. **Out-of-memory errors and disk spills.** One partition may not fit in a core's share of memory (about 2 GB in the example). Spark then either fails with OOM or spills to disk. Spills are expensive because Spark writes data to disk and reads it back.

## 6. Hands-on demo (notebook)

The demo compares a uniform dataset with a skewed one, then shows a skewed join in the Spark UI. The code below is rebuilt from the narration, so the exact numbers are only examples.

### a) Uniform dataset

`spark.range()` makes a one-column dataset. `spark_partition_id()` (from `pyspark.sql.functions`) returns the partition ID of each row. Group by that ID to count rows per partition.

```python
from pyspark.sql import functions as F

df = spark.range(0, 1_000_000, numPartitions=8)
(df.withColumn("partition", F.spark_partition_id())
   .groupBy("partition").count()
   .orderBy("partition")
   .show())
```

**Result:** Every partition has about the same number of rows. The data is uniform.

### b) Skewed dataset

Union three DataFrames where the first is much larger and is forced into a single partition with `repartition(1)`.

```python
df1 = spark.range(0, 1_000_000).repartition(1)   # big, in one partition
df2 = spark.range(0, 10_000).repartition(4)
df3 = spark.range(0, 10_000).repartition(4)

skewed = df1.union(df2).union(df3)
(skewed.withColumn("partition", F.spark_partition_id())
       .groupBy("partition").count()
       .orderBy("partition")
       .show())
```

**Result:** Partition 0 has far more rows than the others. The data is skewed.

### c) Skewed join: transactions × customers

- **Transactions:** customer\_id, transaction\_id, transaction time, and so on.
- **Customers:** customer\_id plus customer details.
- **Join key:** customer\_id.

**Step 1 – check the key distribution before joining.** Count rows per customer\_id, which is about the number of transactions per customer.

```python
(transactions.groupBy("customer_id").count()
             .orderBy(F.desc("count"))
             .show(10))
```

One customer\_id has far more rows than the rest. Whichever partition gets that key will be skewed.

**Step 2 – turn off broadcast join to force a full shuffle.**

```python
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
joined = transactions.join(customers, on="customer_id", how="inner")
joined.count()   # action to trigger the job
```

**Step 3 – look at the Spark UI.**

- Two stages read the two DataFrames.
- The join stage runs with 200 partitions, the default for `spark.sql.shuffle.partitions`.
- That stage runs much longer than expected.
- Its **Event Timeline** shows one task with a very long executor computing time. That task is the skewed partition.

## 7. How to fix skew (covered in follow-up videos)

The video names three fixes: AQE, broadcast join and salting. It covers them in separate videos. The short summaries below are added for reference; they are not from this transcript.

| Fix | Idea | When to use | Key setting / pattern |
| --- | --- | --- | --- |
| AQE (Adaptive Query Execution) | At runtime, Spark finds very large shuffle partitions in a join and splits them into smaller ones. | Spark 3.x sort-merge joins. This is the easiest first step. | `spark.sql.adaptive.enabled=true`, `spark.sql.adaptive.skewJoin.enabled=true` |
| Broadcast join | Send the small table to every executor so the large table is never shuffled, which avoids the skew. | One side of the join is small enough to fit in memory. | `F.broadcast(small_df)` or `spark.sql.autoBroadcastJoinThreshold` |
| Salting | Add a random "salt" to the hot key so its rows spread across many partitions. Copy the other side once per salt value. | Both sides are large, or you have a skewed groupBy. | Key becomes `key + "_" + rand(0..N-1)`; aggregate in two stages for groupBy |

## 8. Quick revision

- **Definition:** Data skew is uneven data across partitions.
- **Symptom:** The job stalls on its last task, while the other tasks finished long ago.
- **Where to check:** Spark UI → stage Event Timeline (one long green bar) and Summary Metrics (a huge min vs max gap).
- **Causes:** Shuffles on a hot key, mainly in `groupBy` aggregations and joins.
- **Costs:** Slow jobs, time spent debugging, idle cores you still pay for, OOM errors and disk spills.
- **Diagnose early:** Use `spark_partition_id()` to see rows per partition, and `groupBy(key).count()` to find hot keys.
- **Fixes:** AQE skew join, broadcast join and salting.
