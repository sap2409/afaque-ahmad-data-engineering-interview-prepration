# Solving Data Skew in Spark: AQE & Broadcast Joins

Sep 29, 2026 · @Sunil Patil

## Overview

Two techniques fix data skew in Spark joins: **Adaptive Query Execution (AQE)**, introduced in Spark 3.0, and **broadcast joins**. In the video's demo, the broadcast join ran in about 3.2 s versus about 10.9 s with AQE, roughly one-third of the time.

- **Data skew** = one join key (e.g. one customer ID) has far more rows than others, so the partition holding it becomes huge and one task runs much longer than the rest.
- **AQE** fixes skew automatically by splitting oversized partitions in a sort merge join at runtime.
- **Broadcast join** avoids the problem entirely by never shuffling the large table by join key.
- A third technique, **salting**, is covered in a separate video.

## Adaptive Query Execution (AQE)

AQE lets Spark re-plan a query at runtime using statistics collected while it runs, instead of relying only on estimates made before execution. Runtime statistics include the number of bytes read (input size) and the number of partitions.

| Optimization | What AQE does | Why it helps |
| --- | --- | --- |
| 1. Coalescing shuffle partitions | Merges empty or tiny shuffle partitions. Example: 200 default shuffle partitions but only 15 distinct join keys, so 185 are empty; AQE coalesces them into 15. | Fewer partitions = fewer tasks = fewer resources wasted on empty work. |
| 2. Switching join strategy | Converts a sort merge join into a broadcast join when one side turns out small enough at runtime. | Sort merge joins start with a shuffle (data moved across the network); broadcast joins need no shuffle. |
| 3. Optimizing skewed joins | Splits oversized (skewed) partitions of a sort merge join into smaller sub-partitions. | Each piece can be processed in parallel by different cores, so no single task dominates runtime. |

The third optimization is the one that directly addresses data skew.

## Enabling AQE skew-join handling

Set both properties to `true`; Spark then splits skewed partitions in sort merge joins dynamically.

```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
```

| Property | Purpose |
| --- | --- |
| `spark.sql.adaptive.enabled` | Turns AQE on as a whole |
| `spark.sql.adaptive.skewJoin.enabled` | Lets AQE detect and split skewed partitions in sort merge joins |

Supplementary (not in the video): AQE is on by default since Spark 3.2. A partition counts as skewed when it exceeds both `spark.sql.adaptive.skewJoin.skewedPartitionFactor` (default 5) × the median partition size and `spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes` (default 256 MB).

## AQE demo: transactions × customers

With AQE on, total join time barely changed (11.8 s → 10.9 s), but work was spread far more evenly across tasks.

**Setup**

- `transactions`: customer ID, start date, transaction ID, other details.
- `customers`: customer ID, customer details, home city.
- Join key: `customer_id`. One customer ID has far more transactions than the rest, so its partition is skewed.
- Broadcast disabled to force a sort merge join:

```python
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
joined = transactions.join(customers, "customer_id")
```

**Without AQE (Spark UI)**

- Event timeline shows one skewed partition whose executor compute time dwarfs the others.
- Query plan: Scan parquet → Exchange (shuffle, 200 partitions each side) → Sort → SortMergeJoin.

**With AQE (Spark UI)**

- Query plan adds an **AQEShuffleRead** step after the Exchange: it reads 4 partitions instead of 200.
- Fewer partitions → fewer tasks → fewer resources needed.

| Metric | Without AQE | With AQE |
| --- | --- | --- |
| Total join time | \~11.8 s | \~10.9 s |
| Partitions after shuffle | 200 | 4 (coalesced) |
| Min task time | 11 ms | 2 s |
| Max task time | 7 s | 7 s |
| Min–max gap | \~7 s | \~5 s |

**Reading the task times:** a wide min–max gap means most cores finish early (around the 75th percentile) and sit idle while a few cores grind through the skewed partition. AQE narrows that gap, so cores stay busy.

**Caveat:** AQE is not always enough. Many cases still need manual tuning, such as broadcast joins or salting.

## How a sort merge join works

A sort merge join runs three steps, and the first one, the shuffle, is the costliest because it moves data across the network.

1. **Shuffle:** rows from both tables are redistributed so that rows with the same join key land in the same partition.
2. **Sort:** each partition is sorted by the join key.
3. **Merge:** two pointers walk the sorted partitions of both tables; when the keys at both pointers match, the rows are joined.

**Worked example**

- 3 executors; `transactions` and `customers` each have one partition per executor (P0, P1, P2).
- Join on `transactions.customer_id = customers.customer_id`, with 3 shuffle partitions.
- Simplified routing rule: partition = key mod 3. Customer ID 3 → 3 mod 3 = 0 → P0; ID 6 → P0; IDs 1, 19 → P1 (remainder 1); ID 5 → P2 (remainder 2).
- After the shuffle, matching keys from both tables sit in the same partition; after sorting, the merge pairs them (e.g. 6 with 6) to produce the joined rows.

**The real partitioning rule**

Spark hashes the key first, so any column type (integer, string, array, or several columns together) becomes an integer, then takes the modulo to fit the bucket count:

```latex
\text{partition} = \text{hash}(\text{join keys}) \bmod \text{spark.sql.shuffle.partitions}
```

**Why this causes skew:** every row with the same key goes to the same partition. If one customer ID has millions of rows, that one partition, and the one task processing it, becomes the bottleneck.

## How a broadcast join works, and why it is immune to skew

A broadcast join sends a full copy of the small table to every executor, so the large table is never shuffled by the join key and can be split evenly.

- Use it when one table is much smaller than the other (e.g. `customers` vs `transactions`).
- Each executor keeps its slice of `transactions` and joins it locally against its copy of `customers`; the result is the same as the sort merge join.
- No shuffle and no sort of the large table, so the costliest step disappears.

**Why it is immune to skew:** skew only arises when data is partitioned by the join key, which forces every row of a hot key into one partition. In a broadcast join, the large table does not have to be partitioned by the join key at all. You can simply `repartition(3)` it into even slices, and each slice still finds its matches in the local copy of the small table.

&#91;embedded content: sort merge join vs broadcast join · 3 executors\]

Top: the shuffle forces the hot key into P0. Bottom: even slices plus a broadcast copy give every task the same amount of work.

## Broadcast join demo

On the same transactions × customers join, the broadcast join finished in about 3.2 s, roughly one-third of the AQE run.

```python
# Broadcast any table smaller than 10 MB
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10 * 1024 * 1024)
joined = transactions.join(customers, "customer_id")

# Or force it explicitly
from pyspark.sql.functions import broadcast
joined = transactions.join(broadcast(customers), "customer_id")
```

| Approach | Join type | Approx. time |
| --- | --- | --- |
| No AQE | Sort merge join (skewed) | \~11.8 s |
| AQE on | Sort merge join, skew split | \~10.9 s |
| Broadcast | Broadcast hash join | \~3.2 s |

The explicit `broadcast()` hint is a common alternative not shown in the video.

## Quick reference

Reach for a broadcast join when one side is small; fall back to AQE (and then salting) when both sides are large.

|  | AQE skew join | Broadcast join |
| --- | --- | --- |
| How it fixes skew | Splits oversized partitions at runtime | Never partitions the large table by join key |
| Shuffle of large table | Yes | No |
| Best when | Both tables are large | One table is small enough to fit in executor memory |
| Effort | Two config flags | One config or a `broadcast()` hint |
| Limitation | May not fully fix severe skew | Small table must fit in memory on every executor |

**Configs at a glance**

| Property | Value used in video | Effect |
| --- | --- | --- |
| `spark.sql.adaptive.enabled` | `true` | Enables AQE |
| `spark.sql.adaptive.skewJoin.enabled` | `true` | Enables skewed-partition splitting |
| `spark.sql.autoBroadcastJoinThreshold` | `-1` | Disables auto broadcast (forces sort merge join) |
| `spark.sql.autoBroadcastJoinThreshold` | `10 MB` | Broadcasts tables smaller than 10 MB |
| `spark.sql.shuffle.partitions` | `200` (default) | Number of partitions after a shuffle |

**Spark UI signs of skew:** one task far longer than the rest in the event timeline; a large gap between min and max task duration in the stage summary.
