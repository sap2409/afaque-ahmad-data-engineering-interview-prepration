# Solving Data Skew with Salting in Spark

Sep 29, 2026 · @Sunil Patil

## 1. What salting is and why skew happens

**Salting** means adding randomness to a key so that its rows spread evenly across partitions. It fixes **data skew**, where one key holds far more rows than the rest.

During any shuffle (a join or a `groupBy`), Spark decides each row's partition with one rule:

```latex
\text{partition} = \text{hash}(\text{key}) \bmod \text{numShufflePartitions}
```

- The same key always produces the same hash, so **every row with that key lands in the same partition**.
- If one key dominates (e.g. 1 million rows of value `1`), that one partition becomes huge.
- The other tasks finish quickly while one task runs for a long time. The whole stage waits on that single straggler.

**Key idea:** change the key from `value` to `(value, salt)`. Now one hot value produces several different hashes, so it spreads across several partitions.

## 2. Concept walkthrough: the "1 million ones" example

With a salt number of 3, the 1M rows of value `1` split into roughly three partitions of \~330K each instead of one partition of 1M.

**Setup:** a dataset joined on column `value`, with 3 shuffle partitions.

| Value | Row count |
| --- | --- |
| 1 | \~1,000,000 |
| 2 | \~5 |
| 3 | \~6 |

**Without salting:** `hash(1) mod 3` gives the same result every time (say 1), so all 1M ones go to one partition. The twos and threes sit in tiny partitions of their own.

**With salting:**

1. Choose a salt number, e.g. **3**. It sets how many pieces a hot key is split into.
2. Add a column `salt` with a random integer in **\[0, 3)**, i.e. 0, 1 or 2, for every row.
3. Join on **`(value, salt)`** instead of `value` alone.

The partition rule becomes `hash(value, salt) mod numShufflePartitions`. Value `1` now produces three different keys, `(1,0)`, `(1,1)` and `(1,2)`, which hash to different partitions (e.g. 0, 1 and 2).

| Partition | Before salting | After salting |
| --- | --- | --- |
| P0 | 1,000,000 ones | \~330K ones + a few others |
| P1 | a few twos | \~330K ones + a few others |
| P2 | a few threes | \~330K ones + a few others |

The split is roughly even, not exact: salts are random, and different keys can still hash to the same partition.

## 3. Salting in joins

A salted join randomly salts the skewed side, replicates the other side once per salt value, then joins on `(value, salt)`.

**Scenario:**

- **Dataset A (skewed):** \~1M rows of value `1`, all in one partition.
- **Dataset B (uniform):** partitions hold 3, 3 and 4 rows, roughly even.

### Step 1: Salt the skewed dataset

- Pick a salt number (here **3**).
- Add a `salt` column with a random integer from 0 (inclusive) to 3 (exclusive).
- Example: row 1 gets salt 0, row 2 gets salt 2, and so on.

### Step 2: Explode the other dataset

- Add an array column containing every salt value: `[0, 1, 2]` (0 to saltNumber − 1).
- `explode` the array so each row becomes **one row per salt value**.
- Example: value `1` becomes `(1,0)`, `(1,1)`, `(1,2)`; value `2` becomes `(2,0)`, `(2,1)`, `(2,2)`.

**Why explode?** Dataset A's salt is random. If B had only one random salt per value too, the salts would rarely match and rows would be lost from the join. Giving B **every possible salt** guarantees that whatever salt a row in A received, a matching row exists in B.

### Step 3: Join on value + salt

- Join condition changes from `value` to **`value` and `salt`**.
- Before: `hash(1) mod 3` sent every `1` to one partition.
- After: `hash(1,0)`, `hash(1,1)`, `hash(1,2)` mod 3 give different partitions, so the ones spread over three partitions.

### Cost to keep in mind

- `explode` multiplies the exploded side's row count by the salt number, which is expensive.
- **Explode the smaller dataset.** If both are similar in size, you have to pick one.

## 4. PySpark: salted join in practice

In the demo, value `0` (\~999K rows) moved from one partition to three partitions of \~332–333K each after salting. The code below reconstructs the video's steps; the video does not show every line verbatim.

### Setup and checking distribution

```python
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("salting").getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", "3")
spark.conf.set("spark.sql.adaptive.enabled", "false")  # so AQE doesn't hide the skew

# Rows per partition: tag each row with its partition id, then count
def show_distribution(df):
    (df.withColumn("partition", F.spark_partition_id())
       .groupBy("partition").count()
       .orderBy("partition").show())
```

- `F.spark_partition_id()` returns the partition id of each row. Grouping by it and counting shows the distribution.
- **Uniform DataFrame:** 12 partitions, all with about the same row count.
- **Skewed DataFrame:** value `0` appears \~999K times, all in partition 0; other values appear only 10–15 times.

### Skewed join (no salting)

```python
joined = df_skew.join(df_uniform, on="value", how="inner")
show_distribution(joined)   # partition 0 holds almost everything
```

### Salted join

```python
SALT = int(spark.conf.get("spark.sql.shuffle.partitions"))   # 3

# Step 1: random salt in [0, SALT) on the skewed side
df_skew_salted = df_skew.withColumn("salt", (F.rand() * SALT).cast("int"))

# Step 2: every salt value on the other side, via array + explode
df_uniform_exploded = df_uniform.withColumn(
    "salt", F.explode(F.array([F.lit(i) for i in range(SALT)]))
)

# Step 3: join on value AND salt
joined_salted = df_skew_salted.join(df_uniform_exploded, on=["value", "salt"], how="inner")

(joined_salted.withColumn("partition", F.spark_partition_id())
    .groupBy("value", "partition").count().show())
```

**Result:** value `0` is spread across partitions 0, 1 and 2 at \~332–333K rows each; the other values are unchanged.

## 5. Salting in aggregations

A salted aggregation runs in two stages: a partial aggregate on `(value, salt)` spreads the heavy work, then a small final aggregate on `value` combines the partial results.

**Goal:** count rows per value in a dataset with 1M zeros and a few ones, twos and threes, using **4** shuffle partitions.

### The problem with a plain group by

- `df.groupBy("value").count()` triggers a shuffle using `hash(value) mod 4`.
- All zeros hash to the same partition, so partition 0 must count 1M rows alone while the others finish almost instantly.

### The salted approach

1. **Choose a salt number** (here **4**) and add a random `salt` in \[0, 4) to each row.
2. **Stage 1: `groupBy(value, salt).count()`.** The shuffle now uses `hash(value, salt) mod 4`. Value `0` becomes four keys, `(0,0)`, `(0,1)`, `(0,2)`, `(0,3)`, landing in up to four partitions with \~250K rows each. Each partition computes a **partial count** in parallel.
3. **Stage 2: `groupBy(value).agg(F.sum("count"))`.** This second shuffle is tiny: each value has at most 4 partial rows. All partial rows for `0` meet in one partition and are summed.

| Stage | Grouping key | Rows for value 0 | Output for value 0 |
| --- | --- | --- | --- |
| Plain group by | `value` | 1,000,000 in one partition | 1 row: count 1M |
| Salted stage 1 | `value, salt` | \~250K in each of 4 partitions | 4 rows: \~250K each |
| Salted stage 2 | `value` | 4 partial rows | 1 row: sum = 1M |

**Why it works:** stage 1 does the heavy lifting (1M rows) in parallel across four tasks. Stage 2 only adds up a handful of numbers. A big job is broken into smaller jobs, and the benefit grows with dataset size.

This works for aggregates that can be combined in pieces: `count` (then `sum`), `sum`, `min`, `max`. An average needs sum and count carried separately, then divided at the end.

## 6. PySpark: salted aggregation in practice

The same count is computed two ways; both return 1M for value `0`, but the salted version spreads the work over four tasks.

```python
SALT = 4

# Without salting: one task counts all the zeros
df.groupBy("value").count().show()

# With salting
# Stage 1: partial counts on (value, salt)
partial = (df.withColumn("salt", (F.rand() * SALT).cast("int"))
             .groupBy("value", "salt")
             .count())

# Stage 2: combine partial counts per value (shuffles only a few rows)
result = (partial.groupBy("value")
                 .agg(F.sum("count").alias("count")))
result.show()
```

- Stage 1 matches the first `groupBy(value, salt).count()` step in the diagram walkthrough.
- Stage 2 shuffles again, but only over the already-summed partial counts, then applies `F.sum`.

## 7. Choosing the salt number and trade-offs

The salt number controls how finely a hot key is split, so choose it deliberately.

- **Too large:** data is cut into many tiny pieces; per-task overhead grows, and in joins the exploded side grows by the same factor.
- **Too small:** the hot key stays packed into a few partitions and skew remains.
- The demo simply set it equal to `spark.sql.shuffle.partitions` (3 for the join, 4 for the aggregation).

|  | Salted join | Salted aggregation |
| --- | --- | --- |
| Salt added to | Skewed side (random) | Every row (random) |
| Other side | Exploded with all salts 0…N−1 | Not applicable |
| New key | `(value, salt)` | `(value, salt)`, then `value` |
| Extra cost | Explode multiplies rows by N | A second, small shuffle |
| Tip | Explode the smaller dataset | Use combinable aggregates (count → sum) |

**Quick recap**

- Skew happens because `hash(key) mod partitions` sends every row of one key to one partition.
- Salting changes the key to `(key, salt)`, giving one hot key several hashes and several partitions.
- The distribution becomes roughly even, not perfectly even.
