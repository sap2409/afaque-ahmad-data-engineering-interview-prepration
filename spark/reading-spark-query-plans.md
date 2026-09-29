# Reading Spark Query Plans: Study Notes

Source: "Master Reading Spark Query Plans" (YouTube, https://www.youtube.com/watch?v=KnUXztKueMU). These notes follow the video's order and clean up transcription errors. Plan snippets are illustrative and simplified, so exact output will differ by Spark version.

## Contents

1. Why query plans matter
2. Setup and datasets
3. How Spark generates a plan
4. Reading `explain()` output
5. Narrow transformations
6. Repartition and round robin partitioning
7. AQE and `isFinalPlan`
8. Coalesce vs repartition
9. Sort merge join and hash partitioning
10. Group by and hash aggregates (count, sum, count distinct)
11. Pushed filters and why the filter appears twice
12. When predicate pushdown fails
13. Key takeaways

---

## 1. Why query plans matter

- A query plan shows what Spark actually does under the hood for your code.
- You need to read plans before attempting any Spark optimization: it tells you where shuffles, scans, filters and aggregations happen.

## 2. Setup and datasets

Two Parquet datasets are used in every example:

| Dataset | Key columns |
|---|---|
| `df_transactions` | `cust_id`, `txn_id`, `amt`, `city`, other transaction details |
| `df_customers` | `cust_id`, `name`, `age`, `gender`, `birthday`, `zip`, `city` |

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("query-plans").getOrCreate()

df_transactions = spark.read.parquet("data/transactions")
df_customers    = spark.read.parquet("data/customers")
```

## 3. How Spark generates a plan

```
Your code (DataFrame / SQL)
   │  syntax check
   ▼
Unresolved logical plan       ← "Parsed Logical Plan" in explain()
   │  resolve against the Catalog
   ▼
Logical plan                  ← "Analyzed Logical Plan"
   │  Catalyst Optimizer (rule-based rewrites)
   ▼
Optimized logical plan        ← "Optimized Logical Plan"
   │  generate candidate physical plans
   ▼
Several physical plans
   │  cost model picks the cheapest
   ▼
Selected physical plan  ──►  executed on the cluster
```

**Step by step**

1. **Syntax check → unresolved logical plan.** Spark parses the code and confirms it is syntactically valid. Table and column names are not verified yet.
2. **Analysis with the Catalog → (analyzed) logical plan.** The catalog is Spark's internal metadata store: which tables and DataFrames exist, their columns, and data types. For `SELECT a, b FROM table1`, Spark checks that `table1` exists and has columns `a` and `b`.
3. **Catalyst Optimizer → optimized logical plan.** Applies rewrite rules, for example:
   - **Projection pushdown (column pruning):** you wrote `select *` but only use 5 columns in the whole query, so Spark reads only those 5.
   - **Filter (predicate) pushdown:** a filter written in the middle of the query is moved down to the data source read, so fewer rows are returned from the start.
4. **Physical planning.** The optimized logical plan is converted into several candidate physical plans (for example, different join strategies).
5. **Cost model.** Spark estimates the cost of each candidate and picks the most efficient one. That physical plan is what runs on the cluster.

## 4. Reading `explain()` output

```python
df.explain(True)   # extended: all four plans
df.explain()       # physical plan only
```

`explain(True)` prints four sections that map to the pipeline above:

| explain() section | Pipeline stage |
|---|---|
| Parsed Logical Plan | Unresolved logical plan |
| Analyzed Logical Plan | Logical plan (after catalog resolution) |
| Optimized Logical Plan | After Catalyst |
| Physical Plan | Plan chosen by the cost model, executed on the cluster |

The video focuses on the **physical plan**, because that is what actually executes.

**Read physical plans from bottom to top.** The bottom node (usually a file scan) happens first.

## 5. Narrow transformations

Narrow transformations do **not** require a shuffle. Examples: `filter`, `withColumn` (add or change a column), `select`.

```python
df_narrow = (
    df_customers
    .filter(F.col("city") == "boston")
    .withColumn("first_name", F.split("name", " ").getItem(0))
    .withColumn("last_name",  F.split("name", " ").getItem(1))
    .withColumn("age", F.col("age") + F.lit(5))
    .select("cust_id", "first_name", "last_name", "age", "gender", "birthday")
)
df_narrow.explain(True)
```

Physical plan (simplified):

```
*(1) Project [cust_id, split(name, ' ')[0] AS first_name,
              split(name, ' ')[1] AS last_name, (age + 5) AS age, gender, birthday]
+- *(1) Filter (isnotnull(city) AND (city = boston))
   +- *(1) ColumnarToRow
      +- FileScan parquet [cust_id, name, age, gender, birthday, city]
           PushedFilters: [IsNotNull(city), EqualTo(city,boston)]
```

Reading it bottom-up:

1. **FileScan parquet**: reads the customers Parquet file and lists the columns it will read.
2. **ColumnarToRow**: Parquet is a columnar format. Spark converts to row format because the downstream transformations are easier to run row by row.
3. **Filter**: our `city == "boston"` filter. Spark added `isnotnull(city)` on its own as part of optimization.
4. **Project**: all the `withColumn` and `select` steps collapse into a single Project. Each derived column appears alongside the expression that computes it (`split(...)[0]` for first name, `split(...)[1]` for last name, `age + 5` for age), plus the other selected columns.

No `Exchange` node appears, which confirms there is no shuffle.

## 6. Repartition and round robin partitioning

`repartition(n)` redistributes data into `n` partitions. It is a **wide** operation and always shuffles.

```python
df_transactions.rdd.getNumPartitions()   # 13 in the video

df_txn_repart = df_transactions.repartition(24)
df_txn_repart.explain(True)
```

Physical plan (simplified):

```
AdaptiveSparkPlan isFinalPlan=false
+- Exchange RoundRobinPartitioning(24), REPARTITION_BY_NUM
   +- FileScan parquet [cust_id, txn_id, amt, city, ...]
```

- **FileScan**: read the file first.
- **Exchange**: always means a **shuffle**.
- **RoundRobinPartitioning(24)**: the partitioning scheme; 24 is the target count.

**How round robin works:** rows are dealt out one at a time across partitions, like dealing cards.

```
Row 0 → P1
Row 1 → P2
Row 2 → P3
...
Row 23 → P24
Row 24 → P1   (wraps around)
...
```

The result is roughly equal-sized partitions, with no relationship between a row's values and its partition.

## 7. AQE and `isFinalPlan`

The top line `AdaptiveSparkPlan isFinalPlan=false` comes from **Adaptive Query Execution (AQE)**.

- AQE re-optimizes the plan at runtime using **runtime statistics** (bytes read, number and size of partitions, and so on).
- `isFinalPlan=false` means the query has not run yet. Spark is saying: "this is my current plan, but I may pick a different one at runtime based on the statistics I observe."
- After an action runs, the plan (for example in the Spark UI) shows `isFinalPlan=true`. The final plan usually looks broadly similar.

## 8. Coalesce vs repartition

Both can **reduce** the number of partitions. The difference is shuffling.

| | `repartition(n)` | `coalesce(n)` |
|---|---|---|
| Direction | Increase or decrease | Decrease only |
| Shuffle | Always | Tries to avoid one |
| Partitioning scheme in plan | Yes (e.g. RoundRobinPartitioning) | None |
| Resulting partition sizes | Roughly even | Can be uneven |

**How coalesce avoids a shuffle: it merges partitions that sit on the same executor.**

Setup: 3 executors holding 3, 4 and 2 partitions (9 total).

```
E1: [p][p][p]     E2: [p][p][p][p]     E3: [p][p]
```

- **`coalesce(3)`**: merge everything within each executor → E1: 1, E2: 1, E3: 1. No data moves between executors.
- **`coalesce(4)`**: e.g. E1 merges into 1, E3 merges into 1, E2 merges its 4 into 2 → 4 partitions total. Still no cross-executor movement.
- **`coalesce(2)`**: with 3 executors you cannot end up with 2 partitions by merging locally only. One executor's data (say E3) must move to E1 and/or E2, and then each of those merges locally. This **does move data**, so aggressive reductions can involve data transfer.

Code:

```python
df_transactions.rdd.getNumPartitions()   # 13
df_txn_coalesce = df_transactions.coalesce(1)
df_txn_coalesce.explain(True)
```

```
Coalesce 1
+- *(1) ColumnarToRow
   +- FileScan parquet [cust_id, txn_id, amt, city, ...]
```

Key point: **repartition shows a partitioning scheme; coalesce does not.** There is no partitioning scheme because there is no Exchange (shuffle) node, and a partitioning scheme only exists to decide where shuffled rows go.

## 9. Sort merge join and hash partitioning

Broadcast joins are disabled so Spark uses a sort merge join:

```python
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

df_joined = df_transactions.join(df_customers, on="cust_id", how="inner")
df_joined.explain(True)
```

Physical plan (simplified):

```
AdaptiveSparkPlan isFinalPlan=false
+- Project [cust_id, txn_id, amt, ..., name, age, ...]
   +- SortMergeJoin [cust_id], [cust_id], Inner
      :- Sort [cust_id ASC]
      :  +- Exchange hashpartitioning(cust_id, 200)
      :     +- Filter isnotnull(cust_id)
      :        +- FileScan parquet transactions  PushedFilters: [IsNotNull(cust_id)]
      +- Sort [cust_id ASC]
         +- Exchange hashpartitioning(cust_id, 200)
            +- Filter isnotnull(cust_id)
               +- FileScan parquet customers     PushedFilters: [IsNotNull(cust_id)]
```

Reading bottom-up, for **each** side of the join:

1. **FileScan** the dataset.
2. **Filter isnotnull(cust_id)**: added by Spark, not by us. Null keys can never match in an inner join, so they are dropped early.
3. **Exchange hashpartitioning(cust_id, 200)**: shuffle into 200 partitions (the default `spark.sql.shuffle.partitions`) using hash partitioning, so the same keys end up in the same partition.
4. **Sort** on `cust_id` within each partition.

Then:

5. **SortMergeJoin** merges the two sorted sides.
6. **Project** selects the output columns.

### How hash partitioning works

```
partition = hash(key) mod num_shuffle_partitions
```

`hash(key)` is usually a large number, so `mod N` maps it to a partition in the range `0 .. N-1`.

Worked example with 4 shuffle partitions, and assuming `hash(k) = k` for simplicity:

| Key | hash mod 4 | Partition |
|---|---|---|
| 1 | 1 | P1 |
| 2 | 2 | P2 |
| 3 | 3 | P3 |
| 4 | 0 | P0 |
| 5 | 1 | P1 |
| 6 | 2 | P2 |

Transactions (`1, 1, 2, 2, 3, 4, 5, 6, ...`) and customers (`1, 2, 3, ...`) both go through the **same** function, so `cust_id = 1` from both datasets lands in the same partition number. That is what makes the join possible without comparing every row with every other row.

After the shuffle, each partition is **sorted** by key on both sides. The join then walks both sorted lists in step, comparing keys and emitting a joined row when they match.

The same scheme is used for `groupBy`: the group by key is hashed instead of the join key.

## 10. Group by and hash aggregates

### 10a. `groupBy().count()`

```python
df_city_counts = df_transactions.groupBy("city").count()
df_city_counts.explain(True)
```

```
HashAggregate(keys=[city], functions=[count(1)])                    ← final count
+- Exchange hashpartitioning(city, 200)                             ← shuffle
   +- HashAggregate(keys=[city], functions=[partial_count(1)])      ← local count
      +- FileScan parquet [city]
```

**Worked example.** Three partitions on executors E1, E2, E3:

```
E1: a, a, b        E2: b, b, c        E3: a
```

**Step 1: partial count (before the shuffle).** Each partition counts locally:

```
E1: (a,2), (b,1)   E2: (b,2), (c,1)   E3: (a,1)
```

**Step 2: shuffle by `city`.** Same keys go to the same partition:

```
P1: (a,2), (a,1)   P2: (b,1), (b,2)   P3: (c,1)
```

**Step 3: final count** (sum of the partial counts):

```
a = 3, b = 3, c = 1
```

Why this matters: shuffling one small `(key, partial_count)` pair per key per partition is much cheaper than shuffling every raw row. It is divide and conquer, and it works like a map-side combiner.

### 10b. `groupBy().agg(sum)`

```python
df_city_amt = df_transactions.groupBy("city").agg(F.sum("amt").alias("txn_amt"))
df_city_amt.explain(True)
```

```
HashAggregate(keys=[city], functions=[sum(amt)])
+- Exchange hashpartitioning(city, 200)
   +- HashAggregate(keys=[city], functions=[partial_sum(amt)])
      +- FileScan parquet [city, amt]
```

This is the same shape as count: a **partial sum** per city within each partition, a shuffle by city, then a **final sum**.

### 10c. `countDistinct`: four hash aggregates and two shuffles

Goal: for each customer, the number of distinct cities they transacted in.

```python
df_cust_cities = (
    df_transactions
    .groupBy("cust_id")
    .agg(F.countDistinct("city").alias("city_count"))
)
df_cust_cities.explain(True)
```

```
HashAggregate(keys=[cust_id], functions=[count(distinct city)])            ← 4. final count
+- Exchange hashpartitioning(cust_id, 200)                                  ← shuffle #2
   +- HashAggregate(keys=[cust_id], functions=[partial_count(distinct city)])  ← 3. partial count
      +- HashAggregate(keys=[cust_id, city], functions=[])                  ← 2. global distinct
         +- Exchange hashpartitioning(cust_id, city, 200)                   ← shuffle #1
            +- HashAggregate(keys=[cust_id, city], functions=[])            ← 1. local distinct
               +- FileScan parquet [cust_id, city]
```

The idea: deduplicate locally, shuffle, deduplicate globally, then count (partial, then final).

A **HashAggregate with keys but an empty `functions=[]` list is a DISTINCT**: it groups by the keys and computes nothing, so the output is the unique key combinations.

**Worked example.** Notation `A1` means customer A transacted in city 1.

```
P1: A1, A1, B1     P2: B1, B2     P3: A1, A2
```

**Step 1: HashAggregate(keys=[cust_id, city], functions=[]), a local distinct.** Note that the keys are `(cust_id, city)`, not just `cust_id` as written in the code.

```
P1: A1, B1         P2: B1, B2     P3: A1, A2
```

**Shuffle #1: Exchange hashpartitioning(cust_id, city).** Identical pairs land together:

```
[A1, A1]   [A2]   [B1, B1]   [B2]
```

**Step 2: HashAggregate(keys=[cust_id, city], functions=[]), a global distinct.** Customer A was in city 1 twice across partitions, but it should only count once:

```
[A1]   [A2]   [B1]   [B2]
```

**Step 3: HashAggregate(keys=[cust_id], functions=[partial_count(distinct city)]).** The key is now just `cust_id`. Each partition counts locally:

```
(A,1)   (A,1)   (B,1)   (B,1)
```

**Shuffle #2: Exchange hashpartitioning(cust_id).** Bring each customer's partial counts together:

```
[(A,1), (A,1)]   [(B,1), (B,1)]
```

**Step 4: HashAggregate(keys=[cust_id], functions=[count(distinct city)]), the final count.**

```
A → 2,   B → 2
```

| Step | Keys | Function | Purpose |
|---|---|---|---|
| 1 | cust_id, city | (none) | Local distinct |
| Shuffle 1 | cust_id, city | | Co-locate identical pairs |
| 2 | cust_id, city | (none) | Global distinct |
| 3 | cust_id | partial_count(distinct city) | Local count |
| Shuffle 2 | cust_id | | Co-locate each customer |
| 4 | cust_id | count(distinct city) | Final count |

## 11. Pushed filters and why the filter appears twice

Back to the narrow transformation plan:

```
+- *(1) Filter (isnotnull(city) AND (city = boston))
   +- *(1) ColumnarToRow
      +- FileScan parquet [...]
           PushedFilters: [IsNotNull(city), EqualTo(city,boston)]
```

The same condition appears **twice**: once as `PushedFilters` on the FileScan, and once as a separate `Filter` node above it. Spark added the pushed filter by default.

**Question: if Spark already pushes the filter to the source, why filter again?**

- **Where each comes from:**
  - The **pushdown** is decided during logical optimization (Catalyst), alongside projection pushdown.
  - The **Filter node** (with scan, etc.) is laid out when the physical plan is built.
- **Correctness guarantee:** not every filter can be pushed down, and even when a filter is pushed, the data source may apply it only partially. For Parquet, pushdown mainly skips row groups using min/max statistics, so rows that do not match can still come back. Spark keeps its own Filter so the result is always correct.
- **Why it is cheap:** the pushed filter has already cut the data a lot, so the second Filter runs over a much smaller dataset. It is redundant but not very expensive. Think of it as a **fail-safe for correctness and integrity**.

## 12. When predicate pushdown fails

Pushdown depends on what the **data source** supports. Two common cases where it does not happen:

### 12a. Map type columns

```python
# properties: MapType, e.g. {"eye": "brown", "hair": "black"}
df.filter(F.col("properties")["eye"] == "brown").explain(True)
```

The FileScan shows **no `PushedFilters`** for this condition (or an empty list). The source cannot evaluate a filter on a map key, so Spark reads everything and filters afterwards.

### 12b. Unsupported expressions, such as `cast`

`age` is stored as a **string** in Parquet. We cast it to int and filter:

```python
df_customers.filter(F.col("age").cast("int") == 50).explain(True)
```

```
Filter (isnotnull(age) AND (cast(age as int) = 50))
+- ColumnarToRow
   +- FileScan parquet [...]  PushedFilters: [IsNotNull(age)]
```

- Only `IsNotNull(age)` is pushed. The cast comparison is **not**.
- Reason: the Parquet file stores `age` as a string. The filter is on a different, derived data type, which the source cannot evaluate against its stored values.
- This is **source dependent**: some databases (for example, a JDBC source that can run the cast in SQL) can push down such a filter.

Practical tip: store columns with the right type, or filter on the raw column (for example, `F.col("age") == "50"`) when you want the filter pushed down.

## 13. Key takeaways

- **Pipeline:** code → unresolved logical plan → (catalog) analyzed logical plan → (Catalyst) optimized logical plan → several physical plans → (cost model) chosen physical plan → execution.
- **Read physical plans bottom to top.** The FileScan is where the query starts.
- **`Exchange` = shuffle.** Count the Exchange nodes to see how many shuffles the query does.
- **Narrow transformations** (filter, withColumn, select) collapse into Filter and Project nodes with no Exchange.
- **ColumnarToRow** follows a Parquet scan because Spark processes rows downstream.
- **Spark adds `isnotnull` filters** on its own for filter columns and join keys.
- **repartition** always shuffles and shows a partitioning scheme (e.g. `RoundRobinPartitioning(n)`). **coalesce** merges partitions within executors, avoids a shuffle where it can, and shows no partitioning scheme.
- **`AdaptiveSparkPlan isFinalPlan=false`**: AQE may still change the plan at runtime using runtime statistics.
- **Sort merge join:** each side is filtered, shuffled with `hashpartitioning(key, 200)`, sorted, then merged. Disable broadcast with `spark.sql.autoBroadcastJoinThreshold = -1` to see it.
- **Hash partitioning:** `hash(key) mod numPartitions`, so the same key always lands in the same partition across datasets.
- **Aggregations are two phase:** a `partial_*` HashAggregate before the shuffle and a final HashAggregate after it. This greatly reduces shuffled data.
- **HashAggregate with empty functions = DISTINCT.** `countDistinct` becomes 4 HashAggregates and 2 shuffles: local distinct, shuffle on (key, value), global distinct, partial count, shuffle on key, final count.
- **PushedFilters on the scan plus a separate Filter node is normal.** The extra filter guarantees correctness and costs little.
- **Pushdown fails** for map type columns and unsupported expressions such as `cast`. It is always data source dependent, so check `PushedFilters` in the plan.
