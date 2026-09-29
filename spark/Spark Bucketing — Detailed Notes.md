# Spark Bucketing — Detailed Notes

Sep 29, 2026 · @Sunil Patil

## Overview

Bucketing splits a dataset into a fixed number of files (buckets) based on the hash of a chosen column, so Spark can skip the shuffle and scan less data.

- **What it is:** a storage layout technique that divides data into more manageable chunks.
- **How rows are placed:** `bucket = hash(bucket_column) mod number_of_buckets`.
- **Why it matters:** rows with the same key always land in the same bucket, which speeds up three kinds of queries:
  - **Filter** — only the one matching bucket is scanned.
  - **Join** — the shuffle step is already done at write time.
  - **Aggregation (group by)** — the shuffle step is already done at write time.
- **Best fit:** high-cardinality columns (many unique values), where partitioning would create too many small files.

## Example datasets

The video uses two datasets that share the `product_id` column, which is the filter, join and group-by key throughout.

| Dataset | Columns | What it holds |
| --- | --- | --- |
| orders | order\_id, product\_id, customer\_id, order date, quantity, total\_amount | Which customer ordered which product, when, and for how much |
| products | product\_id, name, category, brand, price | Product catalogue details |

`product_id` is a **high-cardinality** column — it has many unique values.

## Filter operation

For `WHERE product_id = X`, bucketing wins because Spark reads only the one bucket that can contain X.

### Three storage options compared

| Option | What happens | Verdict |
| --- | --- | --- |
| Store raw (one big file) | Full scan of every record to find matching rows | Inefficient |
| Partition by product\_id | High cardinality → many tiny partitions → **small file problem** | Not suitable |
| Bucket by product\_id | Rows grouped into N buckets by hash; only one bucket scanned | Efficient |

### How rows are assigned to buckets

```latex
\text{bucket} = \text{hash}(\text{product\_id}) \bmod N
```

- `hash()` returns a very large integer, so `mod N` squeezes it into the range 0 to N−1.
- N = number of buckets (4 in the example).

### Worked example (N = 4, assuming hash(id) = id for simplicity)

| Bucket | product\_id values | Calculation |
| --- | --- | --- |
| 0 | 24 | 24 mod 4 = 0 |
| 1 | 9 | 9 mod 4 = 1 |
| 2 | 22, 22, 10 | 22 mod 4 = 2, 10 mod 4 = 2 |
| 3 | 19, 11, 87 | 19, 11, 87 mod 4 = 3 |

To find product 22: compute 22 mod 4 = 2 → scan bucket 2 only. Buckets 0, 1 and 3 are skipped, shrinking the search space.

## Join operation

Bucketing both tables on the join key does the shuffle once, at write time, so later joins need only sort and merge.

### Default sort-merge join (no bucketing)

1. **Shuffle** — redistribute data so the same keys sit on the same partition. Most expensive step: reads and moves the whole dataset over the network.
2. **Sort** — sort rows within each partition by the join key.
3. **Merge** — walk both sorted sides and match keys.

### Storage options for orders ⋈ products on product\_id

| Option | Result |
| --- | --- |
| Store raw | Full shuffle + sort + merge on every join |
| Partition by product\_id | High cardinality → small file problem |
| Bucket both tables by product\_id into the same N | Shuffle avoided; only sort + merge |

### Why the shuffle disappears

- Both tables use the same formula, `hash(product_id) mod 4`, so matching keys land in the same bucket number.
- Orders: bucket 2 = {22, 22, 10}, bucket 3 = {19, 11, 87}, bucket 1 = {9}, bucket 0 = {24}.
- Products: bucket 3 = {19, 11, 87}, bucket 1 = {9}, bucket 0 = {24}, bucket 2 = {22, 10}.
- Bucket *k* of orders and bucket *k* of products are processed on the same executor, so they join directly: 0↔0, 1↔1, 2↔2, 3↔3.
- The data is effectively **pre-shuffled** — only sort and merge remain.

> Payoff grows with reuse. Bucketing for a single join may not help much; for joins repeated many times, every later join skips the shuffle.

## Group by / aggregation

A group by on the bucket column skips the shuffle for the same reason a join does: all rows for a key already sit in one bucket.

- Normal group by: **shuffle** (bring same keys together) → **aggregate**.
- With bucketing, storage is identical to the join case, so the shuffle is already done.
- Example: total sales per product. All rows with product\_id 22 are in bucket 2 on one executor, so Spark just sums them (22 → sum of its two orders; 10 → its one order).
- Only the aggregation step remains.

## Join scenarios on bucketed datasets

The shuffle is avoided only when both sides are bucketed on the join column into the same number of buckets.

| # | Dataset 1 | Dataset 2 | Join key | Shuffle? | Verdict |
| --- | --- | --- | --- | --- | --- |
| 1 | X buckets on product\_id | X buckets on product\_id | product\_id | None | Best case |
| 2 | X buckets | Y buckets | product\_id | One side (e.g. Y reshuffled into X) | Acceptable — one side still benefits |
| 3 | X buckets on product\_id | X buckets on product\_id | A column other than product\_id | Full shuffle on both | Bad — bucketing layout is useless |

Rule: the bucket column must match the join column, and bucket counts should match.

## Choosing the number of buckets

Aim for each bucket to hold roughly 128–200 MB; the 4 buckets in the examples were arbitrary.

```latex
\text{number of buckets} = \frac{\text{dataset size}}{\text{optimal bucket size (128–200 MB)}}
```

Example: 1 GB (≈ 1000 MB) ÷ 200 MB = **5 buckets**.

### Estimating dataset size

```latex
\text{size (MB)} = \frac{N \times V \times W}{1024^2}
```

- **N** = number of records (rows)
- **V** = number of variables (columns)
- **W** = average width in bytes of a column
  - small integer ≈ 1 byte
  - medium to large integer ≈ 2–4 bytes
  - floats and strings: estimate from their typical length
- Dividing by 1024² converts bytes to MB.
- Getting N needs one scan of the data, but that is a one-time cost; the bucketed table is then reused many times without shuffles.

## PySpark demo and physical plans

The demo proves the point in the physical plan: the `Exchange` (shuffle) node disappears once the tables are bucketed.

### 1. Join without bucketing

```python
df_joined = df_orders.join(df_products, on="product_id", how="inner")
df_joined.explain()
```

Plan per side: `FileScan` → `Filter (isnotnull product_id)` → **`Exchange hashpartitioning`** → `Sort` → then `SortMergeJoin`. Two expensive shuffles, one per table.

### 2. Write bucketed tables

```python
df_products.write.bucketBy(4, "product_id").saveAsTable("products_bucketed")
df_orders.write.bucketBy(4, "product_id").saveAsTable("orders_bucketed")
```

- `bucketBy(num_buckets, column)` — bucketing only works with `saveAsTable`, not a plain `save`.
- Tables are written under a `spark-warehouse/` folder (e.g. `spark-warehouse/products_bucketed`).

### 3. Join bucketed tables

```python
df_orders_bucketed = spark.table("orders_bucketed")
df_products_bucketed = spark.table("products_bucketed")

df_joined_bucketed = df_orders_bucketed.join(df_products_bucketed, on="product_id", how="inner")
df_joined_bucketed.explain()
```

Plan: `FileScan` → `Filter` → `Sort` → `SortMergeJoin`. **No `Exchange`** — the shuffle is gone.

### 4. Aggregation without vs with bucketing

```python
from pyspark.sql import functions as F

# not bucketed
df_orders.groupBy("product_id").agg(F.sum("total_amount")).explain()

# bucketed
df_orders_bucketed.groupBy("product_id").agg(F.sum("total_amount")).explain()
```

|  | Plan steps |
| --- | --- |
| Not bucketed | FileScan → partial (local) sum → **Exchange hashpartitioning** → global (final) sum |
| Bucketed | FileScan → partial sum → global sum (no Exchange) |

The partial + final sum still appear because one executor can hold several buckets: each bucket is summed, then the results are combined.

## Bucket pruning

Bucket pruning is the filter benefit in practice: Spark reads only the bucket that can hold the filtered key and skips the rest.

```python
(df_orders_bucketed
    .filter(F.col("product_id") == 1)
    .groupBy("product_id")
    .agg(F.sum("total_amount"))
    .explain())
```

- Every row with product\_id = 1 lands in the same bucket, since `hash(1) mod 4` is always the same.
- The plan's FileScan shows **selected buckets: 1 out of 4** — the other 3 buckets are never scanned.

## Revision cheat sheet

| Question | Answer |
| --- | --- |
| What decides a row's bucket? | hash(column) mod number\_of\_buckets |
| When to bucket instead of partition? | High-cardinality columns (partitioning → small file problem) |
| Which operations benefit? | Filter (bucket pruning), join (no shuffle), group by (no shuffle) |
| What remains in a bucketed join? | Sort + merge only |
| When is the shuffle still needed? | Different bucket counts (one side) or joining on a non-bucket column (both sides) |
| How many buckets? | Dataset size ÷ 128–200 MB |
| Dataset size estimate? | N × V × W ÷ 1024² MB |
| API | `df.write.bucketBy(n, "col").saveAsTable("name")`, read back with `spark.table("name")` |
| How to confirm it worked? | `.explain()` — no `Exchange` node; FileScan shows selected buckets |
| When is it worth it? | Tables joined or aggregated on the same key repeatedly |
