# Apache Spark Physical Joins — Detailed Notes

Sep 29, 2026 · @Sunil Patil

## Overview

Joins are among the most used operations in Spark and also where most performance problems live. These notes cover the four core physical join strategies; understanding them builds intuition for the rest.

1. **Sort Merge Join (SMJ)** — the default for large-vs-large equi-joins
2. **Broadcast Hash Join (BHJ)** — one side small enough to ship to every executor
3. **Shuffle Hash Join (SHJ)** — sits between BHJ and SMJ
4. **Broadcast Nested Loop Join (BNLJ)** — non-equi joins and cross joins

Each join is studied in three parts:

- **When Spark chooses it** — the exact conditions that make Spark pick that strategy over the others
- **How it works under the hood** — a step-by-step visual walkthrough
- **Proof in code** — the physical plan (`explain()`) and the DAG in the Spark UI (SQL / DataFrame tab)

**Reading tip:** a physical plan is read **bottom to top**. In plans, `Exchange` means a shuffle and `Project` means selecting output columns.

## Quick comparison

The choice comes down to two questions: is the join condition an equality, and how big is the smaller side relative to `spark.sql.autoBroadcastJoinThreshold` (default 10 MB)?

| Join | Chosen when | Steps | Shuffle? | Plan keywords |
| --- | --- | --- | --- | --- |
| Sort Merge Join | Both sides > broadcast threshold; equi-join on sortable keys | Shuffle → Sort → Merge | Both sides | `Exchange hashpartitioning`, `Sort`, `SortMergeJoin` |
| Broadcast Hash Join | One side < broadcast threshold; equi-join | Broadcast → Build → Probe | None | `BroadcastExchange HashedRelationBroadcastMode`, `BroadcastHashJoin ... BuildRight` |
| Shuffle Hash Join | Small side too big to broadcast, but each post-shuffle partition fits in memory; `preferSortMergeJoin=false` | Shuffle → Build → Probe | Both sides | `Exchange hashpartitioning`, `ShuffledHashJoin ... BuildRight` (no `Sort`) |
| Broadcast Nested Loop Join | Non-equi condition (`>`, `<`, `between`, `!=`…) or cross join; one side small | Broadcast → Nested loop | None | `BroadcastExchange IdentityBroadcastMode`, `BroadcastNestedLoopJoin` |

## 1. Sort Merge Join (SMJ)

SMJ is Spark's fallback for joining two large tables. It is the only join that handles unlimited size on both sides without loading either dataset fully into memory.

### When Spark chooses it

1. **Both DataFrames are large** — both sides exceed `spark.sql.autoBroadcastJoinThreshold` (default 10 MB), so neither can be broadcast.
2. **Equi-join on sortable keys** — the condition must use `=` (e.g. `df1.x == df2.x`). `>`, `>=`, `<`, `<=`, `!=` rule SMJ out. Keys must be sortable because sorting is one of its steps.

### How it works: Shuffle → Sort → Merge

Example: `customers` and `orders` spread across 2 executors, joined on `customer_id`, with shuffle partitions = 2.

**Step 1 — Shuffle.** Every row is sent to a partition using:

```latex
partition = hash(join\_key) \bmod spark.sql.shuffle.partitions
```

- With 2 shuffle partitions the result is always 0 or 1.
- Simplified example (real Spark uses Murmur3 hash): C1 → 1 mod 2 = 1, C3 → 1, C6 → 6 mod 2 = 0, C4 → 0.
- All rows with the same key — from **both** tables — land in the same partition on the same executor.
- *Shuffle partitions* = number of output partitions produced per dataset during the shuffle.

**Step 2 — Sort.** Within each partition, rows are sorted by the join key (e.g. C2, C4, C6 on one side; C1, C3, C5 on the other). Ties keep multiple rows (e.g. orders 1 and 7 for the same customer).

**Step 3 — Merge.** Works like the merge step of merge sort:

- A pointer is placed at the top of each sorted partition.
- Keys equal → emit a joined row and advance.
- Keys differ → advance the pointer on the side with the smaller key.
- Each co-located partition pair produces one joined output partition.

### Lab (Databricks)

Data: `customers.csv` and `orders.csv` in a Unity Catalog volume (`default` catalog → `default` schema → volume `ecom`).

```python
from pyspark.sql import functions as F

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)  # disable broadcast to force SMJ
spark.conf.set("spark.sql.shuffle.partitions", 4)
spark.conf.set("spark.sql.adaptive.enabled", False)        # turn off AQE for a clean plan

df_customers = spark.read.option("header", True).option("inferSchema", True).csv(customers_path)
df_orders    = spark.read.option("header", True).option("inferSchema", True).csv(orders_path)

df_joined = df_orders.join(df_customers, on="customer_id", how="inner")
df_joined.explain()
df_joined.show()
```

- Threshold is set to `-1` because the sample files are small and Spark would otherwise pick BHJ.
- AQE (adaptive query execution) is disabled so the UI doesn't show extras like *AQE shuffle read*.

### Physical plan (read bottom → top)

1. `FileScan csv` — reads customers. `InMemoryFileIndex` is Spark's file-listing component: it lists files at the path and keeps metadata (sizes, partition values) in driver memory.
2. `Filter isnotnull(customer_id)` — added automatically by Spark's optimizer on the join key; we never wrote it.
3. `Exchange hashpartitioning(customer_id, 4)` — the shuffle; `4` = shuffle partitions; logic = hash(key) mod 4.
4. `Sort [customer_id ASC NULLS FIRST]` — the sort step.
5. Same Scan → Filter → Exchange → Sort for orders.
6. `SortMergeJoin [customer_id], Inner` — the merge step.
7. `Project` — selects the output columns.

### Spark UI (SQL / DataFrame tab)

The DAG shows two scans (orders and customers) → `Filter` on each → `Exchange` hash partitioning on each → `Sort` on each → `SortMergeJoin` (key `customer_id`, Inner) → `Project`. Almost every SMJ DAG looks like this.

## 2. Broadcast Hash Join (BHJ)

BHJ avoids shuffling the large table entirely by shipping a full copy of the small table to every executor.

### When Spark chooses it

- One table is **large** and the other is **small enough to fit entirely in each executor's memory**.
- Spark's **cost-based optimizer (CBO)** picks BHJ automatically when the estimated size of one side is below `spark.sql.autoBroadcastJoinThreshold` (default **10 MB**).
- The join must be an equi-join (a hash lookup needs an exact key match).

### How it works: Broadcast → Build → Probe

Example: a 320-byte `customers` table sits on the driver; `orders` has one partition on each of 2 executors.

**Step 1 — Broadcast.** The driver **serializes** the small table (converts the in-memory table into a flat byte sequence that can travel over the network) and pushes a **full copy to every executor**. 2 executors → 2 copies.

**Step 2 — Build.** Each executor iterates over its received copy and builds an in-memory **hash map**: `customer_id → row` (e.g. C1, C2, C3, C4 each mapped to its customer row: name, city).

**Step 3 — Probe.** Each executor scans its partition of the large table row by row and looks up each key in the hash map — an **O(1)** operation.

- Hit → emit a joined row (e.g. C1 + name + city Bengaluru).
- Miss → no output for an inner join.
- Each executor's order partition becomes one joined output partition.

### Lab

Same setup and datasets as SMJ, but the broadcast threshold is **left at its 10 MB default** so Spark picks BHJ on its own. AQE stays off.

```python
spark.conf.get("spark.sql.autoBroadcastJoinThreshold")   # '10485760b' = 10 MB (default)
spark.conf.set("spark.sql.adaptive.enabled", False)

df_joined = df_orders.join(df_customers, on="customer_id", how="inner")
df_joined.explain()
```

### Physical plan (bottom → top)

1. `FileScan csv` (customers) → `Filter isnotnull(customer_id)`.
2. `BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)), false)`
   - **BroadcastExchange**: the driver serializes the small table and sends it over the network to executors.
   - **Broadcast mode** = how the broadcast side is packaged before shipping. Two modes exist: `HashedRelationBroadcastMode` (used by BHJ) and `IdentityBroadcastMode` (used by BNLJ).
   - **HashedRelation**: rows are packaged as a hash map, key = join key, value = row.
   - `input[0, int, false]` = column index 0 (`customer_id`), type int, non-nullable. It is cast to `bigint` (long) for optimization.
   - The key is wrapped in a `List` because a join key can be **composite** (more than one column).
3. `FileScan csv` (orders) → `Filter isnotnull(customer_id)`.
4. `BroadcastHashJoin [customer_id], Inner, BuildRight` — **BuildRight** means the right side (customers) becomes the hash table. The top/first child in the plan is the left side.
5. `Project` — output columns.

### Spark UI

Customers scan → Filter → **BroadcastExchange** (shows *time to read broadcast*) → feeds into **BroadcastHashJoin** alongside orders scan → Filter. The join node shows *Inner, BuildRight*, then `Project`. Note: *rows output = 6* in the example only because `show(5)` limited the read.

## 3. Shuffle Hash Join (SHJ)

SHJ sits between BHJ and SMJ: the situation is too big for a broadcast, but doesn't need the full sort machinery of SMJ.

### When Spark chooses it

- The smaller table is **too large to broadcast** (exceeds the 10 MB threshold).
- But **one partition** of the smaller table, after the shuffle redistributes it, **fits in a single executor's memory** to build a hash table.
- Equi-join condition.

**Preference gate:** by default Spark **prefers SMJ over SHJ even when SHJ is viable**, because SMJ is more memory-stable. SHJ must hold the whole hash map in memory; a **skewed key** can blow up one partition's hash map and cause an **out-of-memory (OOM)** error. To allow SHJ:

```python
spark.conf.set("spark.sql.join.preferSortMergeJoin", False)
```

Then Spark picks SHJ when the smaller side's per-partition data fits in memory.

### How it works: Shuffle → Build → Probe

Example: customers and orders, one partition each on 2 executors, shuffle partitions = 3.

**Step 1 — Shuffle.** Both tables are redistributed with `hash(join_key) mod 3`, so rows with the same key land in the same partition.

- Simplified: C1 → 1 mod 3 = 1, C2 → 2, C3 → 0, etc.
- 3 output partitions per dataset: partitions 0 and 1 land on executor 1, partition 2 on executor 2. The **Spark scheduler** decides this placement.

**Step 2 — Build.** Each executor takes its partition of the **smaller side** and builds an in-memory hash map `join_key → row` (e.g. C3 → row, C6 → row). The larger side's partitions stay as they are.

**Step 3 — Probe.** Each executor streams its partition of the **larger side** row by row and looks up each key in the local hash map. A hit emits a joined row (e.g. C3 → Rohan, Bengaluru). Output: 3 joined partitions (2 on executor 1, 1 on executor 2).

**SHJ vs SMJ:** both shuffle both sides, but SHJ replaces *sort + merge* with *build hash map + probe*.

### Lab

```python
spark.conf.set("spark.sql.adaptive.enabled", False)
spark.conf.set("spark.sql.shuffle.partitions", 4)
spark.conf.set("spark.sql.join.preferSortMergeJoin", False)     # allow SHJ
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 1 * 1024 * 1024)  # lower to 1 MB

df_joined = df_orders.join(df_customers, on="customer_id", how="inner")
df_joined.explain()
```

- The sample data is **between 1 MB and 10 MB**. At the 10 MB default Spark would choose BHJ; lowering to 1 MB makes it "too big to broadcast" while each shuffled partition is still small enough to hash.

### Physical plan (bottom → top)

1. Customers: `FileScan csv` → `Filter isnotnull(customer_id)` → `Exchange hashpartitioning(customer_id, 4)`.
2. Orders: `FileScan csv` → `Filter isnotnull(customer_id)` → `Exchange hashpartitioning(customer_id, 4)`.
3. `ShuffledHashJoin [customer_id], Inner, BuildRight` — hash map built on the right side (customers) **after** the shuffle.
4. `Project` — output columns.

Key difference from SMJ: **no `Sort` nodes**.

### Spark UI

The job has 3 stages. The DAG looks very similar to SMJ: two scans → Filter → Exchange (hash partitioning) on each side → **ShuffledHashJoin** → Project. The join node shows the metric **time to build hash map** (6 ms in the demo), which is the proof that a hash map was built and probed, plus *BuildRight*.

## 4. Broadcast Nested Loop Join (BNLJ)

BNLJ is used when there is no equality to hash or sort on: every row of the large side is compared against every row of the broadcast side.

### When Spark chooses it

1. **Non-equi join condition** — the predicate has no equality clause: `>`, `>=`, `<`, `<=`, `!=`, a range (`between`), or a complex condition.
2. **Cross join** — no join condition at all; every row pairs with every row.
3. Plus: **one side must be small enough to broadcast** (the other can be large), otherwise the broadcast step can't happen.

### How it works: Broadcast → Nested loop

**Step 1 — Broadcast.** Identical to BHJ: the driver serializes the small table and pushes a full copy to every executor. **No shuffle** of the large table.

**Step 2 — Nested loop.** Each executor runs a double loop over its partition of the large table and the full broadcast table:

```python
for l_row in large_partition:
    for b_row in broadcast_table:
        if condition(l_row, b_row):
            emit(joined_row)
```

**Worked example.** Find, for each customer, every order (anyone's) whose amount exceeds that customer's credit limit. Condition: `orders.amount > customers.credit_limit`. The 320-byte customers table is broadcast to both executors.

| Order amount | vs C1 (limit 200) | vs C2 (limit 350) | vs C3 (limit 100) | Rows emitted |
| --- | --- | --- | --- | --- |
| 250 | match | — | match | 2 |
| 80 | — | — | — | 0 |
| 400 | match | match | match | 3 |

Executor 1 emits 5 rows. On executor 2, an order of 150 matches only C3 (1 row); together with its other order it emits 3 rows.

### Lab

A small **tiers** DataFrame is created in the session; each order is classified into a tier by amount range.

| Tier | Order amount range |
| --- | --- |
| Economy | 1 – 50 |
| Standard | 51 – 150 |
| Premium | 151 – 400 |
| Enterprise | above 400 |

```python
spark.conf.set("spark.sql.adaptive.enabled", False)

df_joined = df_orders.join(
    F.broadcast(df_tiers),   # force the broadcast side
    (df_orders.amount >= df_tiers.min_amount) & (df_orders.amount <= df_tiers.max_amount),
    "inner",
)
df_joined.show()      # e.g. amount 160 → Premium (151–400)
df_joined.explain()
```

- `F.broadcast()` forces only the **broadcast** part; the **nested loop** is chosen automatically because the condition contains inequalities.

### Physical plan (bottom → top)

1. `LocalTableScan` — the tiers DataFrame created locally in the session.
2. `Filter isnotnull(min_amount) AND isnotnull(max_amount)` — both columns used in the condition.
3. `BroadcastExchange IdentityBroadcastMode` — **identity** means the transform is an identity function: the data is shipped **as-is as a plain array**. No hashing, no keying (contrast with `HashedRelationBroadcastMode` in BHJ).
4. Orders: `FileScan csv` → `Filter isnotnull(amount)`.
5. `BroadcastNestedLoopJoin BuildRight, Inner, (amount >= min_amount AND amount <= max_amount)` — **BuildRight**: the right side (tiers) is held in memory and broadcast.
6. `Project` — output columns.

### Spark UI

Two jobs (one stage each). DAG: orders `Scan csv` (left) and the local tiers table (right) → `Filter` on each → **BroadcastExchange (IdentityBroadcastMode)** on the tiers side → **BroadcastNestedLoopJoin** with the range condition → `Project` (7 columns).

## Key configs and takeaways

| Config | Default | Effect on join choice |
| --- | --- | --- |
| `spark.sql.autoBroadcastJoinThreshold` | 10 MB | Max size of a side to broadcast; `-1` disables broadcast joins |
| `spark.sql.shuffle.partitions` | 200 | Number of output partitions per dataset in a shuffle (the `N` in `hash(key) mod N`) |
| `spark.sql.join.preferSortMergeJoin` | true | Set `false` to let Spark pick SHJ when viable |
| `spark.sql.adaptive.enabled` | true (Spark 3.2+) | AQE; disabled in the labs only to keep plans and DAGs clean |

- **Equality + both sides big** → SMJ (safe, memory-stable, but pays for shuffle and sort).
- **Equality + one side small** → BHJ (fastest; no shuffle, O(1) lookups).
- **Equality + medium side, SMJ not preferred** → SHJ (shuffle, but no sort); watch for skew-driven OOM.
- **No equality, or cross join** → BNLJ (broadcast + compare every pair; cost grows with rows × rows).
- Spark silently adds `isnotnull` filters on join keys; `Exchange` = shuffle; `BuildRight`/`BuildLeft` = which side becomes the in-memory table.
- Broadcast modes: `HashedRelationBroadcastMode` (hash map, BHJ) vs `IdentityBroadcastMode` (plain array, BNLJ).
