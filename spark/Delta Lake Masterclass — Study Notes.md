# Delta Lake Masterclass — Study Notes

Sep 25, 2026 · @Sunil Patil

## 1. Why Delta Lake exists

Delta Lake adds a **transaction log (`_delta_log`) on top of Parquet files**, which fixes the three big gaps of a plain data lake.

|  | Data lake (S3 / ADLS) | Delta Lake |
| --- | --- | --- |
| Storage | Cheap, scalable, any format (structured, semi-structured, unstructured) | Same, still Parquet underneath |
| ACID transactions | None | Yes, through the transaction log |
| UPDATE / MERGE / DELETE | Hard: files are immutable, so you read → change → rewrite (partial writes can corrupt data) | Native SQL DML |
| Schema | Schema-on-read, so formats drift | Schema-on-write, with enforcement plus evolution |
| Extras | — | Time travel, audit history, unified batch + streaming |

### ACID in one table

| Property | Meaning | Example from the video | How Delta does it |
| --- | --- | --- | --- |
| **Atomicity** | All or nothing | Moving $500 from savings to checking: if the system crashes after the debit, the money vanishes unless the whole transaction rolls back | A commit is one JSON file in `_delta_log`, so it either exists or it doesn't |
| **Consistency** | Each transaction moves the data from one valid state to another | $100 balance with two parallel purchases ($80 and $60): without rules, both succeed and you spend $140 | Constraints + schema enforcement |
| **Isolation** | Transactions don't see each other's uncommitted changes | A reader sees version V1 while a writer is building V2 | Optimistic concurrency control |
| **Durability** | Once committed, it stays committed | A paycheck lost after a crash and restore from an old backup | The commit is persisted to the log in cloud storage |

> Durability pattern: write START → details → COMMIT to the log. On restart, any transaction with no COMMIT entry is rolled back.

## 2. Lab setup on Azure

The lab uses one resource group with three resources. Unity Catalog stores every table in an ADLS container, and reaches it through the access connector.

&#91;embedded content: lab architecture · workspace → Unity Catalog → access connector → ADLS\]

The workspace never gets storage keys. It *assumes the identity* of the access connector, and the connector holds the role on the storage account.

1. Create the **Azure Databricks workspace** (e.g. `dbdeltalab-ws`).
2. Create a **storage account**: ADLS Gen2 (tick *hierarchical namespace*), LRS for the lowest cost.
3. Create a container called `metastore`.
4. Create an **Access Connector for Azure Databricks**, then grant it **Storage Blob Data Contributor** on the storage account.
5. Link them at `accounts.azuredatabricks.net`: Catalog → Create metastore → path `metastore@<account>.dfs.core.windows.net` + the connector's **resource ID** → assign it to the workspace.

**Accessing your own data.** `%fs ls abfss://lab-data@<account>.dfs.core.windows.net/` fails until you create an **external location**: Catalog → External data → path + storage credential. The credential is created automatically with the access connector.

```sql
CREATE CATALOG IF NOT EXISTS delta_catalog;          -- top-level container
CREATE SCHEMA  IF NOT EXISTS delta_catalog.delta_db;  -- schema = database
```

**Cluster tip:** use single node, 14 GB / 4 cores, no Photon, auto-terminate after 10–20 min. Later labs need **DBR 15.4 LTS** (Delta 3.2) for type widening.

## 3. DML and the Delta log

Every write adds **one numbered JSON commit** (`00000.json`, `00001.json`, …) to `_delta_log/` next to the Parquet data files. Each commit is one table **version**.

A commit JSON holds four sections: `commitInfo` (operation, user, timestamp), `metaData` (schema), `protocol`, and the file actions **`add` / `remove`**. Each `add` carries per-column **stats** (min, max, null count), which Spark uses to skip files.

```sql
CREATE OR REPLACE TABLE delta_catalog.delta_db.invoices AS
SELECT * FROM parquet.`abfss://lab-data@<acct>.dfs.core.windows.net/invoices/101_200.parquet`;

INSERT INTO delta_catalog.delta_db.invoices SELECT * FROM parquet.`.../1_100.parquet`;
UPDATE delta_catalog.delta_db.invoices SET quantity = 10 WHERE customer_id = 1;
DELETE FROM delta_catalog.delta_db.invoices WHERE customer_id = 99;
DESCRIBE HISTORY delta_catalog.delta_db.invoices;   -- one row per version
```

### How an UPDATE works with deletion vectors

&#91;embedded content: update with deletion vectors · nothing is rewritten, the log records a new way to read the file\]

`remove` doesn't delete anything physically. It says "stop reading this file the old way". The `add` that follows says how to read it now.

### Computing the latest state = sum the ledger

| Version | Operation | Actions in the JSON | Net effect |
| --- | --- | --- | --- |
| 0 | CTAS | + 1.parquet (customers 101–200) | 1.parquet |
| 1 | INSERT | + 2.parquet (customers 1–100) | + 2.parquet |
| 2 | UPDATE customer 1 | − 2.parquet, + 2.parquet + DV1, + 3.parquet | 2.parquet reads with DV1 |
| 3 | DELETE customer 99 | − 2.parquet + DV1, + 2.parquet + DV2 (skip 1 and 99) | same file, bigger DV |
| 4 | INSERT | + 4.parquet (customers 201…) | + 4.parquet |

**Latest state** = every `add` that has no matching `remove` = 1.parquet + 2.parquet (with DV2) + 3.parquet + 4.parquet. A DELETE only rewrote the DV; no Parquet file changed.

## 4. Scaling the log: compacted JSON and checkpoints

Millions of commits would mean millions of JSON files to open. Delta avoids this in two ways.

&#91;embedded content: checkpoint read path · checkpoint + newer JSONs\]

| Mechanism | What it is | Summed (add/remove cancelled)? |
| --- | --- | --- |
| **Compacted JSON** (`x.y.compacted.json`) | Bundles the actions of a range of commits, e.g. every 6 commits, into one file | No. It just concatenates the actions |
| **Checkpoint** (`N.checkpoint.parquet`) | The full table state up to version N, stored as Parquet | Yes. It is a snapshot of the state |

Checkpoint interval: **every 10 commits in open-source Delta**. The lab saw **36 on Databricks**, which relaxes the interval thanks to its optimizations; the video notes it may vary by cloud.

## 5. Concurrency control (the I in ACID)

Delta uses **optimistic concurrency control (OCC)**. It assumes conflicts are rare, takes no locks, and checks the version at commit time.

&#91;embedded content: optimistic concurrency · two writers, one retry\]

|  | Pessimistic | Optimistic (Delta) |
| --- | --- | --- |
| Assumption | Conflicts will happen | Conflicts are rare |
| Mechanism | Exclusive lock on the row; others wait | Read version + timestamp; at commit, check the version hasn't moved |
| On conflict | Can't happen, because others are blocked | The loser fails, re-reads the latest version, and retries |
| Downside | Waiting halts the system at scale | Retries cost work when writes collide a lot |
| Best for | Write-heavy, high-contention | **Read-heavy** analytics, high concurrency |

A conflict means two transactions change the same data at the same time and leave it invalid, e.g. both see $1,000 and the last writer wins.

## 6. Time travel and versioning

Every commit is a version, so you can **query** any past version or **restore** the table to it. RESTORE is itself logged as a new version.

| Version | Operation (lab: `invoices_ttv`) |
| --- | --- |
| 0 | CTAS from customers 1–100 |
| 1 | DELETE customer 1 |
| 2 | UPDATE customer 5 qty → 25 |
| 3 | INSERT customers 101–200 |
| 4 | RESTORE TO VERSION 0 |

```sql
SELECT * FROM invoices_ttv VERSION AS OF 0   WHERE customer_id = 1;   -- row is back
SELECT * FROM invoices_ttv TIMESTAMP AS OF '2025-05-01 09:35:00';
RESTORE TABLE invoices_ttv TO VERSION AS OF 0;   -- or TO TIMESTAMP AS OF '...'
```

```python
(spark.read.option("versionAsOf", 2).table("delta_catalog.delta_db.invoices_ttv")
      .filter(col("customer_id") == 5).display())      # quantity = 25
# or .option("timestampAsOf", "2025-05-01 09:31:00")
```

**Timestamp between versions?** Delta picks the **latest version committed at or before** that time. With v2 at 9:31 and v3 at 9:57, asking for 9:35 returns v2.

## 7. Schema validation

Delta is **schema-on-write**: data is checked against the table schema *before* it lands. A data lake is schema-on-read: dump anything now, work out the schema later.

The key rule: **INSERT matches columns by position. MERGE matches them by name.**

| Scenario | INSERT | MERGE |
| --- | --- | --- |
| 1. Columns in a different order (both int) | Succeeds **silently with wrong data**: qty lands in `customer_id` | Correct: matched by name |
| 2. Wrong data type | Tries to cast: `'99499'` → int works, `'ABC'` → int fails. A date string casts to DATE | Same casting rules |
| 3. Column names differ (`c_id`, `qty`) | Succeeds, since names are ignored | Fails: *cannot resolve customer\_id* |
| 4. NULL in a `NOT NULL` column | Fails: constraint violated | Fails |
| 5. Extra column in source | Fails: *schema mismatch* | Succeeds: the extra column is ignored |

```sql
CREATE OR REPLACE TABLE delta_catalog.delta_db.invoices_sv (
  customer_id INT NOT NULL,      -- constraints are validated on every write
  invoice_no STRING, quantity INT, price FLOAT, invoice_date DATE);

MERGE INTO invoices_sv AS t USING src AS s ON t.customer_id = s.customer_id
WHEN MATCHED THEN UPDATE SET t.invoice_date = current_date()
WHEN NOT MATCHED THEN INSERT *;
```

> Only use positional INSERT when you're certain of the column order. MERGE (or `INSERT ... BY NAME`) is the safer habit. You can add CHECK constraints such as `price > 0`.

## 8. Schema evolution

Schema evolution lets a table change shape **without rewriting its data files**. Existing rows get NULL for any new column.

| Scenario | Manual (ALTER) | Automatic (`autoMerge = true`) |
| --- | --- | --- |
| 1. Add a column | `ALTER TABLE t ADD COLUMN quantity INT` | INSERT with an extra column → the column is added |
| 2. Type widening (int → bigint, float → double, varchar(n) → bigger) | `ALTER COLUMN customer_id TYPE BIGINT` (needs Delta 3.2 / DBR 15.4 + the table property) | **Does not work**: *fail to assign BIGINT to INT* |
| 3. Nested STRUCT | `ALTER COLUMN purchase_details.mall_pincode TYPE BIGINT`, `ADD COLUMN purchase_details.store_location STRING` | Insert with `named_struct(...)` holding a new key (`staff_id`), which is added inside the struct |
| 4. Column position | `ADD COLUMN age INT AFTER price` (or `FIRST`) | INSERT fails (positional). MERGE succeeds but puts the new column **last** |

```sql
SET spark.databricks.delta.schema.autoMerge.enabled = true;          -- automatic evolution
ALTER TABLE invoices_se SET TBLPROPERTIES ('delta.enableTypeWidening' = 'true');
ALTER TABLE invoices_se ALTER COLUMN customer_id TYPE BIGINT;
```

```python
# DataFrame API: evolve on append
(df.write.mode("append").option("mergeSchema", "true")
   .saveAsTable("delta_catalog.delta_db.invoices_se_spark"))
```

> In production, prefer manual ALTERs so random upstream columns can't silently reshape your tables.

## 9. Parquet → Delta, and managed vs external tables

**Converting** keeps the Parquet files where they are and just creates a `_delta_log` whose first commit `add`s every existing file.

```sql
CONVERT TO DELTA parquet.`abfss://lab-data@<acct>.dfs.core.windows.net/v1`;
```

```python
from delta.tables import DeltaTable
DeltaTable.convertToDelta(spark, "parquet.`abfss://lab-data@<acct>.dfs.core.windows.net/v2`")

# CSV / JSON: read, then write as Delta
spark.read.csv(path).write.mode("overwrite").format("delta").save(out_path)
```

|  | Managed table | External table |
| --- | --- | --- |
| Data lives in | The Unity Catalog metastore container (`.../tables/<uuid>`) | A path you choose (S3, ADLS, GCS) |
| Metadata lives in | Unity Catalog | Unity Catalog |
| `DROP TABLE` | Deletes the data (after a retention period, e.g. up to 30 days) | Removes it from the catalog only; **files stay** |
| Formats | Delta only | Delta, Parquet, CSV, JSON, … |
| Create with | `CREATE TABLE t AS SELECT ...` | `CREATE TABLE t USING DELTA LOCATION 'abfss://...' AS SELECT ...` |

Check a table's storage location with Catalog → table → *Details*, or with `DESCRIBE EXTENDED`.

## 10. Deletion vectors: copy-on-write vs merge-on-read

Parquet files are **immutable**, so deleting 2 rows from a 10M-row file would normally mean rewriting the whole file. **Deletion vectors (DVs)** record the deleted row positions in a small bitmap file instead.

&#91;embedded content: copy-on-write vs merge-on-read · same delete, two strategies\]

|  | Copy-on-write | Merge-on-read |
| --- | --- | --- |
| When | `delta.enableDeletionVectors = false` | `delta.enableDeletionVectors = true` |
| DELETE / UPDATE | Rewrites the file (lab: 1 removed file, 99 copied rows, 1 added file) | Adds a DV (0 removed files, 0 copied rows, 1 DV added) |
| UPDATE | New full file | Updated DV (the old row is soft-deleted) + a small Parquet with the new row |
| Write cost | High | Low |
| Read cost | Low | Slightly higher (check the DV) |
| Best for | **Read-heavy**, few writes | **Frequent updates / deletes** |

```sql
CREATE OR REPLACE TABLE invoices_mor
TBLPROPERTIES ('delta.enableDeletionVectors' = true) AS SELECT ...;

OPTIMIZE invoices_mor;               -- applies DVs, writes one clean file
SET spark.databricks.delta.retentionDurationCheck.enabled = false;
VACUUM invoices_mor RETAIN 0 HOURS;  -- physically removes obsolete files and DVs (lab only)
```

In the Parquet file stats, an UPDATE counts as a DELETE plus an INSERT.

## 11. Cloning: shallow vs deep

A clone snapshots a table at a point in time. The clone **starts its own history at version 0** and can't time-travel into the source's history.

&#91;embedded content: shallow vs deep clone · metadata condensed, data referenced or copied\]

|  | Shallow clone | Deep clone |
| --- | --- | --- |
| Copies metadata | Yes: condensed to `0.checkpoint.parquet` + `0.json` | Yes |
| Copies data files | No, it references the source's files | Yes, 1-to-1 |
| Speed / storage | Fast, minimal | Slower, full storage |
| Changes after cloning | Independent both ways | Independent both ways |
| VACUUM on source | Won't delete files a shallow clone still references | No link, so vacuums are independent |
| Typical use | Test / dev copies, experiments | Backups, **disaster-recovery replicas** (re-running the clone syncs only incremental changes) |

```sql
CREATE OR REPLACE TABLE invoices_c1_100_scl SHALLOW CLONE invoices_c1_100;
CREATE OR REPLACE TABLE invoices_c1_100_v0  SHALLOW CLONE invoices_c1_100 VERSION AS OF 0;
CREATE OR REPLACE TABLE invoices_c1_100_dcl DEEP CLONE    invoices_c1_100;  -- also TIMESTAMP AS OF
```

**Lab proof (shallow + vacuum):** customer 1099 was deleted in the source and vacuumed, yet its file survived, because 3 shallow clones still referenced it. Only after deleting 1099 from every clone did the next VACUUM remove the file.

**Deep clone vs CTAS:** the rows look identical, but CTAS keeps only the SELECT output. A deep clone also carries table properties, partitioning and constraints, and it syncs incrementally.

## 12. The small file problem and OPTIMIZE

Thousands of tiny files mean thousands of *find → open → read → close* operations. That's like reading a 300-page novel saved as 300 one-page PDFs: compute is wasted and I/O is poor.

**OPTIMIZE** compacts small files into \~**1 GB** files with bin packing. The 1 GB default is battle-tested, so leave it unless you have a strong reason (`spark.databricks.delta.optimize.maxFileSize`).

&#91;embedded content: OPTIMIZE bin packing · small files merged into \~1 GB files\]

```sql
OPTIMIZE delta_catalog.delta_db.optimize_example1;
OPTIMIZE delta_catalog.delta_db.optimize_example1 WHERE category = 'fruits';  -- only the new partition
```

```python
DeltaTable.forName(spark, "delta_catalog.delta_db.optimize_example1") \
          .optimize().where("category = 'fruits'").executeCompaction()
```

The old small files are **tombstoned** (a `remove` in the log) but stay on disk until VACUUM. Lab: 5 files per partition → 1 file, and the query dropped from 2.3 s to 1.4 s.

### Root causes

1. **Repartition too high:** 10 GB ÷ `repartition(10000)` gives \~1 MB files.
2. **Partitioning on a high-cardinality column:** thousands of distinct values, so thousands of tiny folders.
3. **Frequent small writes** (e.g. every 5 minutes). This is a business need, not a bug, so schedule OPTIMIZE (e.g. daily).

### Three ways to fight it

| Approach | How | Trade-off |
| --- | --- | --- |
| Manual OPTIMIZE | Run on a schedule, optionally with `WHERE` on a partition | You own the schedule |
| **Optimized writes** | `.option("optimizeWrite", "true")`: shuffles data so each partition is written in one go | Adds a **shuffle**, which raises write latency. Lab: 288 partitions, query 6.97 s → 1.82 s |
| **Auto compaction** | `spark.databricks.delta.autoCompact.enabled = true`: compacts after a write once `autoCompact.minNumFiles` is reached (default 50; lab used 3) | Runs extra work after writes; catches small streaming appends |

## 13. VACUUM

VACUUM **physically deletes tombstoned files** that the current version no longer references and that are older than the retention window. The default retention is **168 hours (7 days)**.

```sql
VACUUM delta_catalog.delta_db.vacuum_example1;                 -- keeps 7 days
SET spark.databricks.delta.retentionDurationCheck.enabled = false;  -- labs only!
VACUUM delta_catalog.delta_db.vacuum_example1 RETAIN 0 HOURS;
```

Two things to remember:

1. **It saves storage cost, not query time.** File skipping already happens in the log, so queries never read tombstoned files anyway.
2. **It limits time travel.** Any version whose files were vacuumed can no longer be read.

| Lab version | Operation | Files it needs | Readable after `VACUUM RETAIN 0`? |
| --- | --- | --- | --- |
| 0 | Write customers 101–150 | File A | Yes |
| 1 | Append 151–200 | File A + File B | **No**: File B is gone |
| 2 | DELETE 151–200 (tombstones B) | File A | Yes |

DESCRIBE HISTORY also logs `VACUUM START` and `VACUUM END` entries.

## 14. Z-ORDER

Z-ORDER **co-locates similar values in the same files** so the min/max stats in the log can skip more files. Fewer files cross the network, so queries get faster.

&#91;embedded content: Z-ORDER data skipping · overlapping min/max ranges vs co-located ranges\]

Z-ORDER is essentially **sort + repartition**. The goal is minimal overlap between file ranges, but files stay sensibly sized, so one value (e.g. age 10) may still span two files.

**Multiple columns:** Z-order is a *space-filling curve*. It maps multi-dimensional points (e.g. `product_id`, `quantity`) to one dimension while **preserving locality**: points close in 2-D stay close in 1-D.

```sql
OPTIMIZE zorder_example1 ZORDER BY (customer_id);          -- always runs with OPTIMIZE
OPTIMIZE zorder_example2 WHERE invoice_date = '2025-05-04'  -- Hive partition + Z-order
  ZORDER BY (customer_id);                                  -- a daily-pipeline pattern
```

```python
DeltaTable.forName(spark, "delta_catalog.delta_db.zorder_example1") \
          .optimize().executeZOrderBy("customer_id")
```

**Lab (20M rows):** 256 files, each spanning customer\_id 201–99457, became 2 files (201–48632 and 48632–99457). A `WHERE customer_id = 201` query went from 785 ms to 144 ms, about 5.5× faster.

## 15. Liquid clustering

Hive partitioning and Z-ORDER make you **fix the layout columns up front**. If queries move from `country` to `category`, the layout stops helping and you have to rewrite the data. Liquid clustering lets you **change clustering columns at any time**, and applies the new layout incrementally.

&#91;embedded content: liquid clustering · merge small, split large, even file sizes\]

It keeps a balanced layout: **uniform file sizes** and **an appropriate number of files**, with similar data co-located. That solves three problems:

1. **Flexibility:** change `CLUSTER BY` columns later without a full rewrite.
2. **No small files:** tiny files are merged (in 2-D, e.g. year × customer, small and medium cells combine into optimal files).
3. **No file-level skew:** oversized chunks are split, so every task gets even work.

```sql
CREATE OR REPLACE TABLE delta_catalog.delta_db.liquid_clustering_example2
CLUSTER BY (invoice_date, customer_id)   -- replaces PARTITION BY invoice_date + ZORDER BY customer_id
AS SELECT customer_id, category, price, quantity, invoice_date FROM ...;

ALTER TABLE liquid_clustering_example2 CLUSTER BY (category);   -- change keys later
OPTIMIZE liquid_clustering_example2;                          -- clusters the data
```

Lab: partition + Z-order took 254 ms, liquid clustering 150 ms, on small data.

**Rules**

- Can't combine liquid clustering with Hive partitioning or Z-ORDER on the same table.
- Choose the columns **most often used in filters**. Of two highly correlated columns (e.g. product category → location), keep only one.
- Doesn't change **shuffle skew**: a hot join key still lands in one partition.

| Migrating from | Use as clustering keys |
| --- | --- |
| Hive partition column | That partition column |
| Z-ORDER column | That Z-order column |
| Partition + Z-ORDER | Both columns |
| Generated column that reduces cardinality (e.g. timestamp → date) | The **original** column (the timestamp); drop the generated one |

## 16. Cheat sheet

| Need | Command / setting | Remember |
| --- | --- | --- |
| See versions | `DESCRIBE HISTORY t` | Includes operationMetrics |
| Table details | `DESCRIBE EXTENDED t` | Location, managed/external, properties |
| Read old data | `VERSION AS OF n` / `TIMESTAMP AS OF 'ts'` | Timestamp resolves to the latest version at or before it |
| Undo | `RESTORE TABLE t TO VERSION AS OF n` | Logged as a new version |
| Upsert safely | `MERGE INTO … WHEN MATCHED / NOT MATCHED` | Matches by name |
| Auto-add columns | `spark.databricks.delta.schema.autoMerge.enabled` / `mergeSchema` | Doesn't widen types |
| Widen a type | `delta.enableTypeWidening` + `ALTER COLUMN … TYPE` | Delta 3.2+ |
| Convert Parquet | `CONVERT TO DELTA parquet.\`path\`\` | Files stay in place |
| Cheap deletes | `delta.enableDeletionVectors = true` | Merge-on-read |
| Copy a table | `SHALLOW CLONE` / `DEEP CLONE` | Both start at v0 |
| Compact files | `OPTIMIZE t [WHERE …]` | Targets \~1 GB |
| Compact on write | `optimizeWrite`, `autoCompact.enabled` (+ `minNumFiles`) | optimizeWrite adds a shuffle |
| Free storage | `VACUUM t [RETAIN n HOURS]` | Default 168 h; breaks time travel to older versions |
| Skip files | `OPTIMIZE t ZORDER BY (col)` | Fixed columns |
| Flexible layout | `CLUSTER BY (cols)` + `OPTIMIZE` | Not with partitioning or Z-order |

Source: [Delta Lake Masterclass | Azure Databricks | PySpark (YouTube)](https://www.youtube.com/watch?v=8IjCyvyAPpM), from the transcript you shared.
