# PySpark Interview Questions: Detailed Study Guide

Sep 30, 2026 · @Sunil Patil

## How to use this guide

This guide turns the \~3h45m "PySpark interview questions 2025" video into 52 questions you can revise in about an hour: each has the scenario, the approach to say out loud, and working PySpark code. Where the video's answer is incomplete or wrong, the code here is already fixed and the reason is listed under **Corrections to the video**.

**Organization.** Coding questions come first (Parts 1–4), grouped by the skill they test rather than the video's order. Conceptual questions follow (Parts 5–6). The cheat-sheet at the end maps each scenario to the function you should reach for.

**Where to practice.** Any PySpark environment works: a Databricks workspace, or `pip install pyspark` locally with Java 17. Every snippet assumes this header:

```python
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
```

**How to approach any coding question in the interview** (the video's most useful advice):

1. Read the whole scenario, then write 2–3 bullet points in the notebook: the key column, the deciding column, the expected output. Interviewers are hiring a problem solver, not a typist.
2. Check the schema before coding (`df.printSchema()`). Dates stored as strings are the most common hidden trap.
3. Build the answer in steps and display after each one, so you can explain what every transformation does.
4. Always give aggregated columns an alias; it signals production habits.

## Part 1 – Deduplication, nulls and data cleaning

Removing duplicates is the task you will do most often as a data engineer, because duplicates break a warehouse's totals. Learn one pattern well: **`row_number()` over a window, then keep `rn = 1`**. It works for "latest", "first", "third" and ties, and its result is deterministic.

### Q1. Remove duplicates, keep the latest record per key

**Scenario.** Ingested data has duplicate `product_id` rows. Keep only the row with the latest `date`.

**Approach.** Bullets to write first: key = `product_id`, deciding column = latest `date`. Then check the schema: `date` is a string, so cast it before sorting (string sort breaks on formats like `1/12/2024`).

```python
df = df.withColumn("date", F.to_date("date"))          # or F.col("date").cast("date")

w = Window.partitionBy("product_id").orderBy(F.col("date").desc())

latest = (df.withColumn("rn", F.row_number().over(w))
            .filter(F.col("rn") == 1)
            .drop("rn"))
latest.display()
```

**Say this in the interview:** "`orderBy` followed by `dropDuplicates` is not guaranteed to keep the latest row in Spark, because the shuffle does not preserve sort order. A window with `row_number` is deterministic." (The video used the sort-then-drop approach; see Corrections.)

### Q2. Fill missing values in a streaming column

**Scenario.** A real-time pipeline has nulls in `category`. Replace them.

**Approach.** `fillna` is a stateless, row-level operation, so it works the same on a streaming DataFrame. Pass a dictionary so each column gets its own default and other columns are untouched.

```python
df = df.fillna({"category": "Unknown"})
# several columns at once
df = df.fillna({"category": "Unknown", "quantity": 0})
```

**Follow-up to expect:** `fillna("x")` without a dict only fills columns whose type matches the value (a string fills string columns only).

### Q3. Remove duplicates but keep the first row as it arrived

**Scenario.** The stakeholder says: do not pick by date or any column; keep the first record for each `name` in the order the data came in.

**Approach.** A DataFrame has no built-in row order, so capture the arrival order first with `monotonically_increasing_id()`, then use `row_number` over it.

```python
df = df.withColumn("_arrival", F.monotonically_increasing_id())

w = Window.partitionBy("name").orderBy("_arrival")

first_seen = (df.withColumn("rn", F.row_number().over(w))
                .filter("rn = 1")
                .drop("rn", "_arrival"))
```

**Know the difference between the ranking functions** (value 25 appears twice):

| Function | Output for 20, 25, 25, 30 | Use when |
| --- | --- | --- |
| `row_number()` | 1, 2, 3, 4 | You need exactly one row per group, or a surrogate key |
| `rank()` | 1, 2, 2, 4 | Ties share a rank and the next rank is skipped |
| `dense_rank()` | 1, 2, 2, 3 | Ties share a rank with no gaps ("2nd highest salary") |

**Bonus the video added:** `row_number` is also how you generate a **surrogate key**, a simple 1, 2, 3… integer key used in dimension tables instead of long business keys, which makes joins cheaper.

### Q4. Use the primary contact, else the secondary

**Scenario.** Rows have `primary_contact` and `secondary_contact`; either can be null. Produce one `contact` column.

**Approach.** This is SQL's `COALESCE`: return the first non-null value.

```python
df = df.withColumn("contact", F.coalesce(F.col("primary_contact"), F.col("secondary_contact")))
```

Do not confuse `F.coalesce()` (columns, first non-null) with `df.coalesce(n)` (reduces partitions).

### Q5. Add a "processed at" audit column

**Scenario.** Record when each row was processed by your pipeline (not when it was created at the source).

```python
df = df.withColumn("processed_time", F.current_timestamp())
```

**The important part is the use case.** Audit columns like `created_at` / `modified_at` are essential for Slowly Changing Dimensions (SCD Type 2), incremental loads and debugging which run wrote a row. Say where you used it.

### Q6. Classify transactions as High or Low

**Scenario.** Label each sale `High` if the amount is above 50, else `Low`.

**Approach.** PySpark's version of SQL `CASE WHEN` is `when(...).otherwise(...)`. Chain `when` calls for more than two buckets.

```python
df = df.withColumn(
    "price_category",
    F.when(F.col("sales") > 50, "High").otherwise("Low")
)
# three buckets
df = df.withColumn("tier",
    F.when(F.col("sales") > 200, "High")
     .when(F.col("sales") > 50, "Medium")
     .otherwise("Low"))
```

## Part 2 – Aggregations and window functions

Two tools cover almost every question here. `groupBy().agg()` collapses rows into one row per group. A **window function** (`... .over(Window...)`) keeps every row and adds a value computed across related rows, which is what you need for "latest per customer", "top per month" and running totals.

### Q7. Top 5 most active users

**Scenario.** Each row is a user's action count. Find total actions per user, then the top five.

```python
top5 = (df.groupBy("user_id")
          .agg(F.sum("actions").alias("total_actions"))
          .orderBy(F.col("total_actions").desc())
          .limit(5))
top5.display()
```

If the interviewer cares about ties at 5th place, use `dense_rank` and filter `<= 5` instead of `limit(5)`.

### Q8. Most recent transaction per customer (without `dropDuplicates`)

**Scenario.** Same as Q1, but the interviewer asks you to solve it with window functions.

**Approach.** Cast `transaction_date`, rank rows within each `customer_id` newest first, keep rank 1.

```python
df = df.withColumn("transaction_date", F.col("transaction_date").cast(DateType()))

w = Window.partitionBy("customer_id").orderBy(F.col("transaction_date").desc())

latest = (df.withColumn("flag", F.row_number().over(w))
            .filter(F.col("flag") == 1))
```

**Why windows are preferred:** they give you any position, not only first or last. "Each customer's 3rd purchase" is the same code with `flag == 3`. Use `dense_rank` instead of `row_number` only if two transactions on the same date should both be returned. Import path to know: `from pyspark.sql.window import Window`.

### Q9. Cumulative (running) sum of sales per product

**Scenario.** For each product, show sales accumulated over time: 100, then 100 + 150 = 250, then 450, and so on. Common in finance interviews.

**Approach.** An aggregate function (`sum`) used over a window becomes a running total. Partition by product, order by date, and set the frame from the first row to the current row.

```python
df = df.withColumn("date", F.to_date("date"))

w = (Window.partitionBy("product_id")
           .orderBy("date")
           .rowsBetween(Window.unboundedPreceding, Window.currentRow))

df = df.withColumn("cum_sum", F.sum("sales").over(w))
```

Without `rowsBetween`, Spark uses a RANGE frame, so two rows with the same date get the same (combined) running total. Stating this frame explicitly shows real understanding.

### Q10. Average session duration per user

```python
df.groupBy("user_id").agg(F.avg("duration").alias("avg_duration")).display()
```

Simple, but interviews mix easy and hard questions; always alias the result.

### Q11. Product with the highest sales in each month

**Scenario.** Rows are `product_id`, `date`, `sales`. For every month, return the top-selling product.

**Approach, in three steps:**

1. Derive the month. Keep the year too, otherwise December 2023 and December 2024 merge.
2. Aggregate: total sales per (month, product).
3. Rank products within each month by total sales and keep rank 1. The key insight: the window partitions by **month**, not by product.

```python
monthly = (df.withColumn("date", F.to_date("date"))
             .withColumn("month", F.date_format("date", "yyyy-MM"))
             .groupBy("month", "product_id")
             .agg(F.sum("sales").alias("total_sales")))

w = Window.partitionBy("month").orderBy(F.col("total_sales").desc())

top_per_month = (monthly.withColumn("rnk", F.dense_rank().over(w))
                        .filter(F.col("rnk") == 1))
```

`dense_rank` returns both products if two tie for first. The same pattern answers "best product per week / quarter / region".

### Q12. Department with the most employees

```python
counts = (df.groupBy("department")
            .agg(F.count("employee_name").alias("total_employees"))
            .orderBy(F.col("total_employees").desc()))

# only the top department(s), ties included
w = Window.orderBy(F.col("total_employees").desc())
counts.withColumn("r", F.dense_rank().over(w)).filter("r = 1").display()
```

In the video's data HR and Finance tie, which is exactly why `limit(1)` is a risky answer.

### Q13. List all products in each category (`collect_list`)

**Scenario.** Aggregate, but instead of counting, return the product names themselves.

```python
df.groupBy("category").agg(F.collect_list("product").alias("products")).display()
```

This is the PySpark equivalent of SQL `GROUP_CONCAT` / `STRING_AGG` (wrap in `F.concat_ws(", ", ...)` for a single string).

### Q14. Unique products bought by each customer (`collect_set`)

The keyword is **unique**: `collect_set` drops duplicates, `collect_list` keeps them.

```python
df.groupBy("customer_id").agg(F.collect_set("product_id").alias("unique_products")).display()
```

### Q15. Average number of courses per student

**Scenario.** Each row has a `student_id` and an array column `courses`.

**Approach.** Two steps: count the array's elements with `size`, then average that count.

```python
df = df.withColumn("course_count", F.size("courses"))
df.agg(F.avg("course_count").alias("avg_courses")).display()
```

If a student can appear on several rows, first `groupBy("student_id")` and sum `course_count`, then average.

## Part 3 – Dates, strings, arrays and nested data

These questions test whether you know the right built-in function. There is little logic; recall is everything, so the cheat-sheet at the end repeats them.

### Q16. Customers inactive for more than 30 days

**Bullets first:** compare `last_purchase_date` to today; keep gaps over 30 days.

```python
df = (df.withColumn("last_purchase_date", F.to_date("last_purchase_date"))
        .withColumn("gap_days", F.datediff(F.current_date(), F.col("last_purchase_date")))
        .filter(F.col("gap_days") > 30))
```

`datediff(end, start)` puts the later date first. A future date gives a negative gap and is excluded automatically.

### Q17. Most frequently used words in customer reviews

**Approach, step by step (display after each step):**

1. Lowercase the text so "Great" and "great" count as one word.
2. `split` the sentence into an array of words.
3. `explode` the array into one row per word.
4. `groupBy` the word, `count`, sort descending.

```python
words = (df.withColumn("word",
              F.explode(F.split(F.lower(F.regexp_replace("feedback", r"[^a-zA-Z\s]", "")), r"\s+")))
           .filter(F.col("word") != "")
           .groupBy("word")
           .agg(F.count("*").alias("word_count"))
           .orderBy(F.col("word_count").desc()))
```

The `regexp_replace` strips punctuation so "great!" and "great" match. Lowercasing must happen **before** the grouping, not after.

### Q18. Combine first and last name only when an email exists

```python
df = df.withColumn(
    "full_name",
    F.when(F.col("email").isNotNull(),
           F.concat_ws(" ", F.col("first_name"), F.col("last_name")))
     .otherwise(None)
)
```

**`concat` vs `concat_ws`:** `concat_ws` takes the separator once and skips null inputs; `concat` needs the separator between every column and returns null if any input is null.

### Q19. Number of products in each customer's list

```python
df = df.withColumn("num_products", F.size("product_ids"))
```

`size` counts array elements. If the question says "per customer" across many rows, `groupBy` first and sum the sizes.

### Q20. Pad employee IDs to six characters with leading zeros

```python
df = df.withColumn("employee_id", F.lpad(F.col("employee_id").cast("string"), 6, "0"))
# "7" -> "000007", "123" -> "000123"; use rpad to pad on the right
```

Careful: `lpad` truncates values longer than the target length.

### Q21. Validate phone numbers that start with 91

```python
valid = df.filter(F.col("phone_number").startswith("91"))
# equivalent: F.substring("phone_number", 1, 2) == "91"   (substring is 1-based)
# as a flag instead of a filter:
df = df.withColumn("is_india", F.col("phone_number").startswith("91"))
```

### Q22. Label product codes by length

**Scenario.** Codes of length 5 are `Standard`, anything else is `Custom`. This is `when` plus `length`.

```python
df = df.withColumn("code_flag",
    F.when(F.length("product_code") == 5, "Standard").otherwise("Custom"))
```

### Q23. Flatten a nested struct so analysts can query it with SQL

**Scenario.** `product_info` is a struct with `price` and `quantity`. Analysts should not have to deal with nesting.

```python
flat = df.select(
    "product_id",
    F.col("product_info.price").alias("price"),
    F.col("product_info.quantity").alias("quantity"),
)
# or every field at once: df.select("product_id", "product_info.*")

flat.createOrReplaceTempView("flat_view")
spark.sql("SELECT * FROM flat_view").display()
```

For arrays of structs, `explode` first, then select the fields. Nested JSON from APIs is common, so expect at least one nested column in real work.

## Part 4 – Reading, writing, SQL views and Delta Lake

These are "do you know the option?" questions, and Delta Lake questions are where many candidates are weakest. Delta is the default table format on Databricks, so expect at least two of these.

### Q24. Files with inconsistent schemas → one DataFrame

**Scenario.** Source A sends 3 columns, B sends 4, C sends 2. Load them all into one DataFrame.

```python
df = (spark.read.format("parquet")
           .option("mergeSchema", "true")
           .load("/mnt/raw/data_files/"))
```

`mergeSchema` unions the columns of every file; a column missing from a file is null for its rows. For DataFrames you already have, use `df1.unionByName(df2, allowMissingColumns=True)`. For Delta writes, the equivalent is `.option("mergeSchema", "true")` on the write (schema evolution).

### Q25. Make sure a large dataset gets the right schema

**Answer in two parts:**

- **CSV / JSON:** `.option("inferSchema", "true")` lets Spark sample the data and pick types. It costs an extra pass over the data.
- **Parquet:** the schema is stored in the file itself, so inference is not needed. "Right schema" here means *enforcing* one: pass an explicit schema and validate.

```python
# CSV: infer
df = spark.read.option("header", True).option("inferSchema", True).csv(path)

# Production best practice: declare the schema, fastest and safest
schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("amount", DoubleType(), True),
    StructField("order_date", DateType(), True),
])
df = spark.read.schema(schema).parquet(path)
df.printSchema()
```

### Q26. Skip corrupt records while reading a CSV

**Scenario.** Some rows from an API export are malformed. The stakeholder says drop them (the raw copy stays in staging).

```python
df = (spark.read.format("csv")
           .option("header", True)
           .option("mode", "DROPMALFORMED")
           .schema(schema)
           .load("/mnt/staging/orders/"))
```

The three read modes:

| Mode | What happens to a bad row | Use when |
| --- | --- | --- |
| `PERMISSIVE` (default) | Fields become null; raw text goes to `_corrupt_record` if you add that column | You want to inspect or quarantine bad rows |
| `DROPMALFORMED` | Row is dropped | Stakeholder accepts losing bad rows |
| `FAILFAST` | Read fails on the first bad row | Bad data must stop the pipeline |

On Databricks, `.option("badRecordsPath", "/mnt/bad/")` also saves the rejected rows for later review.

### Q27. Query a DataFrame with SQL (temporary view)

```python
df.createOrReplaceTempView("temp_sql_df")

spark.sql("SELECT * FROM temp_sql_df WHERE product_id = 'P1'").display()
```

In a Databricks notebook you can also use a `%sql` cell and write plain SQL against `temp_sql_df`. A temp view is only visible in the current Spark session (your notebook).

### Q28. Share the view across notebooks (global temp view)

```python
df.createOrReplaceGlobalTempView("global_view")

spark.sql("SELECT * FROM global_temp.global_view").display()   # note the global_temp prefix
```

A global temp view lives in the `global_temp` database and is visible to every notebook on the same cluster until the cluster (Spark application) stops. Forgetting the `global_temp.` prefix gives "table not found", a classic trick question. For anything durable, save a real table with `df.write.saveAsTable(...)`.

### Q29. Write data partitioned by a column

```python
(df.write.format("parquet")
   .mode("append")
   .partitionBy("category")
   .save("/mnt/curated/sales/"))
# creates folders: category=Food/, category=Clothes/, category=Electronics/
```

Queries filtering on `category` read only the matching folders (**partition pruning**). Pick a low-cardinality column (date, country, category). Partitioning by a high-cardinality column like `user_id` creates millions of tiny files and hurts performance.

### Q30. Write Parquet with compression

```python
(df.write.format("parquet")
   .option("compression", "snappy")
   .mode("overwrite")
   .save(path))
```

Snappy is Spark's default Parquet codec: fast to compress and decompress, with moderate size reduction. Mention the trade-off: `gzip` or `zstd` compress smaller but cost more CPU. Smaller files mean lower storage cost and less I/O.

### Q31. Concurrent updates to a Delta table: ACID and avoiding corruption

**Scenario.** A large, partitioned Delta table is updated by several users and pipelines. Concurrent writes cause inconsistent reads and duplicate IDs.

**Part 1, ACID.** Delta Lake keeps a transaction log (`_delta_log`). Every write is an atomic commit to that log, readers always see the last committed snapshot, and concurrent writers are handled with **optimistic concurrency control**: if two commits conflict, one fails with a concurrency exception and can be retried, instead of corrupting the table.

**Part 2, avoid duplicates: upsert with MERGE** (update when the key exists, insert when it does not).

```python
from delta.tables import DeltaTable

new_df = spark.read.format("parquet").load("/mnt/raw/customers/")      # source

target = DeltaTable.forPath(spark, "/mnt/delta/customers")              # target

(target.alias("tgt")
       .merge(new_df.alias("src"), "tgt.id = src.id")
       .whenMatchedUpdateAll()
       .whenNotMatchedInsertAll()
       .execute())
```

Two senior-level additions: deduplicate the source first (MERGE fails if two source rows match one target row), and include the partition column in the merge condition so concurrent jobs touching different partitions do not conflict. MERGE is also the basis for **SCD Type 1 and 2**.

### Q32. The Delta pipeline is getting slow as data grows

**Answer:** compact the files and co-locate related data.

```sql
OPTIMIZE sales_delta ZORDER BY (order_date);
```

**What each part does:**

- **`OPTIMIZE`** rewrites many small files into fewer large files (bin-packing, around 1 GB each). Reading a few big files is much faster than thousands of small ones.
- **`ZORDER BY`** sorts and clusters rows so similar values of the chosen column(s) sit in the same files.
- **Data skipping** is the payoff. Delta stores min/max statistics per file for the first 32 columns. For `WHERE order_date <= '2024-01-31'`, Spark reads those stats and skips every file whose minimum is later. That is why the Z-order column should be among the first 32 columns and be one you often filter on.

**Worth mentioning:** on newer Databricks runtimes, **liquid clustering** (`CLUSTER BY` on the table) is the recommended replacement for partitioning plus Z-ordering on new tables, because it clusters incrementally and lets you change the keys later.

### Q33. Someone deleted production data. Get it back (time travel)

```sql
-- 1. See every version and the operation that created it
DESCRIBE HISTORY sales_delta;

-- 2. Look at the data before the delete
SELECT * FROM sales_delta VERSION AS OF 2;
SELECT * FROM sales_delta TIMESTAMP AS OF '2025-01-10 09:00:00';

-- 3. Roll the table back
RESTORE TABLE sales_delta TO VERSION AS OF 2;
```

Python read of an old version: `spark.read.format("delta").option("versionAsOf", 2).load(path)`. The limit to mention: `VACUUM` deletes old data files (default retention 7 days), after which those versions can no longer be restored.

## Part 5 – Core concepts

The video's advice for theory questions: understand the mechanism instead of memorizing definitions. Under interview pressure a memorized definition disappears; a mechanism you can picture does not, and it answers every rephrasing of the question.

### Q34. Why Spark over Hadoop MapReduce?

This comes in many forms ("why did we stop using MapReduce?", "why is Spark faster?"). One answer covers them all:

1. **In-memory processing.** MapReduce writes intermediate results to disk after every map step, and the reduce step reads them back. Spark keeps intermediate data in memory across the whole chain of transformations, avoiding repeated disk I/O.
2. **Optimization.** Spark builds a plan of the whole job (DAG) and optimizes it with the Catalyst optimizer before running. MapReduce runs each step as written.
3. **Batch and streaming.** MapReduce is batch-only. Spark handles batch, streaming (Structured Streaming), SQL and ML in one engine.
4. **Developer experience.** DataFrame and SQL APIs in Python, Scala, SQL and R, versus verbose Java map/reduce code.

**Follow-up trap:** "But Spark also writes to disk." Yes, during shuffles (shuffle files), when data does not fit in memory (spill), or when you persist to disk. The difference is that disk is the exception, not the rule after every step, and even then the optimized plan keeps Spark faster.

### Q35. What is SparkContext?

SparkContext is the original entry point to Spark: the driver's connection to the cluster manager, through which Spark requests executors (CPU and memory) and runs jobs. It is the low-level RDD API's entry point.

### Q36. Explain the Spark architecture

&#91;embedded content: Spark architecture · driver, cluster manager, executors\]

The cluster manager only hands out resources; after step 2 the driver and executors talk directly.

Walk through it in this order:

1. You submit an application. The **cluster manager** (YARN, Kubernetes, Spark Standalone, or Databricks' own) receives it.
2. A **driver** process starts. It runs your `main` code, creates the SparkSession/SparkContext, builds the plan (DAG), splits it into stages and tasks, and schedules them. The driver is the brain.
3. Through SparkContext, the driver asks the cluster manager for resources; the cluster manager launches **executors** on **worker nodes**.
4. From then on the driver talks to the executors directly: it sends tasks, executors run them on their partitions of data (one task per partition per core), cache data, and report results back.

**Deploy modes follow-up:** in **cluster mode** the driver runs on a node inside the cluster (production jobs). In **client mode** the driver runs where you submitted from, such as your laptop or an edge node (interactive work, debugging).

### Q37. What is SparkSession, and how is it different from SparkContext?

Both are entry points; the difference is age and scope. Before Spark 2.0 you juggled three: `SparkContext` (RDDs), `SQLContext` (DataFrames/SQL) and `HiveContext` (Hive tables). Spark 2.0 introduced **SparkSession**, one unified entry point that wraps all three. It creates and manages the SparkContext internally (`spark.sparkContext`). Use SparkSession in all new code; SparkContext remains for low-level RDD work and legacy apps.

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("interview").getOrCreate()
sc = spark.sparkContext
```

### Q38. RDD vs DataFrame vs Dataset

|  | RDD | DataFrame | Dataset |
| --- | --- | --- | --- |
| What it is | Low-level distributed collection of objects | Distributed table with named columns and a schema | Typed DataFrame (DataFrame = `Dataset[Row]`) |
| Schema | None | Yes | Yes, with compile-time types |
| Optimization | None: runs exactly as written | Catalyst optimizer + Tungsten | Catalyst + Tungsten |
| Type safety | Compile-time in Scala, not optimized | Errors appear at runtime | Errors caught at compile time |
| Languages | Scala, Java, Python | Scala, Java, Python, R, SQL | Scala and Java only |
| Use when | Fine-grained control, unstructured data | Almost always, especially in PySpark | Scala teams wanting type safety |

**The one-liner:** the Dataset API does not exist in Python, because Python is dynamically typed, so PySpark developers use DataFrames. Every DataFrame still executes as RDDs underneath.

### Q39. How does Spark optimize a query? (Catalyst)

This explains what happens between the code you write and tasks on executors:

1. **Unresolved logical plan:** your DataFrame/SQL code parsed into a tree of operations.
2. **Analysis:** column and table names are resolved against the catalog. A misspelled column fails here.
3. **Optimized logical plan:** the Catalyst optimizer applies rules, such as **predicate pushdown** (filter as early as possible, even inside the Parquet reader), **column pruning** (read only needed columns), constant folding, and reordering. This is the video's example: select the two needed columns before a join instead of after.
4. **Physical plans:** Spark generates candidate physical plans (for example broadcast hash join vs sort-merge join) and picks one using a cost model.
5. **Code generation and execution:** whole-stage code generation (Tungsten) compiles the plan to efficient bytecode, which runs as RDD tasks across the executors.

See it yourself with `df.explain(True)`. Quick answers: the logical plan comes first; the physical plan comes from the optimized logical plan; RDDs/tasks come last.

### Q40. What is lazy evaluation?

Transformations (`filter`, `select`, `join`, `groupBy`, `withColumn`...) do not run when you call them. Spark only records them in the plan. Nothing executes until an **action** asks for a result: `show`, `display`, `collect`, `count`, `take`, `first`, or a `write`.

**Why it matters:** because Spark sees the whole chain before running it, Catalyst can reorder, combine and skip work. For example, a filter written after a join can be pushed below the join. A cell that "finishes instantly" has done nothing yet.

### Q41. Narrow vs wide transformations

|  | Narrow | Wide |
| --- | --- | --- |
| Data movement | Each output partition depends on one input partition; no shuffle | Output needs rows from many partitions; data is **shuffled** across the network |
| Examples | `filter`, `select`, `withColumn`, `map`, `union` | `groupBy`, `join` (non-broadcast), `distinct`, `orderBy`, `repartition` |
| Cost | Cheap, pipelined within one stage | Expensive; each shuffle creates a new **stage** boundary |

**Example to give:** IDs 1, 2, 5, 9 on node A and 3, 6, 7 on node B. `filter(id > 3)` runs on each node independently (narrow). `groupBy("id")` must bring every row with `id = 1` from all nodes to one place (wide).

### Q42. `coalesce` vs `repartition`

|  | `coalesce(n)` | `repartition(n)` |
| --- | --- | --- |
| Direction | Decrease only | Increase or decrease |
| Shuffle | No full shuffle; merges existing partitions | Full shuffle |
| Result | Possibly uneven partition sizes | Evenly sized partitions; can also partition by column: `repartition("country")` |
| Typical use | Reduce output files before writing | Fix skew, increase parallelism, or co-locate by key |

Rule of thumb: fewer, larger files (around 128 MB to 1 GB) read faster than many small ones.

### Q43. `cache` vs `persist`

**When:** a DataFrame is reused by several actions (for example, a cleaned dataset used by three aggregations). Without caching, Spark recomputes it from the source every time.

**Difference:** `cache()` is `persist()` with the default storage level. For DataFrames that default is `MEMORY_AND_DISK`: keep partitions in memory, spill the rest to disk. `persist(level)` lets you choose:

| Storage level | Behaviour |
| --- | --- |
| `MEMORY_ONLY` | Memory only; partitions that do not fit are recomputed when needed |
| `MEMORY_AND_DISK` | Memory first, overflow to disk (DataFrame `cache()` default) |
| `DISK_ONLY` | Disk only |
| `MEMORY_ONLY_2`, `MEMORY_AND_DISK_2`... | Same, replicated on two nodes |

```python
from pyspark import StorageLevel
df.persist(StorageLevel.DISK_ONLY)
df.count()        # caching is lazy too: an action fills the cache
df.unpersist()    # free memory when done
```

Note: an RDD's `cache()` defaults to `MEMORY_ONLY`, which is a common follow-up.

### Q44. Why do partitions matter?

Partitions are how Spark achieves **parallelism** (massively parallel processing, MPP): data is split into chunks, and each partition is processed by one task on one core at the same time as the others. Too few partitions leaves cores idle and risks out-of-memory errors; too many creates scheduling overhead and tiny files. Useful numbers: `spark.sql.shuffle.partitions` defaults to 200, and a common target is roughly 100–200 MB per partition, or 2–3 tasks per core.

## Part 6 – Performance, memory and Delta Lake advantages

These questions separate mid-level from senior candidates. Most trace back to one cost: **shuffling** data across the network, and one risk: **one partition bigger than the memory that holds it**.

### Q45. What is Adaptive Query Execution (AQE)?

AQE re-optimizes the query **at runtime**, using real statistics collected after each shuffle stage, instead of relying only on estimates made before the job starts. It is enabled by default since Spark 3.2 (`spark.sql.adaptive.enabled = true`). Its three features:

1. **Coalescing shuffle partitions.** After a shuffle, many of the default 200 partitions may be tiny or empty. AQE merges small ones into reasonably sized partitions, so you rarely need to tune `spark.sql.shuffle.partitions` by hand.
2. **Switching join strategy.** If a table turns out to be small at runtime (for example after a filter), AQE converts a sort-merge join into a broadcast hash join.
3. **Optimizing skewed joins.** If one partition is far larger than the rest, AQE splits it into smaller sub-partitions and processes them in parallel.

Do not confuse AQE with **Dynamic Partition Pruning** (also Spark 3.0+), which uses a filter on a dimension table to skip partitions of the fact table at runtime.

### Q46. How do you handle skewed data?

**Skew** means a few keys hold most of the rows (for example 80% of orders have `country = 'IN'`). In a join or groupBy, all rows of a key go to one task, so one task runs for an hour while the others finish in a minute, or it runs out of memory.

**Answers, modern first:**

1. **AQE skew join** (`spark.sql.adaptive.skewJoin.enabled`, on by default with AQE). Mention it first.
2. **Broadcast the smaller side** if it fits (Q47): no shuffle means no skew.
3. **Salting,** the classic manual fix: append a random number to the hot key so its rows spread over N partitions, and replicate the other side N times so every salted key still finds its match.

```python
N = 10
big   = big.withColumn("salt", (F.rand() * N).cast("int"))
small = small.withColumn("salt", F.explode(F.array([F.lit(i) for i in range(N)])))

joined = big.join(small, on=["key", "salt"]).drop("salt")
```

### Q47. What is a broadcast join, and when should you use it?

When one side of a join is small (for example a 3 MB country lookup against a 1 TB fact table), Spark sends a full copy of the small table to every executor. Each executor then joins its own partitions of the big table locally, so **the big table is never shuffled**.

```python
result = big_df.join(F.broadcast(small_df), "country_code")
```

Spark broadcasts automatically when a table's estimated size is under `spark.sql.autoBroadcastJoinThreshold` (default 10 MB); the `broadcast()` hint forces it. Do not broadcast large tables: every executor and the driver must hold the copy in memory.

### Q48. What are broadcast variables?

A **read-only** variable that Spark ships to each executor **once** and caches there, instead of sending it with every task. Typical use: a lookup dictionary used inside a UDF or RDD function.

```python
country_names = spark.sparkContext.broadcast({"IN": "India", "US": "United States"})

@F.udf("string")
def to_name(code):
    return country_names.value.get(code)

df = df.withColumn("country", to_name("country_code"))
```

Benefit: less network traffic and serialization. A broadcast **join** (Q47) uses the same idea for a whole DataFrame.

### Q49. `df.show()` vs `df.collect()`

|  | `show(n)` | `collect()` |
| --- | --- | --- |
| Returns | Prints the first n rows (default 20) as a table | A Python list of **all** `Row` objects on the driver |
| Data moved to driver | Only n rows | The entire DataFrame |
| Risk | None | **Driver out-of-memory** on large data |
| Use for | Previewing | Small results you need as Python objects (for example a list of dates to loop over) |

Safer alternatives to `collect()`: `take(n)`, `limit(n).collect()`, or writing results out.

### Q50. What happens when a PySpark job runs out of memory?

**Driver OOM:** the driver is asked to hold too much. Causes: `collect()` or `toPandas()` on big data, broadcasting a large table, or huge numbers of tasks/files to track. Fixes: avoid collecting, write output instead, increase `spark.driver.memory` only if needed.

**Executor OOM:** a task's partition does not fit in executor memory. Causes: **skewed keys**, too few partitions (each too large), `explode` multiplying rows, large cached data. Fixes: handle skew (AQE, salting), `repartition` into more partitions, increase `spark.executor.memory` or memory overhead, cache less.

### Q51. What is spill?

When an executor's memory is not enough for a shuffle, sort or aggregation, Spark writes the excess to local disk and reads it back later. This is **spill**. The job succeeds but slows down, because disk is much slower than memory. You see it in the Spark UI as "Spill (Memory)" and "Spill (Disk)" on a stage. Causes and fixes are the same as executor OOM: skew, oversized partitions and too little memory. Spill is the warning sign before an OOM.

### Q52. Advantages of Delta Lake over plain Parquet/CSV files

Give three, and have one or two more ready:

1. **ACID transactions** through the `_delta_log` transaction log: a write fully succeeds or leaves no trace, readers never see half-written data, and concurrent writers cannot corrupt the table.
2. **Schema enforcement and evolution.** Plain data lakes are schema-on-read: bad data lands silently and breaks readers later. Delta checks the schema on write and rejects mismatches, while `mergeSchema` lets you evolve it deliberately.
3. **Performance features:** `OPTIMIZE`, Z-ordering, liquid clustering and file-level statistics for data skipping.
4. **Time travel and audit history** (`DESCRIBE HISTORY`, `VERSION AS OF`, `RESTORE`).
5. **DML:** `MERGE`, `UPDATE` and `DELETE` on data lake files, plus one table serving both batch and streaming.

## Corrections to the video

The video is a strong resource, but 14 of its answers would cost you points with a sharp interviewer. The code in this guide already uses the corrected versions.

| # | What the video says or does | The problem | Say or do this instead |
| --- | --- | --- | --- |
| 1 | Q1: `orderBy(...)` then `dropDuplicates` keeps the latest row | Spark does not guarantee that sort order survives into `dropDuplicates` | `row_number()` over a window, keep `rn = 1` (Q1) |
| 2 | `DateType` comes from `pyspark.sql.functions` | It lives in `pyspark.sql.types` | `from pyspark.sql.types import DateType` |
| 3 | Word count: `lower` applied after grouping | "Great" and "great" are already counted separately by then | Lowercase before `split`/`explode` (Q17) |
| 4 | Top product per month: groups by `month(date)` only | December 2023 and December 2024 merge | Group by `yyyy-MM` or by year and month (Q11) |
| 5 | "Keep the original order": partitions by name, ordered by age | That keeps the youngest row, not the first to arrive | Capture arrival order with `monotonically_increasing_id()` (Q3) |
| 6 | Use `inferSchema` on Parquet | Parquet stores its schema; `inferSchema` is a CSV/JSON option | Explicit schema or `printSchema` check (Q25) |
| 7 | AQE's features: "dynamic partition pruning", join strategy, skew | Feature 1 as described is coalescing shuffle partitions; DPP is a separate feature | Coalesce partitions, switch join strategy, skew join (Q45) |
| 8 | AQE auto-enabled "after 3.0 or 3.12" | There is no Spark 3.12 | On by default since Spark 3.2 |
| 9 | `OPTIMIZE` coalesces partitions; Z-order sorts data inside partitions | `OPTIMIZE` compacts small files; Z-order clusters values across files; skipping happens per file | See Q32 wording |
| 10 | Join types include "shuffle join" | Not a strategy name | Broadcast hash, shuffle hash, sort-merge, broadcast nested loop, cartesian |
| 11 | "If you want type safety, use DataFrame" (a slip) | Type safety is the Dataset's feature | Dataset, Scala/Java only (Q38) |
| 12 | `cache()` equals `persist(MEMORY_AND_DISK)` | True for DataFrames only | RDD `cache()` is `MEMORY_ONLY` (Q43) |
| 13 | "Delta by default uses schema on read" | Reversed | Plain lakes are schema-on-read; Delta enforces schema on write (Q52) |
| 14 | `concat_ws("-", first, last)` for a full name | Gives "John-Smith" | Use `" "` as the separator (Q18) |

**Practice environment update.** The video's setup uses Databricks Community Edition, which [retired on 1 January 2026](https://community.databricks.com/t5/announcements/psa-community-edition-retires-on-january-1-2026-move-to-the-free/td-p/141888). Use [Databricks Free Edition](https://docs.databricks.com/aws/en/getting-started/free-edition) instead. It runs on serverless compute, so low-level snippets that use `spark.sparkContext` (RDDs, broadcast variables) or `cache()` may not run there; everything DataFrame- and SQL-based works.

## Quick revision cheat-sheet

Read this the night before: find the scenario, then say the function out loud.

| Scenario in the question | Reach for | Q |
| --- | --- | --- |
| Keep latest / first / nth row per key | `row_number().over(Window.partitionBy(k).orderBy(...))` | 1, 3, 8 |
| Top N overall, ties matter | `dense_rank()` + filter | 7, 12 |
| Top item per group (month, region) | `groupBy` + `agg`, then `dense_rank` partitioned by the group | 11 |
| Running total | `sum().over(window.rowsBetween(unboundedPreceding, currentRow))` | 9 |
| Replace nulls | `fillna({"col": value})` | 2 |
| First non-null of several columns | `F.coalesce(c1, c2)` | 4 |
| If / else labels | `when(...).when(...).otherwise(...)` | 6, 22 |
| Audit timestamp | `current_timestamp()` | 5 |
| Days since a date | `datediff(current_date(), col)` | 16 |
| String → date | `to_date(col)` or `.cast("date")` | 1, 8 |
| Words in text → rows | `explode(split(lower(col), "\\s+"))` | 17 |
| Join strings | `concat_ws(sep, ...)` (skips nulls) | 18 |
| Pad a value | `lpad` / `rpad` | 20 |
| Starts with / first chars | `startswith`, `substring(col, 1, n)` (1-based) | 21 |
| Length of string / array | `length` / `size` | 15, 19, 22 |
| Values per group as a list | `collect_list` (all) / `collect_set` (unique) | 13, 14 |
| Flatten a struct | `select("s.field")` or `"s.*"` | 23 |
| Files with different columns | `.option("mergeSchema", "true")` | 24 |
| Bad CSV rows | `.option("mode", "DROPMALFORMED")` | 26 |
| SQL on a DataFrame | `createOrReplaceTempView`; across notebooks `GlobalTempView` + `global_temp.` | 27, 28 |
| Faster reads by column | `write.partitionBy(col)` | 29 |
| Smaller files | `.option("compression", "snappy")` | 30 |
| Upsert / avoid duplicates in Delta | `DeltaTable.merge(...).whenMatchedUpdateAll().whenNotMatchedInsertAll()` | 31 |
| Slow Delta table | `OPTIMIZE ... ZORDER BY (col)` or liquid clustering | 32 |
| Recover deleted data | `DESCRIBE HISTORY`, `RESTORE TABLE ... TO VERSION AS OF n` | 33 |
| Small table in a join | `F.broadcast(small_df)` | 47 |
| Skewed key | AQE skew join, then salting | 45, 46 |
| Reused DataFrame | `cache()` / `persist(level)` | 43 |
| Fewer output files | `coalesce(n)`; even redistribution: `repartition(n)` | 42 |
