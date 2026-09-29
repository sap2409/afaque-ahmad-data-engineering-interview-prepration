# Data Engineering System Design Interviews — Detailed Notes

Sep 28, 2026 · @Sunil Patil · Source: "How I Mastered System Design Interviews" (YouTube)

## Why this round is hard

Candidates rarely fail for lack of knowledge alone; they fail because the round tests how you **navigate ambiguity**. Knowledge is the entry bar — without it you fail by default — but structure is what passes.

- **DSA has a destination.** A two-pointer or DP problem has a correct, optimal answer you arrive at by breaking it down.
- **System design is open-ended.** Ask 10 senior engineers to design the same ingestion pipeline and you get 10 valid architectures, each with different trade-offs (cost, latency, complexity, failure handling).
- **You are graded on reasoning, not the answer.** The interviewer changes constraints and checks whether your reasoning still holds:
  - What if volume grows 10x?
  - What if the business wants the dashboard in 10 minutes instead of 1 day?
  - What if a source becomes unreliable?
- **SWE prep doesn't transfer.** URL-shortener and Twitter-feed examples don't cover what DE interviews probe: ingestion patterns, data modeling, batch vs streaming, schema evolution, idempotency, backfills, cost and scale.

The topic list is large, but the **approach is always the same**. A lakehouse for e-commerce, a recommendation pipeline, fraud detection, or a customer-360 platform can all be broken down with one framework (next section).

## The 6-step framework

Carry these six steps, in order, into every DE system design interview, whatever the company or domain. Try to touch as many as you can.

1. **Requirements gathering** — understand the problem before drawing a single box: who uses it, what it must do, latency, volume.
2. **Pipeline design** — how data moves from source to destination: batch or streaming; Lambda, Kappa or lakehouse; orchestrator (Airflow, Prefect, Dagster, Mage).
3. **Data modeling** — the shape of the data: star vs snowflake, facts and dimensions, medallion layers (bronze/silver/gold), SCD type 1/2/3. Typically the dimensional model lives in **silver** and aggregated metrics in **gold**.
4. **Storage & file formats** — where data lives and in what format (Parquet, Avro, Delta, Iceberg), plus optimization (partitioning, bucketing, liquid clustering). Drives cost heavily.
5. **Data quality & observability** — how you know outputs are correct: DQ rules, testing, monitoring, alerting, SLAs. This builds trust in the platform.
6. **Scalability, backfills & DataOps** — can it handle 10x load, fail gracefully, reprocess 3 months of data, and is it idempotent?

**How interviewers use it: "wide, then zoom."** They open broad ("design a pipeline that tracks user activity on an e-commerce platform") and then drill into two or three of these six areas that interest them.

## Step 1: Requirements gathering

Spend the first \~5 minutes asking questions, not drawing Kafka/Spark/Airflow boxes. It is the most underrated step and the one that most quickly separates strong candidates from average ones. Nail down two kinds of requirements.

### Functional requirements — what the system must do

Ask three questions:

1. **Who is the end user?** This alone can pick the architecture.
   - Marketing analyst running SQL → batch is fine.
   - ML team feeding a feature store for recommendations → streaming.
   - Product team monitoring real-time metrics → streaming.
2. **What do they need?** Aggregated metrics, raw events, or historical snapshots?
3. **How will they access it?** Direct SQL, a BI tool (Tableau, Power BI), or REST APIs?

**Example — marketing purchase funnel.** Marketing wants to see where users drop off between landing and buying:

| Stage | Users | Drop-off from previous stage |
| --- | --- | --- |
| Home page | 100K | — |
| Search | 60K | 40% |
| View product | 30K | 50% |
| Add to cart | 10K | 67% |
| Purchase | 4K | 60% |

They want to slice it by segments such as **user type** (new vs returning, free vs paid) and **device type** (e.g., desktop converting at 2% vs mobile at 6% hints the desktop checkout is broken).

**Resulting functional requirement:** funnel metrics at daily granularity, segmented by user type, device type and other dimensions, accessed in Tableau.

### Non-functional requirements — how the system must behave

| Area | Question to ask | Why it matters |
| --- | --- | --- |
| Latency SLA | Fresh within a minute, or is 1 hour OK? | ≤ 1 hour → batch. < 1 minute → streaming, with higher cost, complexity and ops overhead. This one answer can change the whole design. |
| Volume | 1 GB, 10 TB or PB per day? | \~1 GB/day → single-node Pandas/Polars. TB–PB → distributed Spark plus partitioning/bucketing/liquid clustering so queries don't scan everything. |
| Availability | How much downtime is tolerable (e.g., 10 min during a deploy)? | Sets redundancy and deployment approach. |
| Data retention | Keep forever, or archive after e.g. 90 days? | Drives storage cost. |

### Same question, two designs

"Design a pipeline to track user activity on an e-commerce platform" yields completely different systems depending on the consumer:

|  | Scenario A: Marketing team | Scenario B: ML team |
| --- | --- | --- |
| Need | Daily reports on page views, add-to-cart, funnel, in Tableau | Real-time recommendations; every action in the feature store within seconds (e.g., Uber Eats trending restaurants, DoorDash re-ranking on what you browsed 30 s ago) |
| Latency | 1 hour is fine | Seconds |
| Design | Classic batch: Spark jobs, Airflow, Delta tables, fact/dim model | Streaming: Kafka, Flink or Spark Structured Streaming, Delta, online feature store read at inference time |

### Back-of-the-envelope calculation

After requirements, do quick math. Most candidates skip it; doing it shows you think in numbers that drive decisions. Agree each assumption with the interviewer and tune it as you go.

**Scenario A worked through:**

- 5M daily active users × 2 sessions/day × 30 events/session = **300M events/day**
- Event = one JSON (user\_id, session\_id, event\_type, timestamp, page\_url…) ≈ 500 B–1 KB → assume **700 bytes**
- 300M × 700 B = **210 GB/day** ≈ **6.3 TB/month**
- 12 months in bronze ≈ **75 TB raw**; with Parquet/Delta at 2–5x compression ≈ **15–38 TB** on S3 — manageable
- Partition by event date (YYYYMMDD) so hourly jobs scan only what they need

**Four decisions fell out of that math:**

1. Compute: Spark (Pandas/Polars can't handle 210 GB/day).
2. Storage format: Parquet or Delta.
3. Orchestration: Airflow scheduled jobs are enough.
4. Ruled out an entire category: no Kafka/Flink streaming stack needed.

## Step 2: Pipeline design

Every interviewer asks some version of "how do you move and transform data from A to B?" Your first decision is **batch or streaming**.

### Batch vs streaming

|  | Batch | Streaming |
| --- | --- | --- |
| Use when | Cost matters more than latency: hourly/daily reporting, analytics. Runs \~90% of pipelines. | Latency matters most: real-time dashboards, fraud detection, event-driven systems, SLA under \~1 minute |
| Typical stack | Spark for heavy lifting; Airflow / Prefect / Dagster to orchestrate | Kafka for ingestion and buffering; Spark Structured Streaming or Flink to process; Delta Lake for historical storage; Redis / DynamoDB / Cassandra for low-latency serving |
| Flow | Source → extract → object store (S3/ADLS) → bronze → silver → gold → dashboard | App/website/DB events → Kafka → stream processor (enrich, filter) → serving store read by app or live dashboard |

**Batch example — daily sales report.** An Airflow DAG kicks off at 2:00 a.m., extracts from Postgres into bronze. A Spark job cleans and transforms into silver. Another job aggregates by product and region into gold. By \~6:00 a.m. Tableau refreshes with fresh numbers.

**Kafka's role in streaming.** It decouples producers from consumers. Events are retained for a configurable period (days, weeks, months) and each consumer reads at its own pace using **offsets**.

**Streaming example — ride-hailing driver earnings.** When a ride ends, a "ride completed" event (fare, surge, driver\_id) goes to Kafka. Spark Structured Streaming consumes it, enriches it with driver metadata from a lookup table, updates the driver's running total, and writes it to Redis, where the driver app reads earnings instantly.

### Lambda architecture — two parallel paths

Use when you need both historical and real-time data in one system. Incoming data splits into:

- **Batch layer** — processes the full historical dataset.
- **Speed layer** — processes real-time data as it arrives.
- Both write to a **serving layer** (shared or separate).

**Example — ride-hailing on AWS.**

1. Tapping "request a ride" sends an event to MSK (managed Kafka). Drivers continuously send location pings to MSK too.
2. **Consumer group 1 (speed): Flink** reads the request within milliseconds, matches nearby drivers, computes surge for the area, and writes fare and driver details (e.g., "₹330, driver arriving in 4 min") to ElastiCache.
3. **Consumer group 2 (batch): Data Firehose** reads the same events, buffers them and flushes raw to S3. It doesn't process anything and doesn't care about Flink — if Flink is down for a deploy, Firehose keeps writing.
4. **Nightly batch:** Airflow triggers EMR (managed Spark) to process bronze → silver → gold for questions needing history: highest-demand pickup zones on Friday 6–9 p.m., routes with 3x cancellation rates, where to pre-position drivers before a cricket match at Wankhede.
5. **Serving combines both.** Real-time is current but noisy with no context; batch is accurate but stale. Surge example: speed layer says 47 requests vs 12 drivers in Bandra (\~1.8x); batch says Friday 7–9 p.m. in Bandra averages 2.1x. Together they give a grounded estimate.

### Kappa architecture — one streaming path

Kappa asks "why maintain two systems?" Everything flows through **one streaming pipeline**: one codebase, one set of logic, simpler to operate.

- **Key difference: Kafka becomes your storage.** It is configured to retain data for days, weeks or months, acting as both ingestion layer and historical store.
- Flow: events (app, CDC, IoT) → Kafka (e.g., 30-day retention) → Spark Structured Streaming → serving layer (Redis/DynamoDB).
- **Reprocessing (logic changed, redo last 2 weeks):** create a new consumer group (C2) pointed at the offset from 2 weeks ago, run a separate replay processor, let it catch up to the present, then swap outputs and retire the old one.

**Limitations:** reprocessing 6 months requires Kafka to retain 6 months (expensive) or the data is already gone. Replaying millions of events through a processor built for small continuous batches works but is far slower than a parallel Spark batch job — not scalable.

**What most companies do:** not pure Kappa. They land data in both Kafka and S3/ADLS for historical processing — essentially the **lakehouse** approach (worth reading further).

## Step 3: Data modeling

Drawing facts and dimensions is easy; interviewers push back with "why not denormalize?", "what happens when this dimension changes?", "how does this perform at scale?" Know four things well.

### 1. Medallion architecture

A layered way to organize a lakehouse.

| Layer | What it holds | Rules |
| --- | --- | --- |
| Bronze | Raw data exactly as received: JSON dumps, CDC logs, raw API responses | No cleaning or transformation. It is your safety net — reprocess from here if anything downstream breaks. |
| Silver | Cleaned and structured: JSON parsed, schema enforced, deduplicated, type-cast | Usually where the dimensional model (facts and dims) lives; the trusted source of truth for analysts and engineers |
| Gold | Pre-aggregated, pre-joined datasets with business metrics | Analysts query directly; fast, no complex joins |

**Spotify example:**

- **Bronze:** `song_played` JSON events (timestamp, user\_id, song\_id, duration, device, app\_version). Expect schema drift across app versions, nulls and duplicates.
- **Silver:** `fact_streams` (user\_id, song\_id, played\_at, duration, device\_type) and `dim_songs` (song\_id, title, artist, genre, release\_year).
- **Gold:** `daily_artist_streams` (artist\_id, date, total\_streams, unique\_listeners) — built by joining and aggregating silver.

### 2. Star schema vs denormalization (OBT)

|  | Star schema (normalized) | One Big Table (denormalized) |
| --- | --- | --- |
| Shape | Separate `fact_orders`, `dim_customer`, `dim_product` | Everything pre-joined into one wide `obt_orders` table |
| Strengths | BI tools are optimized for it; easy to maintain; strong performance with well-indexed joins | No joins for users; fast scans, no shuffles. Best for a known, repeated query pattern (e.g., 20 people querying revenue by region and category over last 30 days daily) |
| Weakness | Consumers join several tables on every query | Dimension changes are painful: renaming category "Clothes" → "Apparel" means updating millions of duplicated rows, multiplied across every changing dimension |
| Change cost | Update one row in the dimension | Update every row carrying that value |

Be ready to justify your choice by its real-world impact.

### 3. Slowly changing dimensions (SCD)

Scenario: a Spotify user upgrades from Free to Premium. Overwrite or keep history?

- **SCD Type 1 — overwrite.** Simple, but history is lost: you can't answer "how many users were on Free 6 months ago?"
- **SCD Type 2 — keep both rows.** Close the old row by setting `valid_to` to the change date; insert a new row with `valid_from` = change date and `valid_to` = NULL (NULL = current). Lets you query the state at any point in time.

| user\_id | tier | valid\_from | valid\_to |
| --- | --- | --- | --- |
| 42 | Free | 2024-01-01 | upgrade date |
| 42 | Premium | upgrade date | NULL |

SCD2 is the standard where history matters: customer tier over time, subscription plan, user geography.

### 4. Partitioning and optimization

- **Default: partition by event date.** Time-series queries almost always filter on date, so this cuts scanned data dramatically.
- **Secondary partition key:** if \~80% of queries also filter by a column like country/region, add it.
- **Weakness of partitioning:** the column is fixed up front. If queries shift from date to region, the strategy stops helping and changing it means rewriting the whole dataset.
- **Liquid clustering (Databricks):** no fixed physical partitions; data is dynamically reorganized on chosen columns (e.g., event\_date + region). When query patterns change, update the clustering columns — no full rewrite.

## Step 4: Storage & file formats

Format choice alone can change a query from a full scan to under 2% of the data. Example: a table with 200 columns and 1B rows where analysts read only 3 columns. In CSV (row-based) the engine reads every column of every row; in Parquet (columnar) it reads 3 and skips 197.

### Row-based vs columnar

- **Row-based:** each full row is stored together as one chunk.
- **Columnar:** all values of one column are stored together.

|  | Parquet (columnar) | Avro (row-based) |
| --- | --- | --- |
| Best for | Read-heavy analytics; the default for analytical workloads | Write-heavy workloads, especially streaming |
| Why | **Column pruning:** `SELECT AVG(revenue) FROM sales WHERE city = 'Bengaluru'` reads only `revenue` and `city`, skipping the other 198 columns. File-level statistics let the engine **skip whole files** that can't match the filter. | Each event (ride completed, click, order) is serialized as one complete row and written in one shot — no reorganizing by column |
| Trade-off | Must buffer a large batch and reorganize column-by-column before writing — fine hourly/daily, bad for 10K events/s arriving in Kafka | A row store must read whole rows to answer column queries |

CSV (row) and ORC (columnar) fall in the same two buckets — worth reading about separately.

### Why Delta Lake / Iceberg (and Hudi) matter

Early data lakes just dumped raw Parquet on S3. Three problems:

1. **No ACID guarantees.** Nothing manages the files. Two Spark jobs writing the same table can overwrite each other; records get corrupted or go missing — silently, with no error.
2. **No schema enforcement.** A string written into an integer column just goes in; you find out only when something downstream breaks.
3. **Small-file problem.** Streaming writes thousands of tiny KB-sized files. Spark must open and close every one; that I/O overhead makes queries much slower than reading a few large files.

Open table formats fix these with:

- **ACID transactions** — multiple writers without corruption.
- **Time travel** — query the table as of yesterday, last week, last month.
- **Schema evolution** — add a column without breaking pipelines.
- **`OPTIMIZE` (Delta)** — compacts many small files into fewer large ones.

### Relational vs non-relational databases

A common interview question when choosing a store.

|  | Relational (SQL) | Non-relational (NoSQL) |
| --- | --- | --- |
| Choose when | Data is structured with clear relationships between entities; you need strong consistency / ACID; complex queries answer business questions | You need a flexible structure; data is semi-/unstructured or evolves rapidly; you can trade strict consistency for speed and availability |
| Examples | Orders, payments, transactional systems | Logs, social media data, IoT data |

## Step 5: Data quality & observability

Everything so far assumes the data is correct — a dangerous assumption. Sources change without warning, fields go null, row counts drop, and without checks the pipeline "succeeds," writes bad data, and stakeholders act on it before anyone notices.

### Data quality dimensions

Don't say "I'll add some checks." Say which check and which dimension it belongs to.

| Dimension | Question | Example failure |
| --- | --- | --- |
| Completeness | Is all the data there? | Orders table normally gets 10M rows/day, today 7.5M; customer email 40% null |
| Accuracy | Is the data correct? | Order amount of −500; delivery time before order time |
| Consistency | Do systems agree? | Orders say March revenue is $2M, payments say $1.8M |
| Freshness | Is it current enough? | SLA is a daily refresh but the last update was 3 days ago |
| Uniqueness | Any duplicates? | A retry after a timeout wrote the same batch twice, inflating every metric |

### Data contracts — shift quality left

DQ checks catch problems *after* data enters your system. A **data contract** is a formal agreement between producer (e.g., a microservice) and consumer (your pipeline) on what will be sent: schema, data types, required fields, allowed values.

- Example: `order_id` is a string, `amount` is decimal, `timestamp` is ISO format; schema changes only via a versioned migration (V1 → V2) with 2 weeks' notice.
- A contract violation is flagged **before data reaches bronze**.
- Enforced in practice via a **schema registry** for streaming (Avro + Confluent Schema Registry) and schema validation tools (e.g., Great Expectations).

### Pipeline-level observability

DQ checks the data; observability checks the pipeline producing it.

- Did the DAG run? Did it fail? How long did it take?
- A job taking 3 hours instead of 40 minutes suggests a volume spike or an undersized cluster.
- Tools: built-in metrics in Airflow / Prefect / Dagster, or Datadog, or CloudWatch on AWS.

## Step 6: Resilience — idempotency, backfills, schema evolution

Things will break and business logic will change. Show you can rerun without duplicates, backfill history, and absorb upstream changes without taking production down.

### Idempotency — the golden rule

Running the pipeline once or five times with the same input gives the **identical output**: no duplicates, no missing data. It sounds obvious but needs intentional design.

- Use **`MERGE` instead of `INSERT`** for incremental loads. MERGE updates a record if it exists and inserts it if not; rerunning it leaves the same row count. Rerunning an INSERT duplicates rows.

### Backfills

A backfill reruns the pipeline over historical data already processed. It happens often.

- **Bug fix:** delivery duration was computed as `delivered_at − created_at` instead of `delivered_at − dispatched_at` for 3 months. Fix the code, then reprocess those 3 months.
- **Definition change:** the business decides "total orders" should exclude orders cancelled within 24 hours. New runs apply the filter; old reports overstate, so reprocess history with the new definition.

**How:** with an idempotent pipeline on Delta/Iceberg, **overwrite only the affected partitions** (e.g., 2026-01-01 to 2026-04-01). Writes are **atomic** thanks to ACID: the whole partition is replaced or nothing changes. If the job fails midway, old data stays intact and consumers never see partial data.

### Schema evolution

Upstream schemas will change eventually: payments adds a `discount_code` column, mobile renames `user_location` to `device_location`, an API silently turns an integer into a string. Small changes, but any can fail Spark jobs, write garbage, or corrupt dashboards.

**Approach: flexible at bronze, strict at silver and gold.**

- **Bronze** accepts everything as-is — no validation or rejection. New columns are added automatically via Delta's **`mergeSchema`** option. Intentional: bronze is the safety net that captures all raw data.
- **Silver/gold** enforce a strict schema because analysts, dashboards and ML pipelines depend on it (amount is always decimal, user\_id always exists). A new upstream column lands in bronze but reaches silver only after pipeline code explicitly maps, validates and places it.

In short: absorb change at the bottom, enforce consistency at the top.

## Worked example: real-time analytics for a food delivery platform

**Question (Uber Eats / Swiggy / Zomato style):** design a pipeline that tracks order events and delivery metrics, serving (1) a real-time operational dashboard for restaurant partners and (2) daily aggregated business reports for executives.

**Answer in one line:** one data platform, one event stream (Kafka) as the source of truth, two consumption paths — a Lambda-style design.

### 1. Clarifying questions (first \~5 minutes)

Open with "I'd like to ask a few clarifying questions before I start." The interviewer is waiting for it and will silently note it if you skip it.

| Question | Assumed answer |
| --- | --- |
| Who are the end users? | Restaurant partners (live orders) and the executive/finance team (business reports) — different SLAs |
| Latency SLAs? | Restaurant dashboard ≤ 2 minutes end to end; executive reports daily batch |
| Data volume? | 5M orders/day; peak dinner rush (7–9 p.m.) ≈ 500K orders/hour |
| Which events? | 6 types, chronological: order\_placed, restaurant\_confirmed, driver\_assigned, driver\_picked\_up, driver\_delivered, order\_cancelled |
| Retention? | 2 years for analytics; 90 days for real-time serving |

### 2. Back-of-the-envelope math

- **Events:** 5M orders × 6 events = 30M events/day → 30M ÷ 86,400 s ≈ **350 events/s** average
- **Peak:** 500K orders/hr × 6 = 3M events/hr ÷ 3,600 s ≈ **833 events/s**
- **Kafka verdict:** 350–833 events/s is easy — a modest 3-broker cluster; 6–12 partitions give enough parallelism for Spark consumers
- **Storage:** event JSON (order\_id, event\_type, timestamp, restaurant\_id, driver\_id, customer\_id, coordinates) ≈ 1 KB → 30 GB/day → × 365 × 2 years ≈ **22 TB raw**; 2–5x Parquet compression → **4–11 TB** on S3/ADLS
- **Serving layer:** the dashboard needs only 10–15 precomputed fields per restaurant (active orders, avg delivery time last hour, orders completed) ≈ 1 KB × 50K active restaurants = **\~50 MB** — one Redis instance holds it in memory
- **Conclusion:** moderate scale, not massive; Kafka, storage and Redis are all comfortably sized

### 3. Pipeline design

&#91;embedded content: food delivery analytics · Lambda-style, 2 paths\]

Kafka (partitioned by restaurant\_id so each restaurant's events stay ordered) is the single source of truth; each path is a shaped view of it.

- **Path A — streaming:** Kafka → Spark Structured Streaming computes running metrics → NoSQL store (Redis) → live restaurant dashboard. Be ready to explain exactly which metrics and how they are calculated.
- **Path B — batch:** the same events are synced to S3/ADLS (on AWS: MSK → Firehose → S3) → bronze → silver → gold → executive dashboards.

**Likely deep-dive questions once the design is up:**

- **Kafka:** exactly-once vs at-least-once semantics, partitioning strategy, retention, log compaction, watermarking, late-arriving data.
- **Spark:** internals, AQE, query plans, shuffles, join strategies, and storage optimization (partitioning, bucketing, Z-ordering, liquid clustering).
- Brush up on the tools you've actually used so you can reason about your past choices.

### 4. Data model (medallion)

| Layer | Contents | Format |
| --- | --- | --- |
| Bronze | Raw order-event JSON from Kafka (order\_id, event\_type, timestamp, restaurant\_id, customer\_id…). Not cleaned; immutable source of truth. | Parquet |
| Silver | `fact_orders` (order\_id, restaurant\_id, driver\_id, customer\_id, placed\_at, delivered\_at, cancelled\_at, total\_amount, delivery\_duration), partitioned or liquid-clustered by order date; `dim_restaurants` (id, name, city, active\_since); `dim_drivers` (id, name, city, rating, vehicle\_type). Could extend with fact\_payments, dim\_food\_items, etc. | Delta |
| Gold | `daily_restaurant_metrics` for executive reports (the live dashboard is served from Redis, not gold) | Delta |

### 5. Data quality checks by layer

Map every check to a DQ dimension (completeness, accuracy, consistency, freshness, uniqueness).

- **Ingestion:** data contracts via the schema registry reject malformed records before bronze.
- **Bronze:** event\_type in the allowed set; order\_id + event\_type not duplicated (log, don't drop); customer\_id not null.
- **Silver:**
  - Null checks on key attributes.
  - Referential integrity: every restaurant\_id in fact\_orders exists in dim\_restaurants (orphans mean a restaurant was never onboarded).
  - Business rule: delivery duration between 1 and 180 minutes.
  - Time ordering: picked\_up\_at after placed\_at; delivered\_at after picked\_up\_at.
  - Volume: today's row count vs the 7-day rolling average; flag big deviations.
- **Gold:** cancellation rate between 0 and 100%; average delivery minutes positive and within a business-defined sensible range.

### 6. Scalability & operations

- **Backfill strategy:** when gold logic changes, rerun cleanly for a given start/end date range.
- **Idempotency:** n runs with the same input give the same output — non-negotiable.
- **Schema evolution:** schema-on-read at bronze; strict schema at silver/gold, with `mergeSchema` enabled for additive changes.

You won't cover every topic in depth in 45 minutes, and that's fine — showing this structure signals well-rounded thinking.

## Key takeaways & cheat sheet

Strong candidates slow down, ask questions, and make their reasoning visible. Weak ones jump straight to tools and design only for the happy path.

**What separates pass from fail:**

- Spend real time on requirements before touching tools.
- Design for failure, not only the happy path.
- Say your trade-offs out loud: why this tool or path over the alternative, with pros and cons.

**Quick-revision decision table:**

| If you hear… | Lean toward… |
| --- | --- |
| Freshness of an hour or a day is fine | Batch: Spark + Airflow + Delta |
| Freshness under \~1 minute | Streaming: Kafka + Spark Structured Streaming/Flink + Redis/DynamoDB |
| Need both real-time and historical | Lambda-style (or lakehouse: Kafka plus S3/ADLS) |
| \~1 GB/day | Pandas/Polars on a single node |
| TB–PB/day | Spark + partitioning / liquid clustering |
| Read-heavy analytics | Parquet / Delta (columnar) |
| Write-heavy streaming events | Avro (row-based) + schema registry |
| Many writers, schema changes, small files | Delta / Iceberg / Hudi (ACID, time travel, OPTIMIZE) |
| History of a dimension matters | SCD Type 2 |
| A fixed, repeated dashboard query | OBT / gold aggregate |
| Dimensions change often | Star schema |
| Reruns and backfills | MERGE, idempotent jobs, atomic partition overwrite |
| Upstream schema changes | Flexible bronze (mergeSchema), strict silver/gold |

**Interview flow to rehearse (45 min):** clarify requirements → back-of-envelope math → pipeline design → data model → storage/formats → data quality → scalability and operations. Expect the interviewer to zoom into two or three of these.
