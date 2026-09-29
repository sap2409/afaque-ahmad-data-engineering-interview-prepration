# Streaming Data Interview Questions – Notes

Sep 30, 2026 · @Sunil Patil

## Overview

These notes cover the streaming-data interview questions explained by Narendra Kumar (Senior Data Architect, 12 years in IT, Databricks Certified Solution Architect Champion). Topics run end to end: Kafka, Spark Structured Streaming, Lakeflow Spark Declarative Pipelines (SDP), Auto Loader, stateful processing, joins, real-time alerts and project scenarios.

## 1. Consuming data from Kafka

**Q: How do you consume data from a Kafka source?**

- You need a **consumer**. One consumer works; for parallelism, create multiple consumers inside a **consumer group**.
- Consumer options:
  - Custom Python consumer (application code)
  - **Spark Structured Streaming** (Databricks or any Spark platform)
  - **Lakeflow Spark Declarative Pipelines** (Databricks, managed)

**Q: How do you choose between these options?**

- Use Spark Streaming or Declarative Pipelines when you need parallel processing, or want the same structure for both streaming and batch data.
- Use a custom Python consumer when the requirement is very custom, the data is unstructured, or the consumer is an application rather than a big data system.

**Q: Spark Structured Streaming vs Lakeflow Declarative Pipelines — when to use which?**

| Aspect | Spark Structured Streaming | Lakeflow Spark Declarative Pipelines |
| --- | --- | --- |
| Type | Open source, available on all platforms | Databricks managed |
| Control | Code-oriented; you control checkpoints, write options, all parameters | Managed end to end; no checkpoint or write options to manage |
| Portability | Runs anywhere | Databricks only |
| Writing | You write the sink logic | Writes directly into Delta tables |
| Observability | Build it yourself | Built-in lineage and monitoring (which table feeds which) |
| Use when | You need more control or must run outside Databricks | You are on Databricks (default choice) |

## 2. Progress tracking: offsets and checkpoints

**Q: How is read progress tracked so the next run resumes from the right point?**

- Kafka topic data is split into **partitions** for parallel writes and reads.
- Each record in a partition has an **offset** (0, 1, 2, 3 …). Progress = the last offset read per partition.
- **Spark Structured Streaming:** you specify a `checkpointLocation`; offsets are stored and updated there on every read.
- **Lakeflow Declarative Pipelines:** checkpoints are managed automatically. Offsets are tracked internally (not directly visible), so the pipeline resumes from where it stopped.

## 3. Kafka core concepts, connection and triggers

**Q: Explain topic, producer and broker.**

| Term | Meaning |
| --- | --- |
| Topic | Named entity you write data to and read data from; comparable to a table in a database |
| Producer | Anything (web app, mobile app, service) that pushes generated data into the Kafka cluster |
| Broker | A Kafka server that stores the data; several brokers form a cluster |
| Cluster | Group of brokers; one cluster can hold many topics, stored across its brokers |
| Partition | Sub-division of a topic enabling parallel reads/writes |
| Offset | Sequential position of a record within a partition |

**Q: What connection details do you need to read Kafka from Spark Streaming?** (tests hands-on experience)

1. **Bootstrap servers** — URL(s) of the Kafka cluster
2. **API key** and
3. **API secret** — passed through a **JAAS config** (Java Authentication and Authorization Service), a standard format that takes the key and secret.

```python
df = (spark.readStream.format("kafka")
  .option("kafka.bootstrap.servers", "<host:9092>")
  .option("subscribe", "<topic>")
  .option("kafka.security.protocol", "SASL_SSL")
  .option("kafka.sasl.mechanism", "PLAIN")
  .option("kafka.sasl.jaas.config",
          'org.apache.kafka.common.security.plain.PlainLoginModule required '
          'username="<API_KEY>" password="<API_SECRET>";')
  .load())
```

**Q: What trigger types are available in Spark Streaming?**

| Trigger | Behaviour | Example |
| --- | --- | --- |
| Available now | Batch-like: processes everything available so far once, then stops | `.trigger(availableNow=True)` |
| Processing time | Runs micro-batches at a fixed interval, reading new data each time | `.trigger(processingTime="10 seconds")` |
| Continuous | Never stops; reads and writes to the target continuously | `.trigger(continuous="1 second")` |

## 4. File-based streaming with Auto Loader

**Q: Files arrive as a streaming source. How do you ingest them in Databricks?**

- Use **Auto Loader**, a Databricks feature that reads **only new files** on each run.
- It is tightly integrated with Spark Structured Streaming: same code pattern, different options.

**Q: How exactly do you use Auto Loader?**

- In `spark.readStream`, set `format("cloudFiles")` — that means Auto Loader.
- Add options: the file format (`cloudFiles.format`), the schema location, and whether to infer column types.

```python
df = (spark.readStream.format("cloudFiles")
  .option("cloudFiles.format", "json")
  .option("cloudFiles.schemaLocation", "/path/_schema")
  .option("cloudFiles.inferColumnTypes", "true")
  .load("/path/landing/"))
```

**Q: How does Auto Loader know which files it has already read?**

- Same as Kafka: a **checkpoint location** stores the progress (files processed so far), so they are not read again.

**Q: How do you handle corrupted records in the files?**

- Auto Loader automatically adds a **`_rescued_data`** column.
- Records that don't match the schema are written into `_rescued_data`; the main columns are left null.
- You then filter or route those rows and handle them however needed.

## 5. Transformations: state, late data and aggregations

**Q: What is stateless vs stateful processing?**

|  | Stateless | Stateful |
| --- | --- | --- |
| Definition | Each record processed independently; nothing kept in memory | Must keep previous events in memory; output depends on other events |
| Examples | Simple filters, one-to-one record transforms, routing one stream to multiple targets by condition | Aggregations, windows, running totals, fraud detection across multiple events |

**Q: How do you handle late-arriving data?** (the most important streaming question)

- Define a **watermark** on a timestamp column, with the allowed delay in seconds or minutes.

```python
df.withWatermark("event_time", "10 minutes")
```

**Follow-up: Is the delay measured from the current time?**

- No. It is measured from the **latest value of the watermark column** seen so far (max event time).
- Records up to 10 minutes older than that latest value are accepted; anything older is dropped.

**Q: How do you aggregate streaming data (count, sum) when it never stops?**

1. Define a **watermark** — how much late data to allow.
2. Define a **window** — the interval over which to aggregate.
3. Apply the aggregation (`count`, `sum`, …).

**Q: What window types exist?**

| Window | How it works | Example (10-min window) |
| --- | --- | --- |
| Tumbling | Fixed interval, no overlap | 0–10, 10–20, 20–30 … |
| Sliding | Fixed window length plus a slide interval; windows overlap | Slide 5 min: 0–10, 5–15, 10–20 … (every 5 min, aggregate the last 10 min) |

```python
from pyspark.sql import functions as F

# Tumbling window: every 1 minute
df.withWatermark("event_time", "10 minutes") \
  .groupBy(F.window("event_time", "1 minute")).count()

# Sliding window: 10-minute window, sliding every 5 minutes
df.withWatermark("event_time", "10 minutes") \
  .groupBy(F.window("event_time", "10 minutes", "5 minutes")).count()
```

## 6. Joins

**Q: How do you join streaming data with static data?**

- Read the stream with `readStream`; read the static table normally (`spark.read.table`).
- Join them like any two DataFrames: join column + join type. Nothing special is needed.

```python
stream_df = spark.readStream.table("orders_stream")
static_df = spark.read.table("customers")
joined = stream_df.join(static_df, "customer_id", "left")
```

**Follow-up: Is a stream-static join stateful or stateless?**

- **Stateless.** Each streaming record is picked up on its own and matched against the static data; the result is written immediately. It does not depend on any other streaming record.

**Q: How do you join two streams? Is it stateful?**

1. Read both sources as streaming DataFrames.
2. Apply a **watermark** on each to bound how much late data is allowed.
3. Join them as a normal DataFrame join (join column + join type).

```python
a = spark.readStream.table("impressions").withWatermark("imp_time", "10 minutes")
b = spark.readStream.table("clicks").withWatermark("click_time", "20 minutes")
joined = a.join(b, "ad_id", "inner")
```

- **Stateful.** A record from one stream may need to wait for its match from the other stream within the time window, so state is held until the watermark confirms all records have arrived.

## 7. Real-time alerts (e.g. fraud detection)

**Q: How do you send real-time alerts based on conditions in streaming data?**

1. **Apply alert conditions** to the incoming stream (e.g. `amount > threshold`).
2. **Write matches to a target streaming table** holding the alert details needed for the notification.
3. **Create a sink** in Lakeflow Spark Declarative Pipelines, configured with **SMTP connection details** (the demo uses Gmail; any SMTP server works).
4. **Read the alerts table with an append flow** and continuously append to the sink.
5. **Process batch by batch** (foreachBatch style): split each batch into rows, build the email body, and send one email per row via SMTP.

In short: stream → condition → alerts table → append flow → SMTP sink → email per alert.

## 8. Scenario / project-based questions

**Q: How do you control streaming data during development?**

- Build a **dummy producer** in Python that reads from a file or generates data and sends it on demand.
- Use it to simulate the stream, run and test the pipeline, then run it continuously to confirm data keeps flowing.

**Q: How do you debug and fix errors in production?**

- **Compute issues** (pipeline stopped): admins fix the infrastructure; on restart the pipeline resumes from its checkpoint automatically.
- **Data issues:**
  - The **bronze layer** stores raw data as key-value pairs (as Kafka delivers it) with no transformation, so it never fails.
  - Keep extra metadata columns in bronze: **partition, offset**, timestamp, etc.
  - When a later layer fails, identify the failing records, replicate that bronze data in the **dev environment**, reproduce the same operation, fix the issue, and rerun.

**Q: How do you set a pipeline to run continuously vs on trigger?**

- In every Declarative Pipeline's settings there is a **Pipeline mode** option:
  - **Continuous** — never stops.
  - **Triggered** — runs when triggered; add a **schedule** at the frequency you want (minimum every 1 minute).

## Quick revision

| Question | One-line answer |
| --- | --- |
| Consume Kafka? | Consumer / consumer group via Python, Spark Structured Streaming or Lakeflow SDP |
| Spark Streaming vs SDP? | Streaming = control + portability; SDP = managed, auto checkpoints, lineage (default on Databricks) |
| Progress tracking? | Offsets per partition, stored in checkpoint (auto-managed in SDP) |
| Kafka connection? | Bootstrap servers + API key + secret via JAAS config |
| Triggers? | availableNow, processingTime, continuous |
| File streaming? | Auto Loader: `format("cloudFiles")` + schema location + checkpoint |
| Corrupt records? | Go to `_rescued_data` column |
| Stateless vs stateful? | Independent records vs needs prior events (aggregations, windows, fraud) |
| Late data? | `withWatermark(col, delay)`, measured from max event time seen |
| Streaming aggregation? | Watermark + window + agg |
| Window types? | Tumbling (no overlap) vs sliding (overlap via slide duration) |
| Stream-static join? | Normal join; stateless |
| Stream-stream join? | Watermark both, then join; stateful |
| Real-time alerts? | Condition → alerts table → append flow → SMTP sink → email |
| Dev testing? | Dummy Python producer |
| Prod debugging? | Raw bronze with partition/offset; replay in dev |
| Run mode? | Pipeline mode: continuous or triggered + schedule (min 1 min) |
