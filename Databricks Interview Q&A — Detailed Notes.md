# Databricks Interview Q&A — Detailed Notes

Sep 30, 2026 · @Sunil Patil

## Overview

These notes turn a \~58-minute Databricks interview Q&A video into question-by-question study notes. The speaker, Narendra Kumar, is a senior data architect (12 years in IT, 8 in data engineering) who holds the Databricks and Microsoft data engineering certifications and interviews Databricks candidates regularly.

Each section below follows the order of the video: the likely interview question in bold, then the answer to give. Unity Catalog comes first because, in the speaker's words, every Databricks interview will include it. A quick-revision cheat sheet closes the notes.

Prices quoted (DBU rates) are the speaker's approximate figures; check current Databricks pricing for your cloud and region before quoting them.

## 1. Unity Catalog

Unity Catalog (UC) is the central governance layer for Databricks; expect at least one question on it in every interview.

**Q: What exactly is Unity Catalog?**

A centralized data catalog that provides access control, auditing, lineage, quality monitoring and data discovery across multiple Databricks workspaces.

- **Before UC:** each workspace had its own user management, its own metastore (Hive) and its own compute.
- **With UC:** users are common across an organization's workspaces, so they are managed once, centrally.
- **Metastore:** one central metastore holds catalogs for the different workspaces, so all data is visible and governed in one place.
- **Access:** permissions on metastore objects are controlled in UC itself.
- **Compute:** stays separate per workspace.

**Q: How are tables and databases arranged in Unity Catalog?**

1. **Metastore** – the top-level container. Only **one metastore per cloud region** (e.g. East US, Central India).
2. **Catalog** – many per metastore. Typically one per environment (dev, prod) and/or per business unit.
3. **Schema** – equivalent to a database in SQL Server; many per catalog.
4. **Objects** – the lowest level: tables, views, volumes, functions (and models).

**Q: How does the three-level namespace work?**

Because the metastore is implicit for the region, you never name it in a query. Objects are referenced as:

```sql
SELECT * FROM catalog_name.schema_name.table_name;
```

## 2. Compute (Clusters)

Go beyond "all-purpose and job clusters": listing every classification shows depth.

**Q: What types of clusters can you create in Databricks?**

- **By purpose:** all-purpose, job and SQL warehouse – together called **classic compute**.
- **Serverless:** the newer, Databricks-managed type.
- **By size:** single-node or multi-node (for all-purpose and job clusters).
- **By access mode:** single user or shared.

**Q: What is the difference between all-purpose, job cluster and SQL warehouse?**

| Aspect | All-purpose | Job cluster | SQL warehouse |
| --- | --- | --- | --- |
| Languages | Python, SQL, Scala, R, ML – all of Spark | All of Spark | SQL only |
| Primary purpose | Interactive development and exploration | Automated production jobs | SQL analytics and BI workloads |
| Typical users | Data engineers, data scientists | Jobs, pipelines, schedulers | BI users, analysts, reporting tools |
| Life cycle | Starts on demand; auto-terminates after idle time (e.g. 60 min, configurable) | Starts with the job; terminates as soon as it finishes | Starts on demand; auto-stops when idle |
| Cost | Highest: idle time plus higher rate (\~$0.55/DBU); shared across users | Low: no idle time, lower rate (\~$0.30–0.33/DBU) | Optimized for SQL queries, efficient for SQL |

**Q: What is serverless compute and how does it differ from classic?**

"Classic" here means all-purpose and job clusters (SQL warehouse also has its own serverless version).

| Aspect | Classic compute | Serverless compute |
| --- | --- | --- |
| Where it runs | VMs in the customer's own cloud account (e.g. Azure) | Databricks-managed account, not visible to the customer |
| Infra management | Customer manages networking, quotas, limits | Fully managed by Databricks |
| Customization | Full control: node count, node type, libraries | Little control; Databricks decides nodes internally |
| Operational overhead | Higher: maintain configs, upgrade runtimes | None |
| Cost | Lower | Higher |
| Data privacy / visibility | High: full visibility and control | Lower: you don't see where VMs run |

## 3. Notebooks

**Q: What is a magic command, and which ones exist?**

A keyword placed at the very start of a cell that decides the language or behaviour of that cell.

| Command | What it does |
| --- | --- |
| `%python` | Cell runs as Python |
| `%sql` | Cell runs as SQL |
| `%scala` | Cell runs as Scala |
| `%r` | Cell runs as R |
| `%md` | Markdown for documentation; never executed |
| `%sh` | Runs shell commands/scripts |
| `%fs` | Interacts with the file system (list directories, files) |
| `%pip` | Installs Python packages |
| `%run` | Runs/imports another notebook (see below) |

**Q: How do you run one notebook from another?**

Two options: the `%run` magic command, or `dbutils.notebook.run()`.

**Q: How do you choose between them?**

- **`%run`** – to import reusable functions and variables kept in a separate notebook. After `%run ./utils`, a function such as `area_of_circle()` and a variable defined there are directly usable in the main notebook.
- **`dbutils.notebook.run()`** – to execute another notebook as a separate unit: pass input parameters, get an output back, and handle exceptions.

| Aspect | `%run` | `dbutils.notebook.run()` |
| --- | --- | --- |
| Main use | Import functions and variables | Run a notebook separately and get its output |
| Placement | Must be the first and only line in the cell | Anywhere, programmatically |
| Separate job? | No – runs in the same context | Yes – a separate, trackable run; caller waits for it |
| Input parameters | Supported | Supported |
| Output / return value | Not supported | Supported (via `dbutils.notebook.exit`) |
| Exception handling | Not supported (not needed) | Supported (try/except around the call) |

```python
result = dbutils.notebook.run("./child_notebook", 600, {"run_date": "2026-09-30"})
```

## 4. Secrets, Storage and Volumes

**Q: How do you store credentials securely in Databricks?**

Use a **secret scope**: a named container holding multiple key–value pairs (the secrets). Two ways to back it:

- **Databricks-backed** – secrets stored by Databricks itself.
- **Cloud-backed** (typical in production) – e.g. **Azure Key Vault**-backed: the secrets live in Key Vault and the scope simply maps to it.

**Q: How do you read a secret?**

```python
password = dbutils.secrets.get(scope="my_scope", key="db_password")
```

Two parameters: the scope name and the key (secret name). Values are redacted if printed in notebook output.

**Q: How do you connect to ADLS (Azure) or S3 (AWS)? What is a Databricks volume?**

A **volume** is a Unity Catalog object that maps to a storage location (e.g. an ADLS container). Setup, usually done by the admin team:

1. Create a **storage credential** in Unity Catalog (e.g. backed by a managed identity).
2. Grant that credential permission on the ADLS account.
3. Create an **external location** in UC using the credential.
4. Create a **volume** on top of the external location.

After that, the container is accessed only through the volume path – no need to deal with the storage account again:

```
/Volumes/<catalog>/<schema>/<volume>/path/to/file.csv
```

**Q: How do you list, copy, move or delete files?**

Use `dbutils.fs` on volume paths:

| Function | Purpose |
| --- | --- |
| `ls(path)` | List files in a directory |
| `cp(src, dst)` | Copy a file; `cp(src, dst, recurse=True)` copies a whole directory |
| `mv(src, dst)` | Move a file |
| `rm(path)` | Delete a file (`recurse=True` for directories) |
| `mkdirs(path)` | Create directories |
| `head(path)` | Show the first bytes of a file |
| `put(path, text)` | Write a string (hard-coded or dynamic) directly to a file |

## 5. Databricks Jobs (Lakeflow Jobs)

**Q: How do you orchestrate or schedule notebooks, dashboard refreshes, etc.?**

Create a **job** in Databricks (Jobs & Pipelines). A job chains tasks – notebooks, pipelines, dashboard refreshes – and can be scheduled or triggered.

**Q: How do you pass a parameter from a job to a notebook?**

Define **job parameters** at the job level. They can be edited manually or supplied at run time, and are **automatically pushed down** to every notebook task in the job. The notebook receives them through widgets.

**Q: How do you parameterize a notebook?**

Use **widgets**:

```python
dbutils.widgets.text("run_date", "2026-01-01")   # name, default value – creates the widget
run_date = dbutils.widgets.get("run_date")         # reads the value (from the job or the UI)
```

**Q: How do you return a value from a notebook to the job?**

Use **task values**:

```python
dbutils.jobs.taskValues.set(key="row_count", value=1250)
```

Downstream tasks reference it with a dynamic value: `{{tasks.<task_name>.values.<key>}}`, e.g. `{{tasks.load_sales.values.row_count}}`.

**Q: How do you pass a parameter to the inner task of a For-each loop?**

Give the For-each task an **input array/list** (e.g. `["sales", "orders", "customers"]`). In the inner task, reference the current element as `{{input}}`; each iteration receives one value from the array.

**Q: What mechanisms exist to handle job failures?**

- **Task-level retries** – set per task: number of retries and wait time between them (the video's example showed 3 retries, i.e. 4 attempts).
- **Repair run** – rerun only failed tasks (below).
- **Email / notifications** – on failure and other events.
- **Conditional tasks** – run a custom task based on the outcome of a previous task ("Run if" dependencies such as *all failed* / *at least one failed*).

**Q: How do you rerun only the failed task, not the whole job?**

Open the failed run and click **Repair run**. It reruns from the point of failure only. Even inside a For-each loop, successful iterations are skipped and only the failed ones rerun.

**Q: How do you set up automated emails on failure?**

In the job's **Job notifications** settings, add email addresses (or other destinations) and choose the events: start, success, failure, duration warning, etc.

## 6. Auto Loader

**Q: What is Auto Loader?**

A ready-to-use feature that **incrementally and efficiently processes new files as they arrive** in cloud storage, with no extra setup. You give it a path to monitor; it picks up only new files and tracks what it has already processed, out of the box.

**Q: Which sources are supported?**

- **Azure:** ADLS Gen2 and Blob Storage
- **AWS:** Amazon S3
- **Google Cloud:** Google Cloud Storage

**Q: Which file formats are supported?**

Practically any file. Structured formats with schema tracking: JSON, CSV, XML, Parquet, Avro, ORC, text. Unstructured files (images, PDFs, etc.) can be ingested with the **binaryFile** format.

**Q: How do you use Auto Loader?**

```python
df = (spark.readStream                      # always readStream, even for batch-style runs
      .format("cloudFiles")
      .option("cloudFiles.format", "json")
      .option("cloudFiles.schemaLocation", "/Volumes/cat/sch/vol/_schema")
      .load("/Volumes/cat/sch/vol/landing/"))

(df.writeStream
   .option("checkpointLocation", "/Volumes/cat/sch/vol/_checkpoint")
   .trigger(availableNow=True)
   .toTable("cat.sch.bronze_orders"))
```

**Q: How does Auto Loader know which files are new?**

| Mode | How it works | Efficiency |
| --- | --- | --- |
| **Directory listing** (default, older) | Lists the whole input directory each run and compares against files recorded in the checkpoint | Less efficient as the directory keeps growing |
| **File notification** (recommended) | Cloud storage pushes an event for each new file to an event/queue service; Auto Loader reads only the new files from the queue | Very efficient; needs one-time infra setup/permissions from the cloud admin |

**Q: What trigger types are available?**

- **`availableNow`** – processes all files not yet ingested, then stops. Best for scheduled batch-style runs.
- **`processingTime`** – continuous (near) real-time: the stream never stops and checks for new files at the interval you set (e.g. `"5 minutes"`).
- **`once`** – processes everything once and exits; a one-time load (rarely used now; `availableNow` replaces it).

**Q: What is a checkpoint location?**

A location where Auto Loader writes internal files recording which files and offsets it has processed, so the next run knows exactly what is new. Set with `checkpointLocation` on `writeStream`.

**Q: What is the schema location and why is it important?**

Set with `cloudFiles.schemaLocation`. Auto Loader stores and tracks the inferred schema there, so it can tell whether new files match the schema of data already loaded and flag any mismatch.

**Q: What are the options for handling schema?**

1. **Provide a schema** (`.schema(my_schema)`) – preferred for production, where data must follow a fixed format.
2. **Let it infer** – column names are inferred, but **all columns default to string**. Add `.option("cloudFiles.inferColumnTypes", "true")` to infer real data types too.

**Q: How do you handle corrupted data? What is rescued data?**

With `cloudFiles.schemaEvolutionMode = "rescue"`, Auto Loader adds a **`_rescued_data`** column. Values that are corrupt or don't match the schema go into that column instead of failing the load. (The speaker notes rescue behaviour is on by default; strictly, the default mode is `addNewColumns` and the `_rescued_data` column is added automatically when the schema is inferred.)

## 7. Debugging and AI-Assisted Development

**Q: How do you debug your code in Databricks?**

Add **breakpoints** in notebook cells and use the **Debug cell** option. The debugger offers:

| Control | What it does |
| --- | --- |
| Step in | On a line that calls a function, moves execution inside that function |
| Step out | Inside a function, finishes it and returns to the caller |
| Step over / next line | Executes the current line and moves to the next one |
| Continue | Runs until the next breakpoint |
| Debug console | Run small snippets of code mid-debug |
| Variable watcher | Watch variable values change as the code executes |

**Q: What is the difference between step in and step out?**

Step in enters the function being called on the current line. Step out completes the current function and returns to the line after the call.

**Q: How do you use AI for development inside Databricks?**

Use **Genie Code** (the Databricks AI assistant), opened from its icon in the workspace. Two modes:

- **Chat mode** – ask a question, get an answer.
- **Agent mode** (recommended) – give a requirement; it writes code, edits the notebook, runs the code and fixes errors it hits on its own.

Inside a cell, **Edit code with Genie** writes or modifies code for that cell. Slash commands include `/doc` (add comments), `/explain` (explain code), `/rename` (rename the cell) and `/fix` (fix errors).

## 8. Cost Calculation and Job Optimization

**Q: When optimizing a job, how do you track cost and prove the saving?**

Calculate the exact cost of a job run with SQL over two system tables:

- **`system.billing.usage`** – per job run: start and end time, DBUs consumed, and **SKU name** (the compute type).
- **`system.billing.list_prices`** – the price per DBU for each SKU (e.g. \~$0.30 for jobs compute, \~$0.55 for all-purpose).

Join them on SKU (and the price's effective period) and multiply DBUs by the rate:

```sql
SELECT u.usage_metadata.job_run_id,
       SUM(u.usage_quantity)                          AS dbus,
       SUM(u.usage_quantity * p.pricing.default)      AS cost_usd
FROM system.billing.usage u
JOIN system.billing.list_prices p
  ON u.sku_name = p.sku_name
 AND u.usage_start_time >= p.price_start_time
 AND (p.price_end_time IS NULL OR u.usage_start_time < p.price_end_time)
WHERE u.usage_metadata.job_run_id = :job_run_id
GROUP BY 1;
```

**Before/after method to explain an optimization:**

1. Run the job; record **execution time** and **cost** (query above).
2. Apply the optimization in code or configuration.
3. Run again; record time and cost.
4. Report both improvements – interviewers want **time and cost** savings, not just one.

## 9. Lakeflow Connect and Spark Declarative Pipelines

Under **Jobs & Pipelines → Create** there are three options: **Ingestion pipeline** (= Lakeflow Connect), **ETL pipeline** (= Spark Declarative Pipelines) and **Job** (section 5).

**Q: What is Lakeflow Connect and when should you use it?**

Databricks' managed **data ingestion service**. Connect to a source, select the tables, choose full load, incremental or CDC (change data capture), and ingestion runs automatically – no code to write.

- **Use it when:** you want a managed connector with no custom code.
- **Not a good fit when:** you need a **reusable, metadata-driven framework** for hundreds or thousands of tables. There, a custom framework (JDBC connectors, Auto Loader, or ADF) lets you onboard a new table just by adding a metadata row instead of selecting tables one by one.

**Q: What are Lakeflow Spark Declarative Pipelines (SDP) and when should you use them?**

The new version of **Delta Live Tables (DLT)**. You declare tables, streaming tables and materialized views with your business logic; Databricks handles the dependencies, orchestration, data quality checks (expectations), monitoring, auditing and logging.

- Sits between **managed SQL transformations** and hand-written **Spark Structured Streaming**. Structured Streaming can do the same, but you write and manage everything yourself and don't get the built-in auditing and lineage.
- Deployable through asset bundles in a managed way.

```python
import dlt  # newer releases: from pyspark import pipelines as dp

@dlt.table
def bronze_orders():
    return spark.readStream.format("cloudFiles").option("cloudFiles.format", "json").load("/Volumes/cat/sch/vol/orders")

@dlt.table
def silver_orders():
    return dlt.read_stream("bronze_orders").where("amount > 0")
```

Each table references the one before it; the pipeline works out the order and keeps them up to date.

## 10. Declarative Automation Bundles and CI/CD

**Q: What is a Declarative Automation Bundle (formerly Databricks Asset Bundle)?**

**Declarative Automation Bundles** is the new name for **Databricks Asset Bundles (DABs)**. They bring the software-engineering life cycle to data and AI objects: version your code and objects, and promote them from one environment to another in a managed, automated way.

**Q: What can you put in a bundle?**

- Workspace settings and cluster configurations
- Notebooks and Python files (your code)
- ML models
- Unit and integration tests
- Lakeflow jobs and Spark Declarative Pipelines
- Dashboards

**Q: How does CI/CD work end to end in your project?** *(asked in almost every interview)*

The GitHub repo is connected to the Databricks workspaces; `main` is production and `dev` is branched from it.

1. Create a **feature branch** from `dev` for each new feature.
2. Develop and test on the feature branch.
3. Raise a **pull request** into `dev`; an architect or senior engineer reviews it, then it is merged.
4. Changes on `dev` are deployed to the **dev workspace** with bundles.
5. When development is complete, raise a PR from `dev` to `main`; it is reviewed and merged.
6. The merge into `main` triggers **GitHub Actions**, which deploy the bundle to the **prod workspace** automatically.

&#91;embedded content: CI/CD branch flow · feature → dev → main, plus hotfix\]

The top row is the development loop; only a reviewed merge into main triggers the automated prod deploy. Hotfixes branch from and merge straight back into main.

```bash
databricks bundle validate -t prod
databricks bundle deploy   -t prod
```

**Q: What is a hotfix?**

An urgent production fix that skips the full CI/CD path. Branch a small **hotfix branch directly from `main`**, apply and test the fix, and merge it straight back into `main` so production is fixed quickly. (Also merge it back into `dev` so the fix isn't lost on the next release.)

## 11. Databricks Architecture

**Q: What is the control plane?**

The Databricks application itself – the central unit that drives what users do and what happens in the cloud. It includes:

- The **web application / UI** you log into, where users and applications interact.
- **Compute orchestration** – creating the required nodes, connecting to your cloud, launching and using VMs.
- **Unity Catalog** – access management, lineage, governance.
- Your **notebooks, queries and code** (definitions, not the data being processed).

**Q: With all-purpose or job clusters, how is the cluster set up in your cloud account?**

1. A user creates the compute in the control plane; only the **definition** is stored there.
2. When the cluster starts, the underlying **VMs are created in your cloud account** (Azure, AWS or GCP) – the compute/data plane.
3. Those VMs are **managed by the Databricks control plane**.

**Billing for classic compute is twofold:** the cloud provider charges for the VMs while they run, and Databricks charges **DBUs** for managing the compute.

**Serverless:** the VMs run inside Databricks' own account, so there is no infra for you to manage and a **single bill** from Databricks.

## 12. Data Quality, Delta Sharing, Lakehouse Federation, System Tables

**Q: What ready-to-use data quality features does Databricks offer?**

- **Anomaly detection** – checks two things today:
  - **Freshness:** was the table updated when expected, given its normal schedule?
  - **Completeness:** did the last 24 hours bring the expected number of rows, or was there a sudden rise or drop?
- **Data profiling** – a detailed statistical summary of a table: mean, max, median, distinct values and similar metrics.

Results show on the table itself: **green** = no issues, **orange** = one or more checks failed.

**Q: What is Delta Sharing and when do you use it?**

A way to share Delta tables with users **outside your workspace, and even outside Databricks**. It is built on an **open protocol**, so recipients can read the data from Power BI, Tableau, Spark, pandas, Java, and any cloud. Use it whenever you need to share data with people who are not in your Databricks environment.

**Q: What is Lakehouse Federation?**

Querying an external source **without ingesting its data**. You create a connection and register the source as a **foreign catalog** in Unity Catalog; its tables then appear and are governed like UC tables. Sources include Synapse, Oracle, Teradata, Snowflake and others.

**Q: What are system tables and what do they store?**

Out-of-the-box tables (in the `system` catalog) that give deep visibility into platform operations – governance, lineage, usage, billing. You can build monitoring dashboards on them.

| Area | What it records |
| --- | --- |
| Audit | Every access – who accessed what |
| Lineage | Table and column lineage |
| Billing (`usage`, `list_prices`) | DBU usage and prices – cost calculation (section 8) |
| Compute (clusters, warehouse events) | Cluster and SQL warehouse information and events |
| Data classification | Classification results (e.g. sensitive data detection) |
| Lakeflow | Jobs, tasks and pipeline runs |
| Query history | SQL queries run by users |

## 13. Genie and Databricks One

**Q: What is Genie and what is it used for?**

An AI-powered service for asking **natural-language questions over your tables**. Create one via **New → Genie space**, select the tables or views, and click Create. Genie then understands the question, generates SQL, runs it, and summarizes the result.

**Q: How do you make a Genie space more accurate?**

- **Instructions** – describe what the data holds, which tables are dimensions and which are facts, plus general guidance.
- **Join conditions** – predefine the correct joins so Genie uses them.
- **SQL expressions** – define common measures and dimensions.
- **Example SQL queries** – frequently asked queries Genie can reuse for fast, correct answers.
- **Benchmarks** – define test questions with expected answers and evaluate how well the space performs.

**Q: What is Databricks One?**

A newer feature, often asked to check how up to date a candidate is. It is a **simplified, no-code UI built for business users**. From one place they can open their **dashboards, Genie spaces and Databricks Apps**, and ask questions in natural language to get AI-generated answers, as with Genie.

## Quick Revision Cheat Sheet

| Topic | One-line answer / key syntax |
| --- | --- |
| UC hierarchy | Metastore (1 per region) → Catalog → Schema → Table/View/Volume/Function |
| Namespace | `catalog.schema.table` |
| Cluster types | All-purpose, Job, SQL warehouse (classic) + Serverless; single/multi-node; single-user/shared |
| Cheapest for prod jobs | Job cluster (\~$0.30/DBU, no idle time) |
| Serverless trade-off | No infra work, less control/visibility, higher cost |
| Import helpers | `%run ./notebook` (first and only line in cell) |
| Run with params + output | `dbutils.notebook.run(path, timeout, params)` |
| Read secret | `dbutils.secrets.get(scope, key)` |
| Storage access | Credential → External location → Volume → `/Volumes/cat/sch/vol/` |
| File ops | `dbutils.fs.ls / cp / mv / rm / mkdirs / head / put` |
| Notebook params | `dbutils.widgets.text()` + `dbutils.widgets.get()` |
| Return value to job | `dbutils.jobs.taskValues.set(key, value)` → `{{tasks.<task>.values.<key>}}` |
| For-each element | `{{input}}` |
| Rerun failed tasks | Repair run |
| Auto Loader | `readStream.format("cloudFiles")`; checkpoint + schemaLocation |
| New-file detection | Directory listing (default) vs file notification (recommended) |
| Triggers | `availableNow`, `processingTime`, `once` |
| Infer types | `cloudFiles.inferColumnTypes = true` |
| Bad records | `_rescued_data` column (schemaEvolutionMode `rescue`) |
| Job cost | `system.billing.usage` ⋈ `system.billing.list_prices` |
| Ingestion (no code) | Lakeflow Connect |
| Declarative ETL | Spark Declarative Pipelines (formerly DLT) |
| CI/CD | Feature → dev (PR) → dev workspace; dev → main (PR) → GitHub Actions → prod |
| Share outside Databricks | Delta Sharing (open protocol) |
| Query without ingesting | Lakehouse Federation (foreign catalog) |
| NL questions on data | Genie; business-user UI = Databricks One |
