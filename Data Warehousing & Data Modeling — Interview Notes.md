# Data Warehousing & Data Modeling — Interview Notes

Sep 30, 2026 · @Sunil Patil

## 1. The big picture: OLTP → ETL → OLAP → BI

Data starts with users, lands in operational (OLTP) systems, is moved and reshaped by ETL/ELT, and ends in analytical (OLAP) systems that feed reports and business decisions.

1. **End users generate data.** Buying on Amazon/Flipkart, a bank transfer, a LinkedIn post — every action is a transaction, and every transaction is data.
2. **OLTP (Online Transaction Processing) stores it.** These systems run day-to-day operations: place an order, view order history, update a profile.
   - Relational: MySQL, PostgreSQL, Oracle, SQL Server
   - NoSQL: DynamoDB, MongoDB
3. **ETL moves and transforms it.** Extract from OLTP, Transform into an analytics-friendly shape, Load into OLAP. (ELT is the modern variant — see section 4.)
4. **OLAP (Online Analytical Processing) serves analytics.** Used for business reporting and analysis.
   - Examples: BigQuery, Redshift, Snowflake, Databricks (Delta Lakehouse)
5. **BI tools and business users consume it.** Power BI / Tableau connect to OLAP; business users decide inventory levels, which customers to target for promotions, etc. Business users are themselves a subset of end users — the cycle closes.

### Why not run analytics directly on OLTP? Three reasons

| # | Reason | What it means |
| --- | --- | --- |
| 1 | Different technology | OLTP engines are built so operational transactions are fast; OLAP engines are built so analytical queries are fast. |
| 2 | Different data layout | OLTP data is modelled (normalized) for fast inserts/updates/deletes; OLAP data is modelled (dimensional) for fast aggregations. |
| 3 | Protect the money-making system | Heavy analytical queries eat OLTP bandwidth, slow the website/app for customers, and can even bring it down. |

> Swiggy example: **placing an order** is OLTP; **analyzing monthly revenue by city over the last 5 years** is OLAP.

## 2. Data modeling for OLTP: normalization

**Data modeling** is the process of designing how data is structured and related before it is stored in a database or data warehouse. It answers: what tables do we need, what columns go in each, how are tables connected, and (in a warehouse) what is a fact vs a dimension.

### The problem with one flat table

Imagine storing every order in one wide table: order ID, order date, customer ID, name, gender, age, product ID, name, cost price, category, sales.

| Problem | Example |
| --- | --- |
| **Data redundancy** | Customer C001 places 2 orders → name, gender, age stored twice. A product bought a million times → its details stored a million times. |
| **Inconsistency** | Product cost price changes 100 → 110; some rows show 100, some 110, with no way to tell which was valid when. |
| **Data integrity** | Nothing stops an order pointing to a customer that doesn't exist. |
| **Slow writes** | Every new order must insert all customer + product columns again — painful at thousands of orders per second. |
| **Awkward updates/deletes** | Changing a product's price means updating thousands of rows; you can't "delete a product" without touching orders. |

### The fix: split into entities with keys

1. Move customer columns into a **Customer** table with a unique **customer key**.
2. Move product columns into a **Product** table with a unique **product key**.
3. The **Orders** table keeps only order ID, order date, customer key, product key, sales.
4. Go further: low-variety columns (gender, category) get their own small lookup tables (Gender, Category), storing an integer instead of repeated text — less space, one place to add a new category.

In OLTP these tables are called **entities** (Orders entity, Customer entity) — not facts/dimensions. A real OLTP ERD also has Order Items, Order Status, City → State → Country, Category → Subcategory, etc.

### Keys

- **Primary key (PK):** uniquely identifies each row; cannot repeat and cannot be NULL (e.g. customer key in Customer).
- **Foreign key (FK):** a column whose values must come from another table's primary key (e.g. customer key in Orders → Customer). A FK constraint enforces **data integrity**: inserting customer 6 into Orders fails if customer 6 doesn't exist.

### Normalization and 3NF

- **Normalization** = breaking data into smaller related tables so there is no redundancy and inserts/updates/deletes are fast.
- **Third Normal Form (3NF)** = the form with no redundancy; standard for OLTP. (1NF and 2NF still allow some redundancy.)
- OLTP systems keep **only the latest data**, not history.
- As a data engineer you mostly build/use OLAP, but you must understand OLTP models to extract data correctly.

## 3. Dimensional modeling for OLAP: facts, dimensions, star schema

OLAP systems use **dimensional modeling**: one central fact table holding the measurable events, joined directly to denormalized dimension tables that describe them.

### Why the 3NF model is bad for analytics

Ask for **total sales by country**. In the OLTP model you must join Country → State → City → Customer → Orders — **5 tables**, reading all of every table. That is complex SQL and slow. The design is great for single-row operations, poor for big aggregations.

### The dimensional fix

- Create **one dimension table per business entity** — e.g. `customer_dim` holds all customer attributes (name, gender, age, city, state, country) in one table instead of five.
- Put the events in a **fact table** — e.g. `fact_orders` with order ID, order date, `customer_sk`, `product_sk`, sales.
- Each dimension gets a **surrogate key (SK)**, a warehouse-generated key (not the source ID); the fact table stores these SKs as foreign keys.
- Country-wise sales is now **1 join** (fact → customer\_dim) + aggregate. Category-wise sales is 1 join (fact → product\_dim).

### Why some redundancy is acceptable here

Dimensions are **denormalized** (e.g. city/country repeat across customers). That's fine because warehouse loads are **batch** (e.g. once a day), not thousands of inserts per second. We trade a little storage for **simpler joins and faster aggregations**.

### Star schema

- **Fact table** in the centre: numeric metrics (amount, quantity, profit) + foreign keys to dimensions.
- **Dimension tables** around it: context — *who* bought (customer), *what* (product), *where* (store), *when* (date).
- Each dimension connects **directly** to the fact — the shape looks like a star.
- In Power BI, a data model built by the data engineer typically looks exactly like this.

&#91;embedded content: star schema · 1 fact, 4 dimensions\]

Any "sales by X" question is one join from the fact to the dimension that holds X.

## 4. What is a data warehouse?

A **data warehouse** is a centralized system that stores **integrated, historical** data from **multiple sources**, designed specifically for analytical reporting and business intelligence. It is optimized for OLAP workloads, not transactional processing.

**Interview answer — data warehousing:** the process of collecting, transforming and storing integrated historical data from multiple sources into a centralized system designed for analytical reporting and BI.

**Multiple sources** in practice: orders in MySQL, inventory in SAP/ERP, purchase orders in Oracle, plus flat files, Excel, Drive, cloud storage. ETL gathers and combines all of it.

### Four characteristics (Inmon)

| Characteristic | Meaning | Example |
| --- | --- | --- |
| **Subject-oriented** | Organized around business subjects, not applications | Sales, customer, product areas |
| **Integrated** | Data from CRM, ERP, APIs, files is cleaned and standardized | One source sends "M", another "Male" → warehouse uses one value everywhere |
| **Time-variant** | Keeps history | OLTP overwrites a customer's address; the warehouse shows where they lived in 2022 (via SCD Type 2) |
| **Non-volatile** | Data is inserted and read, rarely updated or deleted | A city change is recorded as a new dated row, not an overwrite |

### Traditional vs modern warehouse

|  | Traditional | Modern cloud |
| --- | --- | --- |
| Infrastructure | On-premises, own physical servers, expensive | Rented cloud, pay per use, scalable storage + compute |
| Approach | ETL | ELT |
| Examples | — | Snowflake, Redshift, BigQuery, Databricks |

### ETL vs ELT

- **ETL (Extract → Transform → Load):** transformation happens *before* loading, in a separate tool (Informatica, DataStage, Python, ADF). The warehouse holds only the final DW layer.
- **ELT (Extract → Load → Transform):** raw data is loaded first, then transformed *inside* the warehouse. The warehouse has a **staging layer** (raw copies of OLTP tables) and a **final DW layer** (transformed models).
- **Why modern systems prefer ELT:** cloud warehouses have powerful compute engines, so a separate transformation tool is no longer needed.

### Where the data lake fits

Modern pipelines don't copy OLTP straight into the warehouse; they land it in a **data lake** first: OLTP → data lake (daily dump) → warehouse (ELT) → BI.

- **What it is:** cheap object storage holding files as-is — e.g. `orders.parquet`, `customers.parquet` (Parquet most common, also CSV). Examples: Amazon S3, Google Cloud Storage, Azure Data Lake Storage (ADLS).
- **Why use it:**
  - **Touch OLTP once.** With 10 ETL jobs reading directly, OLTP is hit 10 times; with a lake it's dumped once at end of day and every consumer (ETL/ELT, data science, ML) reads from the lake.
  - **Security:** grant access to the lake, not to production databases.
  - **Performance:** production databases aren't loaded by analytics.
  - **Backup** of raw data, and **unlimited, pay-for-what-you-store** capacity.

## 5. Key comparisons: OLTP vs OLAP, star vs snowflake

### OLTP vs OLAP

|  | OLTP | OLAP |
| --- | --- | --- |
| Purpose | Run day-to-day operations | Analytics, reporting, BI |
| Data | Current / latest only | Historical |
| Modeling | Highly normalized (3NF), entities | Denormalized, dimensional (facts + dimensions) |
| Workload | Many small inserts/updates/deletes, single-row lookups | Large scans, joins, aggregations; batch loads |
| Storage layout | Row store | Column store |
| Examples | MySQL, PostgreSQL, Oracle, SQL Server, MongoDB, DynamoDB | Snowflake, BigQuery, Redshift, Databricks |
| Example task | Placing an order | Revenue analysis over 5 years |

### Star schema vs snowflake schema

- **Star schema:** one central fact table, multiple **denormalized** dimensions each **directly** connected to the fact. Simple design, fewer joins, better query performance — best for reporting and BI tools.
- **Snowflake schema:** a normalized version of the star, where dimensions are broken into sub-dimensions (e.g. Customer → Location). City-wise sales now needs fact → customer → location. Some dimensions have no direct link to the fact.

|  | Star | Snowflake |
| --- | --- | --- |
| Dimensions | Denormalized | Normalized into sub-dimensions |
| Joins | Fewer | More |
| Queries | Simpler, faster | More complex |
| Storage | More redundancy | Less redundancy, saves storage |
| Use | Preferred by most modern warehouses | Only with a specific reason |

## 6. Fact tables and their types

A **fact table** stores measurable business events: **foreign keys** to dimension tables plus **numeric metrics**. Example `sales_fact`: order ID, customer key, product key, sales amount, quantity.

A **dimension table** stores descriptive attributes of a business entity (customer: name, city, gender; product: name, cost price, category). Dimensions give the fact **context** — without them you can't tell who bought what, so no analysis is possible.

| Type | Grain / what one row is | Example columns | Typical analysis |
| --- | --- | --- | --- |
| **Transaction** | One row per transaction (an order with 2 products = 2 rows) | sales ID, order ID, product, amount | Sales by month, product, customer; trends over time |
| **Periodic snapshot** | State at regular intervals (e.g. daily) | snapshot date, product ID, warehouse, stock qty, inventory value (qty × cost) | Which products hold most stock, and for how long |
| **Accumulating snapshot** | One row per process instance, updated as it moves through stages | order ID, order date, ship date, delivery date, current status | Time between stages; slow products or courier partners |
| **Factless** | Records an event or relationship with **no numeric measure** | date, student ID, class ID (attendance); visit date, customer ID (site visits) | Count events with GROUP BY + COUNT(\*), e.g. students per class per day |

Other factless examples: promotion eligibility, website page visits.

## 7. Slowly Changing Dimensions (SCD)

**SCD** handles changes to dimension data over time. Three types are asked most; **Type 2 is the most used** in real systems because it keeps full history.

| Type | How it handles a change | History kept |
| --- | --- | --- |
| **Type 1** | Overwrite the value in place | None (like OLTP) |
| **Type 2** | Insert a new row with a new surrogate key + effective/end dates; expire the old row | Full |
| **Type 3** | Add a "previous value" column | Current + one previous only |

**Scenario:** customer C001 (Ankit) lives in Mysore on 1 Jan, moves to Bangalore on 10 Jan, then to Mumbai on 20 Jan.

### Type 1 — overwrite

| customer\_id | city | last\_update |
| --- | --- | --- |
| C001 | Mumbai | 20 Jan |

Only the latest city survives. Use when history isn't needed.

### Type 2 — new row per change

| customer\_sk | customer\_id | city | effective\_date | end\_date |
| --- | --- | --- | --- | --- |
| 1 | C001 | Mysore | 1 Jan | 9 Jan |
| 5 | C001 | Bangalore | 10 Jan | 19 Jan |
| 6 | C001 | Mumbai | 20 Jan | 9999-12-31 |

- On each change: **insert** a new row (end date = high date 9999-12-31, meaning active) and **expire** the previous active row (end date = day before the change). Without expiring, two rows would look active on 11 Jan.
- The source **business key (customer\_id) repeats**; the **surrogate key is unique** per version.
- The **fact table stores the surrogate key** active on the order date: order on 5 Jan → SK 1, on 12 Jan → SK 5, on 1 Feb → SK 6. Joining back gives the city at the time of each order.

### Type 3 — previous-value column

| customer\_id | current\_city | previous\_city | last\_update |
| --- | --- | --- | --- |
| C001 | Mumbai | Bangalore | 20 Jan |

On each change, current moves to previous and the new value becomes current. Mysore is lost — which is why Type 2 is more popular.

## 8. Why do we use surrogate keys?

A **surrogate key** is a system-generated unique identifier (usually a running integer) created in the warehouse, separate from the source system's business key. Three reasons:

1. **Required for SCD Type 2.** The business key (C001) repeats across versions. With only C001 in the fact table you'd need a date-range filter to pick the right row; a unique SK makes it a plain join.
2. **Faster joins.** Even for Type 1, source keys may be strings; an integer SK in the fact table joins faster.
3. **Independence from source systems.** Data comes from many systems (SAP, Oracle, online, offline) that may all use "C001" for *different* customers. The warehouse gives each its own SK (SAP C001 → 1, Oracle C001 → 2). Source key formats can also change without breaking the warehouse.

## 9. Special dimensions, load order and data marts

| Concept | Definition | Example |
| --- | --- | --- |
| **Conformed dimension** | One dimension shared across multiple fact tables, ensuring consistent reporting | Date/calendar dimension used by sales, inventory and shipment facts |
| **Degenerate dimension** | A dimension attribute stored in the fact table with no separate dimension table (it has no other attributes) | Order number (e.g. OD0120…), invoice number |
| **Junk dimension** | Combines several low-cardinality, unrelated flags into one table; the fact stores one junk key | is\_gift (Y/N), is\_discount\_applied, payment\_type (card/UPI/debit), order\_source |
| **Role-playing dimension** | Same dimension used several times in one fact table under different roles/aliases | Calendar dim as order date, ship date, delivery date in the accumulating snapshot fact |

### The calendar (date) dimension

One row per day, often \~100–200 years (e.g. 1900–2099), with attributes: date ID, date, weekday, month number/name, quarter, day of month. Facts store the date ID, so month-wise or quarter-wise sales is a join instead of calling MONTH()/QUARTER() each time. It is the classic conformed and role-playing dimension.

### Junk dimension benefits

- Fact table stays clean (no clutter of flag columns).
- Low cardinality = few distinct values (Y/N, 2–3 payment types).
- Reduces repeated columns; attributes need not be related to each other.

### Which loads first: dimension or fact?

**Dimensions first.** Fact rows need the dimension's surrogate key, found by lookup. If new customer C005 isn't in `customer_dim` yet, there's no SK to put in the fact.

### Late-arriving dimension

A fact arrives referencing a dimension member that hasn't arrived yet (e.g. an order for C005, but C005's record was delayed by a source glitch). Ways to handle it:

- **Placeholder (most common):** load the fact with a dummy key (e.g. -1 or -99999), and when the dimension row arrives, look it up and update the fact.
- **Delay the fact load:** park the row in a reject/hold table and load it once the dimension exists.

### Data mart

A **data mart** is a subset of the data warehouse focused on one department or domain (finance, marketing). It keeps only the relevant columns and rows, may add domain-specific transformations, and improves **performance** and **access control**.

## 10. Row store vs column store

OLTP databases are **row stores**; OLAP warehouses are **column stores**. The difference is how data physically sits in the underlying files.

Sample data: (101, Ankit, Mysore, 500), (102, Riya, Delhi, 1000).

- **Row store** keeps each full row together: `101, Ankit, Mysore, 500 | 102, Riya, Delhi, 1000`
- **Column store** keeps each column together: `order_id: 101, 102 | name: Ankit, Riya | city: Mysore, Delhi | amount: 500, 1000`. A row is rebuilt by position (1st value of every column = row 1).

|  | Row store | Column store |
| --- | --- | --- |
| Used by | OLTP databases | OLAP data warehouses |
| Best for | Inserts, updates, deletes, single-record lookups | Aggregations, filters on few columns, large scans, reporting |
| Typical query | "Give me the full row for order 101" (WHERE clause, few rows, all columns) | `SUM(amount)` or city-wise sales (all rows, few columns) |
| Reads | Entire row | Only the needed columns — big I/O saving |

**Interview answer:** a row store is optimized for transactional systems where full rows are accessed frequently; a column store is optimized for analytical systems where large aggregations on selected columns are common.

## 11. Data lake vs data warehouse vs data lakehouse

A lake stores raw files cheaply, a warehouse stores curated tables for fast SQL, and a lakehouse adds warehouse-like reliability (ACID, schema) on top of lake storage via **transaction logs** — no separate warehouse needed.

|  | Data lake | Data warehouse | Data lakehouse |
| --- | --- | --- | --- |
| Stores | Raw data in original format | Structured, cleaned, transformed data | Raw + curated, on lake storage |
| Data types | Structured, semi-structured, unstructured (images, video, Parquet, CSV) | Mostly structured | All types |
| Schema | **Schema-on-read** (define when reading) | **Schema-on-write** (define when writing, after transformation) | Schema enforcement |
| Transformation before storage | Minimal / none | Yes | As needed |
| Record-level insert/update/delete, ACID | No (files can only be read once dumped) | Yes | Yes, via transaction logs |
| Governance | Weak (table/schema/database access hard) | Strong (table, schema, database-level access) | Strong |
| Performance | Not optimized for BI | High-performance SQL analytics | Warehouse-level |
| Cost | Very cheap | Storage more expensive | Cheap lake storage |
| ML / exploration | Good | Less flexible | Supports BI and ML on one platform |
| Examples | Amazon S3, Azure ADLS, Google Cloud Storage | Snowflake, Redshift, BigQuery | Databricks (Snowflake moving this way) |
| Main risk | Can become a **data swamp**; data-quality issues | Limited to structured data | — |

**Closing line for interviews:** a data lake stores raw data; a data warehouse stores curated structured data for analytics; a data lakehouse combines both to provide warehouse-level reliability on top of lake storage.

## 12. Quick-revision cheat sheet

| Question | One-line answer |
| --- | --- |
| OLTP? | Operational system for day-to-day transactions; normalized, current data, row store. |
| OLAP? | Analytical system for reporting/BI; dimensional, historical data, column store. |
| Data modeling? | Designing tables, columns and relationships so data has no redundancy/inconsistency, keeps integrity, and performs well. |
| Normalization / 3NF? | Splitting data into small related tables to remove redundancy; 3NF has none and is used in OLTP. |
| Primary vs foreign key? | PK uniquely identifies a row (unique, not null); FK references another table's PK and enforces integrity. |
| Data warehouse? | Centralized store of integrated historical data from many sources, built for analytics. |
| DW characteristics? | Subject-oriented, integrated, time-variant, non-volatile. |
| ETL vs ELT? | ETL transforms before loading (external tool); ELT loads raw then transforms inside the warehouse (staging → DW). |
| Why a data lake in the pipeline? | Hit OLTP once a day; all consumers read the lake — security, performance, backup. |
| Fact table? | Measurable events: FKs to dimensions + numeric metrics. |
| Dimension table? | Descriptive attributes that give facts context. |
| Fact table types? | Transaction, periodic snapshot, accumulating snapshot, factless. |
| Star vs snowflake? | Star: denormalized dims directly on the fact, fewer joins. Snowflake: dims normalized into sub-dims, more joins. |
| SCD 1 / 2 / 3? | Overwrite / new row with SK + dates / previous-value column. Type 2 most used. |
| Surrogate key? | Warehouse-generated integer key: needed for SCD 2, faster joins, independent of source systems. |
| Load order? | Dimensions first, then facts (facts need dimension SKs). |
| Late-arriving dimension? | Load fact with placeholder key (-1), update when the dimension arrives; or hold in a reject table. |
| Conformed dimension? | Shared by several fact tables (date dim). |
| Degenerate dimension? | Attribute kept in the fact with no dim table (order number). |
| Junk dimension? | Low-cardinality unrelated flags combined into one dim. |
| Role-playing dimension? | One dim used in several roles in the same fact (order/ship/delivery date). |
| Data mart? | Department-specific subset of the warehouse (finance, marketing). |
| Row vs column store? | Row: full rows, good for transactions. Column: columns stored separately, good for aggregations. |
| Lake vs warehouse vs lakehouse? | Raw files / curated tables / warehouse reliability on lake storage. |
