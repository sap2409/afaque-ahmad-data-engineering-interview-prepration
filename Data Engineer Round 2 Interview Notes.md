# Data Engineer Round 2 Interview Notes

Sep 30, 2026 · @Sunil Patil

## Overview

Round 2 is judged less on isolated technical facts and more on how clearly you explain end-to-end work. These notes come from a video by Narendra Kumar, a senior data architect and Databricks Champion with 12 years in IT who has interviewed hundreds of data engineers.

- **Round 1** is usually a technical discussion of the pieces you personally built.
- **Round 2** is usually with a senior person who wants to see your end-to-end visibility of the project, your reasoning, and how well you communicate.
- A common failure is not lack of knowledge but a weak or incomplete explanation.

**Topics covered:** project explanation, metadata-driven framework, data quality, CI/CD and DevOps, data modeling, Databricks-specific questions, PII handling, and AI.

*Note: the source is an auto-generated transcript, so product names have been corrected (e.g. "Medline" → medallion, "SEDD" → SCD, "genius spaces" → Genie spaces).*

## 1. How to explain your project

Start with the business use case, then walk the architecture left to right. This shows the interviewer you know *why* the project exists before *how* it was built.

### Step 1 — The use case (why)

- Who was the project for, and what problem did it solve?
- Example (retail): data was spread across many disconnected sources, so the business had no combined view or analytics.
- What you built: a Databricks platform on a standard medallion architecture; ingested and transformed the data; served it through dashboards and Genie spaces.
- Outcome: a unified view of data and analytics that helped business decision-making.

### Step 2 — Data sources (explain the purpose, not just the names)

Don't just list "Postgres, Salesforce, Blob Storage". Say what each one provided:

| Source | What it provided |
| --- | --- |
| Salesforce | CRM data |
| PostgreSQL | Transactional data (customer purchases) |
| Blob storage | Third-party data |

### Step 3 — Ingestion

- Databricks: Lakeflow Connect or Auto Loader.
- Cloud services: Azure Data Factory or equivalent.

### Step 4 — Transformation (medallion layers, explained properly)

- **Bronze:** raw data, no transformations, so you can see exactly what came from the source.
- **Silver:** harmonized and standardized data. Align differing granularities (e.g. daily vs weekly) and apply data quality checks so only valid data lands. No joins up to silver — a one-to-one mapping with the source, just cleaner.
- **Gold:** joins and business logic are applied between silver and gold, producing curated tables that serve business needs.
- **Platinum / semantic layer (if used):** consistent metrics across business units, with standard definitions of facts and measures.

### Step 5 — Consumption

Say who used the final data and how: dashboards, Genie spaces, Power BI, an ML team, or business users running SQL.

### Step 6 — Orchestration

Databricks Jobs, Azure Data Factory, or whatever ran the pipeline end to end.

### Step 7 — Governance

- Essential for 5–6+ years of experience; good to know at junior level.
- Example: Unity Catalog with schemas organized by business unit or domain, and access set up through role-based access control (RBAC).

### Step 8 — AI usage (mention it proactively)

Don't wait to be asked how you used AI. Mention it while explaining your first project:

- Used Databricks Genie Code to write new code, debug errors, and build pipelines, dashboards, metric views and declarative pipelines — only what you have actually done.
- Mention Genie Code *skills* you added and *instructions* set at user and workspace level by admins, if you know them.
- Raising this unprompted signals you are current and have hands-on AI experience.

## 2. Metadata-driven framework

Describe it as an end-to-end framework with three purposes: **driving**, **tracking** and **auditing**. It can be asked directly, or folded into your project explanation if time permits.

| Purpose | Metadata table(s) | What it stores | Why it matters |
| --- | --- | --- | --- |
| Driving | `tables`, `table_parameters` | Table name, source, destination; plus parameters such as partition column and load type (full or incremental) | Adding a new table = inserting rows into these two tables; the pipeline picks it up automatically |
| Tracking | `watermarks` | Last loaded watermark value per table | The next incremental load starts from where the last one ended |
| Auditing | `pipeline_runs` | Start time, end time, status, record count, error message for every run | Powers monitoring and a consolidated email notification |

**Strong closing point:** at the end of each run, read `pipeline_runs` and send one consolidated email showing which tables succeeded, which failed, and the error messages — users get the full picture in a single notification.

## 3. Data quality

Answer by splitting data quality into two areas: **DQ in motion** and **DQ at rest**. Say you have worked on both.

|  | DQ in motion | DQ at rest |
| --- | --- | --- |
| When | While data moves between layers | After data lands in final tables |
| How | Apply checks bronze → silver and silver → gold; split valid vs invalid; load only valid rows | Read the tables, apply rules, summarize which rules pass or fail |
| Output | Clean downstream layers + a DQ monitoring dashboard over segregated data | A DQ summary report |

### Frameworks

- Options: Great Expectations, Soda, **DQX**.
- DQX is trending because it comes from Databricks and integrates closely with PySpark.

### How DQX is used

1. **Profile** the data.
2. **Auto-generate** data quality rules from the profile.
3. **Apply** the rules and segregate valid from invalid data.
4. **Load** valid data into the next layer.

Two apply methods:

- **Apply checks and split** — returns separate valid and invalid DataFrames, and adds warning/error columns.
- **Apply checks** (no split) — adds the same warning/error columns but keeps a single DataFrame.

## 4. CI/CD and DevOps

Expect one or more questions here to test whether you have really followed a CI/CD process.

### Q: How did you use CI/CD in your project?

1. A **production main** branch exists.
2. A **dev** branch is created from it.
3. For each new feature, a developer creates a **feature branch** from dev.
4. After development, raise a **pull request**.
5. The architect or lead **reviews and approves** the PR.
6. Changes are **merged into dev**. Multiple developers each work on their own feature branches, reviewed and merged separately.

### Q: How did you handle merge conflicts?

*When it happens:* Developer 1 (feature-1) and Developer 2 (feature-2) both change the same file. Feature-1 merges first; feature-2's merge then conflicts.

- **Option 1:** Copy Developer 1's changes into the feature-2 branch so it contains both. Then resolve the conflict by keeping feature-2's version — safe, because it already includes Developer 1's work.
- **Option 2:** Exclude the conflicting file from the merge. Afterwards, create a small new feature branch, reapply the changes, and merge again cleanly.

### Q: What is a hotfix branch?

- Used for a **high-impact production failure** that must be fixed immediately and needs only minor testing.
- Instead of going through dev first, branch **directly from production**, fix, test quickly, and merge into production.
- Sync the fix back into dev later if required.
- Called "hotfix" because it deviates from the normal dev → prod flow.

### Who does the deployment?

- As a data engineer, you own the **development and branching process**.
- Actual deployment to workspaces is usually done by a separate **DevOps team**, building CI/CD pipelines with Azure DevOps pipelines or GitHub Actions, via asset bundles or a plain Git-based process.
- **Databricks Asset Bundles** are increasingly common — know how you added artifacts to a bundle and deployed it.

## 5. Data modeling

The common questions are star vs snowflake schema and SCD types. Always explain with examples.

### Star vs snowflake schema

|  | Star schema | Snowflake schema |
| --- | --- | --- |
| Structure | Central fact table; dimensions attached directly | Central fact table; dimensions further normalized into sub-dimensions |
| Joins | One join per dimension | Multiple chained joins |
| Efficient for | Querying (compute) | Storage (less duplication) |
| Preferred in data lakes? | **Yes** | Only where the data needs sub-dimensions |

**The follow-up that separates candidates:** "Which is more efficient, and which would you choose?" Answer: star, because storage is cheap and compute is expensive, so fewer joins wins in big data systems. Real models may still need a sub-dimension or two.

**Example to use:** a fact table of customer transactions with dimensions for customer, payments and products. In a snowflake version, customer links on to country → state → city, and product links on to category → subcategory.

### Slowly Changing Dimensions (SCD)

| Type | Behavior | Example |
| --- | --- | --- |
| SCD 0 | Loaded once, never updated; rare changes handled manually | Country codes |
| SCD 1 | Overwrite with latest value; no history | Customer address used only for sending letters |
| SCD 2 | Full history: new row per change with start date, end date and an is-active flag | Auditing company needing full address history |
| SCD 3 | Extra column holds only the previous value for a specific column | `previous_address` + `current_address` |

**Rule of thumb:** need history → SCD 2 (tracks all columns with one design, no extra columns). Don't need history → SCD 1. SCD 3 is rare.

## 6. Databricks-specific questions

### Q: How do you calculate the exact cost of a job run? (or: how did you measure an optimization's savings?)

1. Query the **usage system table** — it logs DBUs consumed per job run ID, per hour (a run from 4–6 PM has entries for 4–5 and 5–6).
2. Join it to the **prices system table**, which gives the price per DBU for each compute type.
3. Multiply DBUs × price and sum → the dollar cost of the run.
4. To show savings, calculate cost **before** optimization, apply the change (liquid clustering, partitioning, salting for skew, etc.), then calculate cost for the new run.

*Example answer:* "Before optimization the job cost $20; after, $9 — an $11 saving per run."

### Q: How do you call one notebook from another and pass parameters?

Most candidates give only half the answer. Cover both methods and when to use each:

|  | `%run` | `dbutils.notebook.run()` |
| --- | --- | --- |
| What it is | Magic command | Real call to run another notebook |
| Use it for | Importing reusable functions stored in a separate notebook | Running another notebook as a step in a flow |
| Execution | Code is loaded into the current notebook | Child runs as a separate job; control returns to the caller when it finishes |
| Parameters | — | Pass input as key-value pairs; child reads them with **widgets** |
| Return value | — | Child returns JSON via `dbutils.notebook.exit()`; caller stores it in a variable |
| Error handling | — | Can wrap the call in exception handling |

### Q: What best practices do you follow with Genie Code?

- Give **exact table names** so it doesn't have to search.
- Use **@ context** to point it at specific tables.
- Be **specific** — not "do some analytics on this table" but "using these tables, analyze customers by region / by consumption behavior".
- Add output instructions, e.g. split output across multiple cells and avoid excessive print statements.
- Mention **workspace- and user-level instructions** set by your architect/admin team to enforce naming conventions and coding standards.
- Not every project uses it yet, but expect it to become mandatory soon.

## 7. PII data handling

Optional for 2–4 years of experience; expected from about 4–5 years up. Not usually the deciding factor, but a strong value-add if you're average elsewhere.

Frame it as a **decision ladder**: go down only as far as the use case needs. You may combine techniques.

| # | Question to ask | Technique | Example |
| --- | --- | --- | --- |
| 1 | Do we need the PII columns at all? | **Minimization** — don't ingest them | Uncheck columns in Lakeflow Connect or exclude them in ADF queries |
| 2 | Is aggregated data enough? | **Generalization** | Store age groups instead of date of birth |
| 3 | Is an irreversible identifier enough? | **Hashing** | Count distinct customers/patients without real IDs |
| 4 | Can PII live apart from the main data? | **Tokenization** | PII columns in a separate table, other columns in the main table |
| 5 | Must it be stored but protected? | **Encryption** | Encrypt; decrypt only with controlled keys |
| 6 | Store as-is, restrict on read? | **Role-based access control** | Restrict retrieval by role |

**Short version for a time-limited interview:** "We use minimization, generalization, hashing, tokenization, encryption and role-based access control depending on the need." Naming them shows awareness.

## 8. AI questions

Split your answer into two areas: **AI for productivity** and **an AI use case you built**.

### Area 1 — AI for productivity

- **Genie Code:** create notebooks, pipelines and dashboards; fix errors; generate and run test cases from notebook code.
- **Other assistants** (ChatGPT, Claude, etc.): research, documentation, improving code. Before Genie Code, also used for code generation.

### Area 2 — An AI application you built

- Optional for now at 3–4 years; treated as **mandatory above 4–5 years** — at least a basic RAG-based app.
- It doesn't have to be a production project: a POC for a client or for your own learning is fine to mention.

**Sample use case — chatbot over structured + unstructured data in Databricks:**

1. **Structured knowledge base:** load CSV files into Delta tables; expose them through **SQL functions** as agent tools.
2. **Unstructured knowledge base:** parse PDFs → split text into **chunks** → create **embeddings** (numerical vectors produced by an ML model, not ASCII conversion, used to find relevant text) → store in a **Vector Search index** → expose the **vector search endpoint** as an agent tool.
3. **Agent:** a Databricks agent given both tools, so it can query structured and unstructured data.
4. **Front end:** a **Databricks App** (web app) with the out-of-the-box chatbot UI connected to the agent.

### Explaining RAG (Retrieval-Augmented Generation)

Example question: "What is the price of this product?"

- **Retrieval** — the question is converted to an embedding and used to fetch only the relevant text from the vector index.
- **Augmentation** — the user prompt is combined with the retrieved supporting data into a larger prompt.
- **Generation** — the LLM generates the answer from that augmented prompt.

In the chatbot above, this RAG flow runs automatically end to end behind the scenes.

## Quick revision checklist

- [ ] Can explain my project: use case first, then sources → ingestion → bronze/silver/gold → consumption → orchestration → governance → AI
- [ ] Can say what each data source provided, not just its name
- [ ] Can explain what happens in each medallion layer (no joins before silver)
- [ ] Can describe the metadata framework: driving, tracking (watermarks), auditing (pipeline runs + email)
- [ ] Can explain DQ in motion vs DQ at rest and the DQX workflow
- [ ] Can walk through branching, PRs, merge-conflict resolution and hotfixes
- [ ] Know what Databricks Asset Bundles are and how they're deployed
- [ ] Can justify star over snowflake (compute costs more than storage)
- [ ] Can explain SCD 0–3 with an example each
- [ ] Can calculate job cost from the usage + prices system tables
- [ ] Can compare `%run` vs `dbutils.notebook.run()` including parameters and return values
- [ ] Can list Genie Code best practices and mention AI use proactively
- [ ] Can walk the PII ladder: minimization → generalization → hashing → tokenization → encryption → RBAC
- [ ] Can explain a RAG chatbot end to end and define retrieval, augmentation, generation
