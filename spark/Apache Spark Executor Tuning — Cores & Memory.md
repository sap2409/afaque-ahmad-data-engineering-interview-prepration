# Apache Spark Executor Tuning — Cores & Memory

Sep 29, 2026 · @Sunil Patil

## Overview

Well-written Spark code can still run slowly if CPU and memory are allocated badly. Executor tuning means choosing three values at `spark-submit` time: the number of executors, the cores per executor, and the memory per executor.

**How executors sit inside a node**

- A *node* is one machine in the cluster; a cluster has many nodes.
- Example node: 17 cores, 20 GB RAM.
- Requesting 3 executors × (5 cores, 6 GB) uses 15 cores and 18 GB of that node.
- Each executor is a separate JVM holding its own share of cores and memory.

Picking numbers by guesswork is not enough. There are three sizing strategies — **fat**, **thin**, and **optimally sized** — and a set of rules that lead to the optimal one.

**Setup used for the fat/thin comparison and Example 1:** 5 nodes, each with 12 cores and 48 GB RAM.

## Fat executors

Fat executors take a large share of each node's resources — typically one executor per node.

**Calculation (5 nodes × 12 cores × 48 GB)**

1. Reserve 1 core + 1 GB per node for the OS, Hadoop and YARN daemons → 11 cores, 47 GB usable per node.
2. Give one executor all of it → 1 executor per node with 11 cores and 47 GB.
3. 5 nodes → 5 executors.

```
--num-executors 5
--executor-cores 11
--executor-memory 47G
```

Result: five very powerful executors, one per node.

## Thin executors

Thin executors are the opposite: each one takes minimal resources, typically 1 core.

**Calculation (same cluster)**

1. Reserve 1 core + 1 GB per node → 11 cores, 47 GB usable per node.
2. 1 core per executor → 11 executors per node.
3. Memory per executor = 47 GB ÷ 11 ≈ 4 GB.
4. 11 executors × 5 nodes = 55 executors.

```
--num-executors 55
--executor-cores 1
--executor-memory 4G
```

Result: many small executors, each able to run only one task at a time.

## Fat vs thin: pros and cons

Fat executors are good for heavy per-task work and data locality but suffer from GC pauses, poor fault tolerance and idle waste. Thin executors are fault tolerant but lose data locality and add network traffic.

| Aspect | Fat executors | Thin executors |
| --- | --- | --- |
| Parallelism | High *task-level* parallelism: many cores in one executor run many tasks at once | High *executor-level* parallelism: many executors, but each must do lightweight work |
| Memory-heavy tasks | Handles tasks that need a lot of memory | Poor — each executor has little memory |
| Management overhead | Few executors to manage | Many executors to manage |
| Data locality | Enhanced — large memory holds many partitions locally, less shuffling | Reduced — small memory holds few partitions, so needed partitions are often elsewhere |
| Network traffic | Lower | Higher — data must be fetched from other nodes |
| Fault tolerance | Poor — losing one executor (e.g. processing 32 GB) means large recomputation | Good — losing a small executor means little recomputation |
| Resource waste | Risk of paying for idle cores/memory if not fully used | Low |
| HDFS throughput / GC | More than 5 cores per executor degrades HDFS throughput and causes heavy garbage collection | Not an issue |

**Why GC matters:** Garbage collection is the JVM cleaning unused objects from memory. It pauses the program while it runs, so frequent GC cycles in huge executors hurt performance. This is why 3–5 cores per executor is the recommended range.

## Four rules for optimal sizing

Apply these in order; rule 1 is per node, rule 2 is per cluster.

1. **Reserve 1 core + 1 GB per node** for the OS, Hadoop and YARN daemons. *(Node level.)*
2. **Reserve resources for the YARN Application Master.** The AM negotiates resources with the Resource Manager to launch executors. *(Cluster level.)* Two options:
   - Subtract 1 core + 1 GB from the cluster total — the AM runs fine with this. Best when executors are large.
   - Or subtract one whole executor from `--num-executors` — simpler, but wasteful with fat executors (you wouldn't hand an 11-core, 47 GB executor to the AM).
3. **Use 3–5 cores per executor** (1 core = 1 task). More than 5 hurts HDFS throughput and causes long GC pauses.
4. **Exclude memory overhead from executor memory.** Overhead is used for internal system processes:

```latex
\text{overhead} = \max(384\,\text{MB},\; 0.10 \times \text{executor memory})
```

```latex
\text{executor-memory} = \text{memory per executor} - \text{overhead}
```

## Worked example 1: 5 nodes × 12 cores × 48 GB

Result: **10 executors × 5 cores × 20 GB**.

| Step | Rule | Cores | Memory |
| --- | --- | --- | --- |
| Raw per node | — | 12 | 48 GB |
| Reserve for OS/daemons (per node) | 1 | 11 | 47 GB |
| Cluster total (× 5 nodes) | — | 55 | 235 GB |
| Reserve for Application Master (cluster) | 2 | 54 | 234 GB |
| Cores per executor | 3 | 5 | — |
| Executors = 54 ÷ 5 ≈ 10.8 → 10 | — | — | — |
| Memory per executor = 234 ÷ 10 = 23.4 → \~23 GB | — | — | 23 GB |
| Overhead = max(384 MB, 10% × 23 GB) = 2.3 GB | 4 | — | 23 − 2.3 = 20.7 → 20 GB |

```
--num-executors 10
--executor-cores 5
--executor-memory 20G
```

**Why this is balanced**

- 5 cores and 20 GB is neither too small (thin) nor too large (fat), so parallelism is good.
- 5 cores stays within the HDFS throughput / GC limit.
- 20 GB holds a good number of partitions locally, preserving data locality.

## Why data size isn't in the formula

What matters is **memory per core** compared with **partition size**, not the total dataset size (10 GB vs 100 GB).

- Memory per core = executor memory ÷ executor cores = 20 GB ÷ 5 = **\~4 GB**.
- One core processes one partition (one task) at a time.
- So any partition ≤ \~4 GB processes without trouble.
- With the typical 128 MB partitions, this configuration has plenty of headroom.

**How to use this:** find your partition size, then check that the memory each core gets can comfortably hold one partition.

## Worked example 2: 3 nodes × 16 cores × 48 GB

Result: **11 executors × 4 cores × 11 GB**.

| Step | Rule | Cores | Memory |
| --- | --- | --- | --- |
| Raw per node | — | 16 | 48 GB |
| Reserve for OS/daemons (per node) | 1 | 15 | 47 GB |
| Cluster total (× 3 nodes) | — | 45 | 141 GB |
| Reserve for Application Master (cluster) | 2 | 44 | 140 GB |
| Cores per executor = 4 (not 5, see note) | 3 | 4 | — |
| Executors = 44 ÷ 4 = 11 | — | — | — |
| Memory per executor = 140 ÷ 11 ≈ 12.7 → \~12 GB | — | — | 12 GB |
| Overhead = max(384 MB, 10% × 12 GB) = 1.2 GB → \~1 GB | 4 | — | 12 − 1 = 11 GB |

**Why 4 cores, not 5:** 44 ÷ 5 = 8.8, so 5 cores leaves 4 cores unused. 4 cores divides 44 exactly, using every core while staying within the 3–5 range.

```
--num-executors 11
--executor-cores 4
--executor-memory 11G
```

Memory per core here ≈ 11 GB ÷ 4 ≈ 2.75 GB, still far above a 128 MB partition.

## Cheat sheet

| Strategy | Example 1 config (5 × 12 cores × 48 GB) | Best for | Main risk |
| --- | --- | --- | --- |
| Fat | 5 executors × 11 cores × 47 GB | Memory-heavy tasks, data locality | GC pauses, poor fault tolerance, idle waste |
| Thin | 55 executors × 1 core × 4 GB | Lightweight tasks, fault tolerance | Network traffic, poor data locality |
| Optimal | 10 executors × 5 cores × 20 GB | General workloads | — |

**Sizing recipe**

1. Per node: subtract 1 core + 1 GB.
2. Multiply by the node count for cluster totals.
3. Cluster: subtract 1 core + 1 GB (or one executor) for the Application Master.
4. Pick 3–5 cores per executor, preferring a value that divides total cores evenly.
5. Executors = total cores ÷ cores per executor.
6. Memory per executor = total memory ÷ executors.
7. Subtract overhead = max(384 MB, 10%) to get `--executor-memory`.
8. Sanity check: memory per core ≥ partition size (e.g. 128 MB), with headroom.

**Note beyond the video:** in Spark on YARN, overhead (`spark.executor.memoryOverhead`) is added *on top of* `--executor-memory` when requesting the container. That is why step 7 subtracts it — so executor memory plus overhead still fits in the space available.
