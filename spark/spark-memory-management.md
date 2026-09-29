# Apache Spark Memory Management: Study Notes

Source: "Apache Spark Memory Management" (YouTube, https://www.youtube.com/watch?v=sXL1qgrPysg). Companion to [reading-spark-query-plans.md](reading-spark-query-plans.md) and [reading-spark-dags.md](reading-spark-dags.md). These notes follow the video's order and clean up transcription errors. Where the video simplifies a formula, the exact Spark behavior is added as a note.

## Contents

1. Why memory management matters
2. Executor memory layout
3. On-heap memory: the four regions
4. Worked example: 10 GB executor
5. Overhead and off-heap memory, and what Spark asks YARN for
6. Unified memory and dynamic allocation
7. Slider rules: who can borrow and who gets evicted
8. Garbage collection and why off-heap exists
9. Off-heap memory: pros, cons, config
10. Cheat sheet: configs and formulas
11. Interview questions
12. Key takeaways

---

## 1. Why memory management matters

Understanding memory helps with two things:

- **How Spark works** internally (where joins, shuffles, caching actually live).
- **Which part of memory stores what**, so you can diagnose OOMs, spills, GC pauses and cache evictions.

Most Spark tuning problems (OOM errors, slow stages, excessive spill, long GC time) trace back to one of the regions below running out of space.

## 2. Executor memory layout

A Spark executor container has **three** major memory components:

```
┌──────────────────────── Executor container ────────────────────────┐
│                                                                     │
│  ┌──────────────── On-heap (spark.executor.memory) ─────────────┐   │
│  │  Execution  │  Storage  │  User memory  │  Reserved (300 MB) │   │
│  │ └──── Unified memory ───┘                                   │   │
│  └──────────────────────────────────────────────────────────────┘   │
│                                                                     │
│  ┌─── Off-heap (spark.memory.offHeap.size) ───┐  ┌── Overhead ──┐   │
│  │   Execution   │   Storage                   │  │ system-level │   │
│  └─────────────────────────────────────────────┘  └──────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
```

| Component | Managed by | Default | Used for |
|---|---|---|---|
| **On-heap** | JVM | `spark.executor.memory` | Nearly all Spark work: joins, shuffles, sorts, aggregations, caching, user objects |
| **Off-heap** | OS (outside JVM heap) | Disabled (`0`) | Optional extra execution + storage space that avoids GC |
| **Overhead** | OS / container | `max(384 MB, 10% of executor memory)` | Internal system-level operations (JVM overheads, native buffers, thread stacks, etc.) |

### Why is on-heap managed by the JVM?

- **JVM = Java Virtual Machine**, a virtual computer that runs Java bytecode.
- Spark is written in **Scala**, which runs on the JVM. The JVM is the execution environment for Java *and* other JVM languages like Scala.
- **PySpark** is a wrapper around Spark's Java/Scala APIs. You write Python, but the underlying execution still happens **on the JVM**.
- So Spark's main working memory is the JVM heap, hence "on-heap".

> Note: Python worker processes (for Python UDFs, `mapPartitions`, pandas UDFs) run *outside* the JVM. Their memory comes from the overhead region, or from `spark.executor.pyspark.memory` if set.

## 3. On-heap memory: the four regions

| Region | What lives here |
|---|---|
| **Execution memory** | Joins, shuffles, sorts, aggregations (intermediate buffers/hash tables) |
| **Storage memory** | Cached RDDs / DataFrames (`cache()`, `persist()`), broadcast variables |
| **User memory** | User-defined objects: variables, collections (lists, sets, dicts) defined in your program, UDFs, Spark internal metadata for your code |
| **Reserved memory** | Fixed **300 MB** Spark needs to run itself and store internal objects |

**Execution + Storage together = Unified memory.** (Why "unified" is explained in section 6.)

## 4. Worked example: 10 GB executor

Assume `spark.executor.memory = 10g`, defaults everywhere else, and (for simple arithmetic) **1 GB = 1000 MB**.

| Parameter | Default | Meaning |
|---|---|---|
| `spark.memory.fraction` | `0.6` | Share of heap given to unified memory (execution + storage) |
| `spark.memory.storageFraction` | `0.5` | Share of unified memory that is storage's *protected* starting size |

### The video's calculation

| Region | Calculation | Size |
|---|---|---|
| Unified (execution + storage) | 0.6 × 10 GB | **6 GB** |
| Storage | 0.5 × 6 GB | **3 GB** |
| Execution | 6 GB − 3 GB | **3 GB** |
| Remaining (user + reserved) | 10 GB − 6 GB | **4 GB** |
| Reserved | fixed | **300 MB** |
| User | 4 GB − 300 MB | **3.7 GB** |

```
10 GB on-heap
├── Unified 6 GB
│   ├── Execution 3 GB
│   └── Storage   3 GB
└── 4 GB
    ├── User      3.7 GB
    └── Reserved  0.3 GB
```

### Exact formula (what Spark actually does)

The video applies the fractions to the full 10 GB for simplicity. Spark first subtracts the reserved 300 MB, then applies the fractions to what's left:

```
usable     = executor_memory − 300 MB
unified    = usable × spark.memory.fraction
storage    = unified × spark.memory.storageFraction
execution  = unified − storage
user       = usable × (1 − spark.memory.fraction)
```

With 10 GB = 10,240 MB:

| Region | Exact size |
|---|---|
| Usable | 10,240 − 300 = 9,940 MB |
| Unified | 9,940 × 0.6 ≈ **5,964 MB** |
| Storage / Execution | ≈ **2,982 MB** each |
| User | 9,940 × 0.4 ≈ **3,976 MB** |
| Reserved | **300 MB** |

The two methods differ by a few hundred MB; the exact version is the one to quote in interviews.

## 5. Overhead and off-heap memory, and what Spark asks YARN for

Key point: **`spark.executor.memory` only sizes the on-heap region.** None of it goes to overhead or off-heap. Those are sized separately.

### Overhead

```
spark.executor.memoryOverhead = max(384 MB, 0.10 × spark.executor.memory)
```

For 10 GB: `max(384 MB, 1 GB)` = **1 GB**.

### Off-heap

- **Disabled by default** (`spark.memory.offHeap.size = 0`).
- When enabled, it has the same two-part structure as unified memory: **execution** and **storage**.
- Suggested starting point from the video: **10 to 20% of executor memory** (1 to 2 GB for a 10 GB executor), then experiment.

### Total container request to the cluster manager

When Spark requests a container from the cluster manager (e.g. YARN), it asks for:

```
container = spark.executor.memory
          + spark.executor.memoryOverhead
          + spark.memory.offHeap.size   (only if off-heap is enabled)
```

Example (off-heap disabled): 10 GB + 1 GB = **11 GB** per executor container.

Example (off-heap enabled at 2 GB): 10 + 1 + 2 = **13 GB**.

> Tip: if YARN kills containers with "exceeding memory limits", the overhead (not the heap) is often what ran out. Increase `spark.executor.memoryOverhead`.

## 6. Unified memory and dynamic allocation

Execution + storage is called **unified** because of Spark's **dynamic memory management**: the boundary between the two is a **movable slider**.

- If execution needs more memory, it can use storage's memory.
- If storage needs more memory, it can use execution's memory.
- **Execution has priority**, because critical operations (joins, shuffles, sorts, group-bys) happen there.

### Before vs after Spark 1.6

| | Before Spark 1.6 (static) | Spark 1.6+ (unified) |
|---|---|---|
| Boundary | **Fixed** | **Movable** slider |
| Execution full, storage empty | Execution **cannot** use free storage space; it spills or fails | Execution borrows the free storage space |
| Result | Wasted memory | Better utilization |

The pre-1.6 model was called the **static memory manager** (`spark.memory.useLegacyMode`, removed in Spark 3.0).

## 7. Slider rules: who can borrow and who gets evicted

### Rule 1: Execution needs more, storage has free space

```
Before: [ Execution ████████ | Storage ███·········· ]
After:  [ Execution ████████████████ | Storage ███·· ]
```

Execution simply takes the **unused** storage memory.

### Rule 2: Execution needs more, storage is occupied

Storage **evicts** some of its cached blocks using **LRU (Least Recently Used)** to make room, and execution takes the freed space.

- Evicted cached data is dropped (or, with a `MEMORY_AND_DISK` storage level, kept on disk) and recomputed/re-read if needed later.
- Storage can only be pushed down to its protected size (`storageFraction` of unified memory); blocks below that line are not evicted for execution.

### Rule 3: Storage needs more, execution is occupied

- **Execution blocks are never evicted** to make room for storage.
- Storage must evict **its own** blocks (LRU) to fit the new cached data.
- Storage can only borrow execution memory that is *free*.

### Summary table

| Who needs memory | Other side's space | What happens |
|---|---|---|
| Execution | Free | Execution borrows it |
| Execution | Used by storage | Storage evicts cached blocks (LRU) down to its protected size; execution takes the space |
| Storage | Free | Storage borrows it |
| Storage | Used by execution | Execution is **not** evicted; storage evicts its own LRU blocks |

### Why execution gets priority, and a caching lesson

- Execution failures stall the job (spill to disk or OOM). A lost cache block can just be recomputed.
- Common anti-pattern: **caching DataFrames that are never reused.** It wastes storage memory and gains nothing.
- Only cache a DataFrame when it is reused multiple times (e.g. in several actions or branches), and `unpersist()` it when done.

## 8. Garbage collection and why off-heap exists

- Most Spark operations run in on-heap memory, which the JVM manages.
- When the heap fills up, the JVM runs a **garbage collection (GC) cycle**:
  - It **pauses** the program (stop-the-world, for many collectors).
  - Cleans up unwanted/unreferenced objects.
  - Resumes the program.
- Frequent or long GC pauses hurt performance. Check **GC Time** in the Spark UI Executors tab; if it's a large share of task time, memory pressure is a likely cause.

This is where **off-heap** memory helps.

## 9. Off-heap memory: pros, cons, config

| Aspect | Off-heap |
|---|---|
| Managed by | The **operating system**, outside the JVM heap |
| GC | **Not subject to** JVM garbage collection pauses |
| Memory allocation/deallocation | Must be handled explicitly (not by the GC); mistakes cause memory leaks |
| Speed | **Slower than on-heap** (data must be serialized; on-heap is "closest" to Spark) |
| vs disk | **Much faster than spilling to disk**, which is orders of magnitude slower |
| Default | Disabled |

- The video says the *developer* is responsible for allocation and deallocation. In practice, when you use Spark's built-in off-heap mode, Spark's Tungsten engine does this for you; the added responsibility is mainly in sizing it correctly and in any custom code that allocates native memory. Either way, it adds complexity and should be used **with caution**.
- If Spark had to choose between **spilling to disk** and **using off-heap**, off-heap is the better option.

### How to enable

```python
spark = (
    SparkSession.builder
    .appName("offheap-demo")
    .config("spark.memory.offHeap.enabled", "true")
    .config("spark.memory.offHeap.size", "2g")   # ~10-20% of executor memory
    .getOrCreate()
)
```

```bash
spark-submit \
  --conf spark.executor.memory=10g \
  --conf spark.memory.offHeap.enabled=true \
  --conf spark.memory.offHeap.size=2g \
  app.py
```

Both settings are required: `enabled=true` **and** a non-zero `size`.

### When to consider off-heap

- High GC time in the Executors tab.
- Large caches or large shuffle/aggregation buffers causing long GC pauses.
- Workloads that would otherwise spill heavily to disk.

## 10. Cheat sheet: configs and formulas

| Config | Default | Controls |
|---|---|---|
| `spark.executor.memory` | `1g` | On-heap size |
| `spark.memory.fraction` | `0.6` | Unified share of (heap − 300 MB) |
| `spark.memory.storageFraction` | `0.5` | Storage's protected share of unified |
| `spark.executor.memoryOverhead` | `max(384 MB, 10%)` | Overhead size |
| `spark.memory.offHeap.enabled` | `false` | Turn off-heap on |
| `spark.memory.offHeap.size` | `0` | Off-heap size |
| Reserved memory | `300 MB` (hard-coded) | Spark internals |

```
Reserved   = 300 MB
Usable     = executor.memory − 300 MB
Unified    = Usable × memory.fraction
Storage    = Unified × storageFraction       (soft boundary)
Execution  = Unified − Storage               (soft boundary)
User       = Usable × (1 − memory.fraction)
Overhead   = max(384 MB, 0.10 × executor.memory)
Container  = executor.memory + Overhead + offHeap.size (if enabled)
```

## 11. Interview questions

1. **What are the main memory regions in a Spark executor?** On-heap (execution, storage, user, reserved), off-heap (execution, storage), and overhead.
2. **Where do joins and shuffles run vs where does caching live?** Execution memory vs storage memory.
3. **Why is it called unified memory?** Execution and storage share one pool with a movable boundary (since Spark 1.6).
4. **Which side has priority?** Execution. Storage evicts its own blocks (LRU); execution blocks are never evicted for storage.
5. **Executor memory is 10 GB. How much does Spark request from YARN?** 10 GB + max(384 MB, 1 GB) = 11 GB, plus off-heap if enabled.
6. **What is reserved memory?** A fixed 300 MB for Spark's own internal objects.
7. **Why use off-heap memory?** To avoid GC pauses and to avoid spilling to disk. Trade-offs: slower than on-heap, more complex, disabled by default.
8. **Container killed by YARN for exceeding memory limits, but no heap OOM. What do you tune?** `spark.executor.memoryOverhead`.
9. **Where does user-defined data (lists, dicts, UDF objects) live?** User memory.

## 12. Key takeaways

- `spark.executor.memory` sizes **only** the on-heap region; overhead and off-heap are added on top.
- On-heap = **Execution + Storage (unified, 60%) + User (40%) + Reserved (300 MB)**.
- Unified memory has a **movable slider**; execution wins conflicts, storage evicts via **LRU**.
- Don't cache what you don't reuse; it competes with execution memory.
- High GC time → consider **off-heap** (start at 10 to 20% of executor memory), but it's slower than on-heap and adds complexity.
- Off-heap beats spilling to disk.
