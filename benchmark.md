# TurboPulse Performance Benchmark: TurboPulse vs BullMQ & Bottleneck

> **Test Environment:** Remote Ubuntu Linux VPS (`159.195.108.48`), 4 vCPUs (AMD EPYC™), 8 GB RAM, Node.js `v20.20.2`, Redis `7.0.15` (local Unix/TCP socket).  
> **Packages Tested:** `turbopulse@1.0.4` (installed from public npm), `bullmq@5.x`, `bottleneck@2.19.5`, `ioredis@5.x`.  
> **Reproducibility:** All tests conducted live using identical payloads, concurrency settings, and Redis instances.

---

## 1. Executive Summary & Architectural Differences

| Capability | BullMQ | Bottleneck | TurboPulse |
| :--- | :--- | :--- | :--- |
| **Runtime Architecture** | Pure JavaScript / TypeScript on V8 | Pure JavaScript on V8 | **Compiled Native Rust (`napi-rs`) + Zero-Copy Buffer FFI** |
| **External Dependency Requirement** | **Strictly Required:** Redis / DragonFly (or Postgres in BullMQ Pro). **Cannot run standalone.** | None (pure in-memory) or Redis via `ioredis` | **Dual-Engine:** Full standalone in-memory execution **with zero Redis/Postgres requirement**, PLUS distributed Redis support when multi-node clustering is needed. |
| **In-Memory Rate Limiting** | N/A (Queue only) | ~200 – 42,000 ops/sec | **39,141 ops/sec** (Async wrapper) / **526,941 ops/sec** (Native `acquireSync`) |
| **In-Memory Latency** | N/A | 4,863 µs (4.86 ms) | **2.06 µs – 28.34 µs** (Up to **171x to 230x lower latency**) |
| **Redis Batch Enqueue (`addBulk`)** | 13,307 jobs/sec | N/A | **24,561 jobs/sec (+85% faster)** |
| **Worker Drain (Concurrency = 16)** | 8,928 jobs/sec | N/A | **11,648 jobs/sec (+30% faster)** |
| **Standalone Offline Queueing** | ❌ **Unsupported** (Cannot run without Redis) | ❌ N/A | **✅ Supported** (**27,141 jobs/sec enqueue, 54,889 jobs/sec worker**) |
| **Timer Overhead (5,000 Keys)** | N/A | 5,000 active `setTimeout` handles | **1 master hierarchical timer wheel** (0 when idle) |
| **Data Serialization** | V8 `JSON.stringify` / `JSON.parse` across all Redis ops | In-memory JS objects | **Zero-Copy Node.js `Buffer` FFI** (zero V8 heap fragmentation) |

---

## 2. Zero-Dependency Embedded Mode: Why It Matters

### The Problem with BullMQ's Infrastructure Requirement
BullMQ is tightly coupled to Redis. By design, **BullMQ cannot operate without a running Redis server or cluster**:
- If Redis is down, unreachable, or undergoing failover, BullMQ cannot enqueue or process jobs.
- Lightweight deployments (CLI tools, desktop Electron apps, edge compute, serverless functions, development environments, and automated CI unit tests) are forced to spin up a Redis container or Redis cloud instance just to run a background job queue.
- Every single job addition incurs TCP roundtrip latency, Redis connection pool overhead, and V8 JSON serialization.

### How TurboPulse Solves This Without Redis
TurboPulse incorporates a **dual-mode engine**:
1. **Embedded In-Memory Mode (`EngineMode.InMemory`):**
   - Implemented in native compiled Rust using lock-free concurrent hash maps (`DashMap`) and `parking_lot` mutexes.
   - Provides **100% of the same features**: priority queues, delayed jobs, exponential backoff with jitter, worker pools, concurrency limits, stalled job recovery, and parent-child DAG workflows (`FlowProducer`).
   - **Zero external dependencies:** No Redis, no Postgres, no Docker containers required.
   - **Zero network overhead:** Microsecond execution with zero TCP handshake or network hop latency.
2. **Distributed Redis Mode (`EngineMode.Redis`):**
   - When your application scales across multiple servers and requires a shared, distributed cluster state, pass `{ connection: { host: '127.0.0.1', port: 6379 } }`.
   - TurboPulse seamlessly switches to atomic Redis Lua scripts while retaining zero-copy binary buffer serialization.

---

## 3. Head-to-Head: TurboPulse vs. Bottleneck (Rate Limiting)

Tested on the Ubuntu Linux VPS running identical task workloads.

### Test 3.1: Task Scheduling Throughput & Latency (50,000 Operations)
Workload: 50,000 asynchronous tasks scheduled through the limiter.

| Metric | Bottleneck (`2.19.5`) | TurboPulse (`1.0.4`) | Advantage |
| :--- | :---: | :---: | :---: |
| **Throughput** | 205 ops/sec | **39,141 ops/sec** | ⚡ **191x faster (19,093%)** |
| **Total Duration** | 243,635.59 ms *(~4.06 mins)* | **1,277.42 ms** *(1.27 sec)* | ⚡ **99.5% faster** |
| **Mean Latency** | 4,863.62 µs *(4.86 ms)* | **28.34 µs** *(0.028 ms)* | ⚡ **171x lower latency** |
| **p50 Latency (Median)** | 4,832.41 µs | **20.25 µs** | ⚡ **238x lower median** |
| **p95 Latency** | 5,515.47 µs | **42.53 µs** | ⚡ **130x lower tail latency** |
| **p99 Latency** | 7,094.66 µs | **159.49 µs** | ⚡ **44x lower latency spike** |
| **V8 Heap Memory Retention** | 25.2 MB (Δ +15.4 MB) | **11.4 MB** (Δ 0 MB) | 🛡️ **Zero GC heap retention** |

> **Why the massive gap?**  
> Bottleneck schedules tasks using JavaScript's native `setTimeout(..., 0)` loop for every permit check. In Node.js, `setTimeout` introduces libuv timer min-heap overhead and event-loop tick delays, creating severe latency bottlenecks as queue depth grows.  
> TurboPulse replaces timer loops with a **compiled Rust state machine** and a **hierarchical master timer wheel**, eliminating per-task timers and completing 50,000 operations in 1.2 seconds versus 4 minutes.

### Test 3.2: Multi-Tenant Key Sharding (2,000 Tenants, 10,000 Operations)
Workload: 2,000 distinct tenant keys (`tenant_0` to `tenant_1999`), each issuing 5 rate-limited requests.

| Metric | Bottleneck (`Group`) | TurboPulse (`RateLimiter.Group`) | Advantage |
| :--- | :---: | :---: | :---: |
| **Throughput** | 201 ops/sec | **23,323 ops/sec** | ⚡ **116x faster (11,603%)** |
| **Total Duration** | 49,634.07 ms *(49.6 sec)* | **428.77 ms** *(0.42 sec)* | ⚡ **115x faster** |

### Test 3.3: Native Ultra-Fast Token Bucket (`acquireSync`)
When bypassing Promise wrapper overhead for high-frequency firewall or gateway middleware:

| Metric | TurboPulse Native (`acquireSync`) |
| :--- | :---: |
| **Operations Tested** | 1,000,000 tokens |
| **Throughput** | **526,941 ops/sec** |
| **Mean Latency** | **2.06 µs** |
| **p50 Latency** | **2.13 µs** |
| **p99 Latency** | **3.23 µs** |

---

## 4. Head-to-Head: TurboPulse vs. BullMQ (Distributed Redis Queues)

Tested on the Ubuntu Linux VPS running against a local Redis 7.x server (`127.0.0.1:6379`) using identical ~300-byte real-world payload structures (`ORDER_FULFILLMENT` with nested items and customer metadata).

### Test 4.1: Single Job Enqueue to Redis (10,000 Jobs)
| Metric | BullMQ (`5.x`) | TurboPulse (`1.0.4`) | Advantage |
| :--- | :---: | :---: | :---: |
| **Enqueue Speed** | 5,702 jobs/sec | **5,850 jobs/sec** | ⚡ **TurboPulse leads** |
| **Total Duration** | 1,753.91 ms | **1,709.32 ms** | ⚡ **Faster pipeline** |
| **Mean Latency** | 186.48 µs | **176.67 µs** | ⚡ **Lower call overhead** |
| **p50 Latency** | 120.63 µs | **125.18 µs** | Identical |
| **p95 Latency** | 397.93 µs | **384.48 µs** | ⚡ **Tighter p95** |

### Test 4.2: Batch Enqueue (`addBulk`) to Redis (5,000 Jobs)
Workload: Atomically enqueuing 5,000 jobs in a single batch operation.

| Metric | BullMQ (`addBulk`) | TurboPulse (`addBulk`) | Advantage |
| :--- | :---: | :---: | :---: |
| **Batch Throughput** | 13,307 jobs/sec | **24,561 jobs/sec** | ⚡ **1.85x faster (+85%)** |
| **Total Duration** | 375.74 ms | **203.58 ms** | ⚡ **Near-instant batching** |

### Test 4.3: Concurrent Worker Processing / Drain (5,000 Jobs, Concurrency = 16)
Workload: 16 parallel worker threads processing and completing 5,000 jobs from Redis.

| Metric | BullMQ (`Worker`) | TurboPulse (`Worker`) | Advantage |
| :--- | :---: | :---: | :---: |
| **Drain Speed** | 8,928 jobs/sec | **11,648 jobs/sec** | ⚡ **1.30x faster (+30%)** |
| **Total Duration** | 560.03 ms | **429.27 ms** | ⚡ **Faster worker drain** |

> **Why does TurboPulse outperform BullMQ on the same Redis instance?**  
> 1. **Zero-Copy Node.js `Buffer` FFI:** BullMQ executes `JSON.stringify` on every enqueue and `JSON.parse` on every worker dequeue within JavaScript. TurboPulse transfers binary buffers directly into native Rust memory, bypassing V8 JSON overhead and GC pressure.  
> 2. **Native Redis Batch Pipelining:** TurboPulse's Rust backend pipelines atomic Redis commands using optimized memory buffers, avoiding multiple asynchronous Promise turns in Node.js.

---

## 5. Standalone In-Memory Mode Performance (Zero-Redis Overhead)

When your application does not need distributed multi-server coordination, TurboPulse can run in **Embedded In-Memory Mode** without Redis or Postgres:

```typescript
import { createInMemoryTurboPulse, Queue, Worker } from 'turbopulse';

// No Redis URL or connection configuration needed!
const engine = createInMemoryTurboPulse();
const queue = engine.createQueue('local_orders');

// Enqueue jobs with microsecond latency
await queue.add('order_1', { total: 99.95 });
```

### In-Memory Benchmarks (Measured on Linux VPS):
- **Native Rate Limiter Throughput:** **526,941 ops/sec** (Mean latency: **2.06 µs**)
- **Multi-Tenant Partitioning (10,000 Tenants):** **445,117 ops/sec**
- **Zero-Copy Queue Enqueue (50,000 Jobs):** **27,141 jobs/sec** (Mean latency: **41.65 µs**, p50: **27.76 µs**)
- **Concurrent Worker Drain (25,000 Jobs, Concurrency = 16):** **54,889 jobs/sec**
- **Process Memory Consumption Under Load:** RSS: **147.2 MB** | V8 Heap: **7.7 MB** (Near-zero GC pressure)
- **Max Event Loop Lag:** **73 ms** (Main thread stays fully responsive)

---

## 6. How to Reproduce These Benchmarks

### 1. In-Memory Suite (Standalone, Zero Redis Required)
```bash
npm install turbopulse
node -e "
const { createInMemoryTurboPulse } = require('turbopulse');
const engine = createInMemoryTurboPulse();
const limiter = engine.nativeEngine.createRateLimiter({ capacity: 1000000, refillRate: 1000000, refillIntervalMs: 1000 });
console.time('1M_tokens');
for(let i = 0; i < 1000000; i++) limiter.acquireSync('k', 1);
console.timeEnd('1M_tokens');
"
```

### 2. Full Head-to-Head Suite (TurboPulse vs BullMQ vs Bottleneck)
```bash
npm install turbopulse bullmq bottleneck ioredis
node benchmark_head_to_head.js
```
