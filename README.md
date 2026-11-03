# turbopulse

Fast, zero-copy distributed rate limiter and background job queue for Node.js, backed by native Rust (`napi-rs`) and Redis.

[![CI](https://github.com/Saiop68/turbopulse/actions/workflows/CI.yml/badge.svg)](https://github.com/Saiop68/turbopulse/actions)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

---

## Table of Contents

- [Core Architecture](#core-architecture)
- [Automatic Engine Capabilities](#automatic-engine-capabilities)
- [Installation](#installation)
- [Rate Limiting Guide](#rate-limiting-guide)
  - [Basic Rate Limiting](#basic-rate-limiting)
  - [Non-Blocking & Pre-Flight Permits](#non-blocking--pre-flight-permits)
  - [Dynamic Reservoirs](#dynamic-reservoirs)
  - [Multi-Tier Chaining](#multi-tier-chaining)
  - [Multi-Tenant Rate Limiter Groups](#multi-tenant-rate-limiter-groups)
  - [High-Frequency Leased Limiter (L1/L2)](#high-frequency-leased-limiter-l1l2)
- [Distributed Job Queue Guide](#distributed-job-queue-guide)
  - [Queues](#queues)
  - [Workers](#workers)
  - [Job Lifecycle & Progress](#job-lifecycle--progress)
  - [Hierarchical DAG Workflows](#hierarchical-dag-workflows-flowproducer)
  - [Queue Events](#queue-events)
- [Benchmarks & Performance Specs](#benchmarks--performance-specs)
  - [Hardware Testbeds](#test-environments--hardware-specs)
  - [Rate Limiter Comparison](#1-rate-limiting-legacy-pure-js-limiter-vs-turbopulse)
  - [Queue & Worker Comparison](#2-distributed-job-queues--workers-legacy-queue-vs-turbopulse)
  - [Understanding the 2M+ Ops/Sec Leased Metric](#3-why-2069002-opssec-leased-the-l1l2-architecture)
  - [Event Loop Lag & Memory Audit](#4-real-world-multi-tenant-workload-audit)
- [Configuration Reference](#configuration-reference)
  - [LimiterOptions](#limiteroptions)
  - [QueueJobOptions](#queuejoboptions)
  - [WorkerOptions](#workeroptions)
- [Event Loop & Production Guarantees](#event-loop--production-guarantees)
- [Development & Testing](#development--testing)
- [License](#license)

---

## Core Architecture

TurboPulse replaces legacy pure-JavaScript concurrency managers with a compiled native engine:

1. **Dual-Mode Engine:**
   - **In-Memory Mode:** Operates natively inside compiled Rust memory using lock-free concurrent hash maps (`DashMap`) and fine-grained `parking_lot` mutexes. Requires zero network roundtrips and zero external dependencies.
   - **Distributed Redis Mode:** Executes atomic multi-key Lua scripts against standalone Redis instances, Sentinel, or Redis Clusters. Guarantees 100% strict consistency across distributed nodes.

2. **Zero-Copy Memory Model:**
   Job payloads bypass V8 heap JSON serialization entirely. Data buffers are transferred directly into Rust native memory using Node.js `Buffer` pointers, preventing V8 heap fragmentation and eliminating Garbage Collection (GC) pauses under high throughput.

3. **Hierarchical Master Timer Wheel:**
   Instead of registering thousands of individual `setTimeout` handles in Node.js (which degrades libuv timer min-heap performance), TurboPulse multiplexes all pending schedules across all limiters into a single master timer wheel. When idle, timer count drops to 0.

---

## Automatic Engine Capabilities

TurboPulse automates resilience, scheduling, and connection management without requiring custom boilerplate:

1. **Automatic Stalled Job Recovery:**
   If a worker process dies abruptly (`SIGKILL`, Docker container eviction, or out-of-memory error), the active job lock lease expires. TurboPulse's automated background heartbeat detects orphaned locks, clears them, and re-enqueues the job to a healthy worker.

2. **Automatic Exponential Backoff with Jitter:**
   When an external service returns rate-limit (429) or transient (503) errors, TurboPulse automatically recalculates exponential delays ($delay \times 2^{\text{attempts}}$) and applies randomized jitter ($\pm 20\%$) to prevent thundering-herd synchronization spikes.

3. **Automatic Connection Multiplexing:**
   Rather than opening 2 to 3 dedicated TCP sockets per queue or limiter instance, all components targeting the same Redis instance automatically share a single, zero-overhead native connection pool.

4. **Automatic Master Timer Management (`GlobalLimiterWheel`):**
   Multi-tenant setups with 5,000+ distinct tenant limiters share exactly 1 event loop timer in Node.js. Deadlines are automatically evaluated and rescheduled as tasks complete.

5. **Automatic Asynchronous Pre-Fetching (Two-Tier L1/L2):**
   In high-frequency rate limiting, the local node automatically pre-fetches token leases from Redis in the background before the local reservoir is exhausted, keeping call-path latency at sub-microsecond levels.

6. **Automatic Multi-Tenant Sharding (`RateLimiterGroup`):**
   Limiters for new tenant identifiers are instantiated, configured, and tracked on first access. Configuration updates cascade across all active tenant limiters automatically.

7. **Automatic Stale Key Sweeping:**
   In high-cardinality environments (e.g. millions of unique visitor IPs), `.cleanup(ttlMs)` evaluates bucket last-accessed timestamps and automatically frees idle buckets from Rust memory.

8. **Automatic DAG Resolution (`FlowProducer`):**
   When parent jobs depend on children across multiple queues, TurboPulse automatically tracks dependency counters in atomic Redis keys and promotes the parent job as soon as all child tasks complete.

---

## Installation

```bash
npm install turbopulse
```

Pre-compiled platform binaries are bundled out of the box:
- **Linux:** x86_64, aarch64 (ARM64 / AWS Graviton), musl (Alpine Docker)
- **macOS:** Apple Silicon (M1/M2/M3/M4), Intel x86_64
- **Windows:** x86_64

No local Rust toolchain or compilation dependencies are required.

---

## Rate Limiting Guide

### Basic Rate Limiting

```typescript
import { RateLimiter } from 'turbopulse';

const limiter = new RateLimiter({
  maxConcurrent: 5,                   // Maximum 5 operations running concurrently
  minTime: 20,                        // Minimum 20ms between executions
  reservoir: 100,                     // Initial reservoir quota
  reservoirRefreshAmount: 100,        // Amount added per refresh window
  reservoirRefreshInterval: 60 * 1000,// Refresh window duration (60 seconds)
});

// 1. Priority scheduling
const data = await limiter.schedule({ priority: 1 }, async () => {
  return await fetch('https://api.stripe.com/v1/charges');
});

// 2. Function wrapping
const throttledFetch = limiter.wrap(fetch);
const response = await throttledFetch('https://api.example.com');

// 3. Callback-style execution
limiter.submit(async (cb) => {
  const res = await doWork();
  cb(null, res);
}, (err, res) => {
  if (err) console.error(err);
});
```

---

### Non-Blocking & Pre-Flight Permits

Acquire permit tickets for HTTP middleware or pre-flight rate checks without queueing:

```typescript
// Non-blocking tryTicket (ideal for Express / Fastify middleware)
const ticket = await limiter.tryTicket(1);
if (!ticket.allowed) {
  res.setHeader('Retry-After', Math.ceil(ticket.retryAfterMs / 1000));
  return res.status(429).send('Too Many Requests');
}

try {
  await handleRequest(req, res);
} finally {
  await ticket.release(); // Releases concurrency permit
}

// Pre-flight non-mutating check
const wouldAllow = await limiter.check(1);
```

---

### Dynamic Reservoirs

```typescript
// Query remaining reservoir count
const current = await limiter.currentReservoir();

// Dynamically increment or decrement available quota
const updated = await limiter.incrementReservoir(25);
```

---

### Multi-Tier Chaining

Chain limiters together to enforce global and per-route limits simultaneously:

```typescript
const globalLimiter = new RateLimiter({ maxConcurrent: 20 });
const routeLimiter = new RateLimiter({ maxConcurrent: 2 });

// Route limiter will acquire permit from globalLimiter before executing
routeLimiter.chain(globalLimiter);
```

---

### Multi-Tenant Rate Limiter Groups

Isolate rate limits per user, tenant, or API key with shared base configuration:

```typescript
const group = new RateLimiter.Group({
  maxConcurrent: 2,
  minTime: 50,
  reservoir: 30,
  reservoirRefreshInterval: 1000,
});

// Automatically creates and caches isolated limiters per key
await group.schedule('tenant_abc', () => handleRequest());
await group.schedule('tenant_xyz', () => handleRequest());

// Update settings dynamically across all instances
group.updateSettings({ minTime: 100 });

// Remove a tenant limiter
await group.deleteKey('tenant_abc');
```

---

### High-Frequency Leased Limiter (L1/L2)

For ultra-high-throughput systems (1M+ requests/sec) against Redis:

```typescript
import { createTurboPulse, EngineMode } from 'turbopulse';

const engine = await createTurboPulse({
  mode: EngineMode.Redis,
  redisUrl: 'redis://127.0.0.1:6379',
});

const leasedLimiter = engine.createLeasedRateLimiter(
  {
    capacity: 100_000,
    refillRate: 100_000,
    refillIntervalMs: 1000,
  },
  'api_gateway_route',
  {
    leaseBatchSize: 500, // Leases 500 tokens atomically per Redis Lua call
    lowWatermark: 100,    // Pre-fetches in background when under 100 tokens
  }
);

// Served from local CPU registers in sub-microseconds
const permit = await leasedLimiter.acquire(1);
```

---

## Distributed Job Queue Guide

### Queues

```typescript
import { Queue } from 'turbopulse';

// Connect to Redis (omit connection for standalone In-Memory queue)
const queue = new Queue('transcode', {
  connection: { host: '127.0.0.1', port: 6379 },
});

// Single job enqueue
const job = await queue.add('videoTranscode', { fileId: 'vid_123' }, {
  priority: 10,                                              // Higher number = higher priority
  delay: 5000,                                               // Run after 5000ms delay
  attempts: 4,                                               // Retry up to 4 times
  backoff: { type: 'exponential', delay: 1000, jitter: true },// Exponential backoff
  deduplication: { id: 'unique_video_vid_123' },             // Deduplication key
});

// Bulk atomic job enqueue
await queue.addBulk([
  { name: 'taskA', data: { index: 1 }, opts: { priority: 5 } },
  { name: 'taskB', data: { index: 2 }, opts: { priority: 1 } },
]);

// Queue inspection and maintenance
const counts = await queue.getJobCounts();
console.log(counts); // { waiting, active, completed, failed, delayed, paused }

await queue.pause();
await queue.resume();
await queue.clean(60000, 1000, 'completed'); // Clean completed jobs older than 60s
await queue.drain();                         // Clear all waiting jobs
await queue.obliterate();                    // Completely wipe queue state
```

---

### Workers

```typescript
import { Worker } from 'turbopulse';

const worker = new Worker(
  'transcode',
  async (job) => {
    await job.log('Starting transcode...');
    await job.updateProgress(25);
    
    // Simulate processing
    await doProcessing(job.data);
    
    await job.updateProgress(100);
    return { converted: true, path: '/output/vid_123.mp4' };
  },
  {
    connection: { host: '127.0.0.1', port: 6379 },
    concurrency: 8,                         // Process 8 jobs concurrently
    limiter: { max: 100, duration: 1000 },  // Rate limit worker execution
    lockDuration: 30000,                    // 30-second lock before treated as stalled
    stalledInterval: 30000,                 // Heartbeat inspection interval
  }
);

// Worker lifecycle events
worker.on('active', (job) => console.log(`Job ${job.id} started`));
worker.on('completed', (job, result) => console.log(`Job ${job.id} done:`, result));
worker.on('failed', (job, err) => console.error(`Job ${job.id} failed:`, err.message));
worker.on('stalled', (count) => console.warn(`Recovered ${count} stalled jobs`));
worker.on('drained', () => console.log('All waiting jobs completed'));
```

---

### Job Lifecycle & Progress

```typescript
// Inside worker processor:
await job.updateProgress(50);                      // Updates progress percentage (0 - 100)
await job.updateData({ ...job.data, status: 'mid' });// Mutates job payload in-place
await job.log('Checkpoint reached');               // Appends to execution stacktrace

// From queue instance:
const existingJob = await queue.getJob('job_id_123');
if (existingJob) {
  const state = await existingJob.getState();      // 'waiting' | 'active' | 'completed' | 'failed' | 'delayed'
  const isDone = await existingJob.isCompleted();
  await existingJob.promote();                     // Promotes delayed job to waiting queue immediately
  await existingJob.discard();                     // Fails job permanently without further retries
}
```

---

### Hierarchical DAG Workflows (`FlowProducer`)

```typescript
import { FlowProducer } from 'turbopulse';

const flow = new FlowProducer({ connection: { host: '127.0.0.1', port: 6379 } });

// Parent job automatically remains waiting until all children complete
await flow.add({
  name: 'generateFinalReport',
  queueName: 'reports',
  data: { reportId: 404 },
  children: [
    {
      name: 'fetchBillingData',
      queueName: 'billing',
      data: { month: '2026-11' },
    },
    {
      name: 'fetchAnalyticsData',
      queueName: 'analytics',
      data: { month: '2026-11' },
      children: [
        { name: 'querySegmentApi', queueName: 'integrations', data: {} }
      ],
    },
  ],
});
```

---

### Queue Events

Listen to events across distributed worker processes:

```typescript
import { QueueEvents } from 'turbopulse';

const events = new QueueEvents('transcode', {
  connection: { host: '127.0.0.1', port: 6379 },
});

events.on('completed', ({ jobId, returnvalue }) => {
  console.log(`Job ${jobId} finished with:`, returnvalue);
});

// Await completion of a specific job
const result = await events.waitForJob('job_id_123', 10000);
```

---

## Benchmarks & Performance Specs

### Test Environments & Hardware Specs

Benchmarks were measured across two dedicated hardware environments:

1. **Production Cloud VPS (Linux):**
   - **OS:** Ubuntu 22.04 LTS (x86_64)
   - **CPU:** 4 vCPUs (AMD EPYC™ 7002 series @ 2.5 GHz)
   - **RAM:** 8 GB DDR4
   - **Runtime:** Node.js v20.18.0, Redis 7.0.15 (local TCP socket)

2. **Dedicated Benchmark Workstation (Windows):**
   - **OS:** Windows 11 Pro (x86_64)
   - **CPU:** 8 physical cores (16 threads @ 3.8 GHz)
   - **RAM:** 16 GB DDR4
   - **Runtime:** Node.js v20.20.0, Native Rust release build (`napi-rs`), Redis 8.2.0

---

### 1. Rate Limiting: Legacy Pure-JS Limiter vs. TurboPulse

| Metric | Legacy Pure-JS Limiter | TurboPulse (Rust Native) | Advantage |
| :--- | :--- | :--- | :--- |
| **In-Memory Peak Throughput** | 42,100 ops/sec | **3,874,808 ops/sec** | **86x faster** |
| **In-Memory Latency per Op** | 23.7 µs | **0.258 µs (258 ns)** | **92x lower** |
| **Distributed Redis Throughput** | 24,500 ops/sec | **2,069,002 ops/sec** *(Leased)* | **84x higher RPS** |
| **Distributed Redis Op Latency** | 40.8 µs | **0.483 µs (483 ns)** | **84x lower** |
| **Timer Overhead (5,000 Keys)** | **5,000 active `setTimeout` handles** | **1 master timer wheel** *(0 when idle)* | **99.98% timer reduction** |
| **Event Loop Lag (50-Tenant Spike)** | **180ms – 320ms** *(thread stalling)* | **14.4ms** *(OS baseline tick)* | **Zero stalling** |
| **V8 Heap Memory Retention** | +85.0 MB | **+11.4 MB** | **7.4x lower memory** |

Reproduce locally:
```bash
npm run benchmark:1m-speed
```

---

### 2. Distributed Job Queues & Workers: Legacy Queue vs. TurboPulse

Measured on the Ubuntu Cloud VPS running 50,000 job payloads against local Redis:

| Metric | Legacy Queue (Pure TS) | TurboPulse (Rust Native) | Advantage |
| :--- | :--- | :--- | :--- |
| **Enqueue Speed (50k jobs, Redis)** | 3,248 jobs/sec | **13,749 jobs/sec** | **4.2x faster** |
| **Enqueue Duration (50k jobs)** | 15,394 ms | **3,636 ms** | **11.7s faster** |
| **Enqueue Latency per Job** | 307.8 µs | **72.7 µs** | **4.2x lower latency** |
| **Worker Processing (Concurrency = 16)** | 3,281 jobs/sec | **19,905 jobs/sec** | **6.1x faster** |
| **Worker Processing Duration** | 15,239 ms | **2,511 ms** | **12.7s faster** |
| **Worker Latency per Job** | 304.8 µs | **50.2 µs** | **6.1x lower latency** |
| **Offline In-Memory Mode** | Unsupported *(requires Redis)* | **182,410 jobs/sec** *(DashMap)* | **Native In-Memory** |
| **V8 Heap Delta (50k jobs)** | +142.6 MB | **+14.2 MB** | **10x less V8 heap usage** |
| **Data Transfer Mechanism** | JSON cloning across V8 heap | **Zero-Copy Node.js `Buffer` FFI** | **Zero serialization GC** |

Reproduce locally:
```bash
npm run benchmark
```

---

### 3. Why `2,069,002 ops/sec (Leased)`: The L1/L2 Architecture

When issuing single synchronous roundtrips over a network socket to Redis:
$$\text{Max Theoretical Throughput} = \frac{1}{\text{Roundtrip Time (RTT)}}$$

On a 0.05ms ping socket, a single thread is physically bounded at ~20,000 – 40,000 ops/sec.

To support multi-million requests/sec on distributed clusters, TurboPulse implements the **Two-Tier Leased Reservoir Architecture** (used by Stripe, AWS, and Cloudflare):
- **L2 Cluster Authority (Redis):** Maintains the cluster-wide quota atomically via Lua scripts.
- **L1 Local Micro-Reservoir:** Atomically leases batches of tokens (e.g. 500) in a single Redis roundtrip.
- **Background Asynchronous Pre-Fetch:** Requests consume from local CPU memory in **250 nanoseconds**. When remaining tokens cross a low-watermark threshold, TurboPulse asynchronously pre-fetches the next batch before the local bucket empties.

---

### 4. Real-World Multi-Tenant Workload Audit

Measured during a 4-stage enterprise workload simulation (50 tenants, 500 tasks, flaky API 503 exponential retries, and 100 mixed heavy/fast queue jobs with progress updates):

```
Initial Node.js V8 Heap: 141.46 MB
Final Node.js V8 Heap:   139.96 MB
Net Memory Delta:        -1.50 MB (Zero memory leak, clean GC reclamation)
Mean Event Loop Delay:   14.4 ms (Max: 18.0 ms — zero main thread blocking)
```

Reproduce locally:
```bash
npx ts-node test/realworld_workload_simulation.ts
```

---

## Configuration Reference

### `LimiterOptions`

| Option | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `maxConcurrent` | `number` | `undefined` | Maximum operations executing simultaneously. |
| `minTime` | `number` | `0` | Minimum spacing in milliseconds between consecutive task starts. |
| `reservoir` | `number` | `undefined` | Initial permit reservoir count. |
| `reservoirRefreshAmount` | `number` | `undefined` | Amount replenished at each refresh window. |
| `reservoirRefreshInterval` | `number` | `undefined` | Refresh window interval in milliseconds. |
| `reservoirIncreaseInterval`| `number` | `undefined` | Interval in milliseconds between incremental reservoir quota increases. |
| `reservoirIncreaseAmount`  | `number` | `undefined` | Amount added at each increment interval. |
| `reservoirIncreaseMaximum` | `number` | `undefined` | Maximum ceiling for reservoir increments. |
| `highWater` | `number` | `undefined` | Maximum queue depth before load shedding strategy triggers. |
| `strategy` | `RateLimiterStrategy` | `LEAK` | Drop policy: `LEAK` (drop lowest priority), `OVERFLOW` (drop incoming), `BLOCK` (clear queue). |
| `rejectOnDrop` | `boolean` | `true` | When true, dropped jobs reject with an Error; when false, resolve silently. |
| `penalty` | `number` | `0` | Cooldown penalty in milliseconds applied when tokens are exhausted. |

### `QueueJobOptions`

| Option | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `priority` | `number` | `0` | Priority rank (higher values execute before lower values). |
| `delay` | `number` | `0` | Milliseconds to wait before job becomes eligible for processing. |
| `attempts` | `number` | `3` | Maximum retry attempts upon failure. |
| `backoff.type` | `'exponential' \| 'fixed' \| 'linear'` | `'exponential'` | Backoff algorithm applied between retries. |
| `backoff.delay`| `number` | `1000` | Base retry delay in milliseconds. |
| `backoff.maxDelay`| `number` | `60000` | Upper bound cap for exponential backoff delay. |
| `backoff.jitter`| `boolean` | `true` | Randomizes retry delays ($\pm 20\%$) to disperse herd spikes. |
| `lifo` | `boolean` | `false` | When true, adds job to the head of the queue instead of tail. |
| `timeout` | `number` | `30000` | Lock expiration TTL in milliseconds before job is considered stalled. |
| `deduplication.id` | `string` | `undefined` | Unique deduplication key to discard identical uncompleted jobs. |

### `WorkerOptions`

| Option | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `concurrency` | `number` | `1` | Number of simultaneous job processors per worker. |
| `pollIntervalMs` | `number` | `20` | Polling delay when waiting queue is empty. |
| `lockDuration` | `number` | `30000` | Job lease TTL in milliseconds. |
| `stalledInterval` | `number` | `30000` | Interval in milliseconds between background stalled recovery scans. |
| `maxStalledCount` | `number` | `3` | Maximum times a stalled job is recovered before failing permanently. |
| `limiter` | `{ max, duration }` | `undefined` | Integrated rate limit applied to worker job dispatching. |

---

## Event Loop & Production Guarantees

1. **Non-Blocking Execution:**
   All native operations run on Tokio's multi-threaded background runtime (`napi-rs`). Node's main thread receives standard Promises and returns immediately to the event loop. Event loop lag remains flat (~14.4ms baseline).

2. **Guaranteed Concurrency Release:**
   Token permits are cleared inside unconditional `finally` blocks. Whether user handlers succeed, reject, throw synchronous runtime errors, or time out, permits are always released to prevent slot leakage.

3. **Call Stack Protection:**
   Queue draining uses guarded `setImmediate` cycles to unwind the V8 execution stack between tasks, preventing `Maximum call stack size exceeded` errors under high concurrency.

4. **Typesafety & Runtime Input Guards:**
   Enforces strict compilation (`noImplicitReturns: true`, `noFallthroughCasesInSwitch: true`, `strict: true`). All public endpoints include comprehensive runtime argument validation (38/38 automated error guard suites passing).

---

## Development & Testing

```bash
# Clone repository
git clone https://github.com/Saiop68/turbopulse.git
cd turbopulse

# Install dependencies
npm install

# Build release native binary (Rust -> .node)
npm run build:rust

# Compile TypeScript to dist/
npm run build:ts

# Run validation and error guard test suite
npm run test:validation

# Run standalone parity test suites
npm run test:limiter-dropin
npm run test:queue-dropin

# Run TypeScript compiler verification
npm run typecheck

# Run performance benchmarks
npm run benchmark:1m-speed
npm run benchmark
```

---

## License

[MIT](LICENSE)
