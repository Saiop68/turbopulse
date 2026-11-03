# turbopulse

Fast, zero-copy distributed rate limiter and background job queue for Node.js, backed by native Rust (`napi-rs`) and Redis.

[![CI](https://github.com/Saiop68/turbopulse/actions/workflows/CI.yml/badge.svg)](https://github.com/Saiop68/turbopulse/actions)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

---

## Features

- **Dual-Mode Engine:** Run in-memory via concurrent Rust maps (`DashMap`) or distributed across multiple nodes via Redis (atomic Lua scripts).
- **Zero-Copy Payloads:** Passes job data directly through native Node.js `Buffer`s, bypassing V8 JSON serialization and GC pressure.
- **Hierarchical Timer Wheel:** Collapses thousands of tenant rate limit schedules into a single master timer in Node.js, preventing libuv timer min-heap thrashing.
- **Full Concurrency Control:** Token bucket, sliding window, leaky bucket, burst protection, `minTime` spacing, and reservoir replenishments.
- **Job Queues & Workers:** Priority scheduling, delayed execution, exponential backoff with randomized jitter, parent-child DAG flows, and automated stalled job recovery.
- **Drop-in Parity:** Compatible with standard Node.js limiter and queue interfaces.

---

## Installation

```bash
npm install turbopulse
```

Pre-built binaries are provided for:
- Linux (x64, ARM64 / Graviton, musl)
- macOS (Apple Silicon M-series, Intel x64)
- Windows (x64)

No local Rust toolchain or build tools are required for installation.

---

## Quick Start

### 1. Rate Limiting

```typescript
import { RateLimiter } from 'turbopulse';

const limiter = new RateLimiter({
  maxConcurrent: 5,              // Max 5 tasks running concurrently
  minTime: 20,                   // Minimum 20ms between executions
  reservoir: 100,                // 100 requests per minute
  reservoirRefreshAmount: 100,
  reservoirRefreshInterval: 60 * 1000,
});

// Schedule an async operation
const result = await limiter.schedule(() => fetch('https://api.example.com/data'));

// Or wrap an existing function
const throttledFetch = limiter.wrap(fetch);
```

#### Multi-Tenant Rate Limiter Groups

Manage isolated limits per user, route, or API key:

```typescript
const group = new RateLimiter.Group({
  maxConcurrent: 2,
  minTime: 50,
  reservoir: 30,
  reservoirRefreshInterval: 1000,
});

// Automatically creates and tracks isolated limiters per key
await group.schedule('tenant_abc', () => handleRequest());
await group.schedule('tenant_xyz', () => handleRequest());
```

---

### 2. Distributed Job Queues & Workers

```typescript
import { Queue, Worker } from 'turbopulse';

// Connect to Redis (omit connection for in-memory)
const queue = new Queue('transcode', {
  connection: { host: '127.0.0.1', port: 6379 },
});

// Enqueue a job with options
await queue.add('video', { file: 'video.mp4' }, {
  priority: 10,
  delay: 2000,
  attempts: 3,
  backoff: { type: 'exponential', delay: 1000, jitter: true },
});

// Process jobs
const worker = new Worker(
  'transcode',
  async (job) => {
    await job.updateProgress(50);
    return { converted: true, id: job.id };
  },
  {
    connection: { host: '127.0.0.1', port: 6379 },
    concurrency: 8,
    limiter: { max: 100, duration: 1000 },
  }
);

worker.on('completed', (job, result) => {
  console.log(`Job ${job.id} completed`);
});
```

#### Parent-Child Workflows (DAGs)

```typescript
import { FlowProducer } from 'turbopulse';

const flow = new FlowProducer({ connection: { host: '127.0.0.1', port: 6379 } });

// Parent job waits until all children finish
await flow.add({
  name: 'generateReport',
  queueName: 'reports',
  children: [
    { name: 'fetchBilling', queueName: 'billing' },
    { name: 'fetchAnalytics', queueName: 'analytics' },
  ],
});
```

---

## Benchmarks

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

### 3. Real-World Multi-Tenant Workload Health Audit

Measured under a 4-stage enterprise workload simulation (50 tenants, 500 tasks, flaky API 503 exponential retries, and 100 mixed heavy/fast queue jobs with progress updates):

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

## Configuration

### `RateLimiter` Options

| Option | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `maxConcurrent` | `number` | `undefined` | Maximum concurrent executions. |
| `minTime` | `number` | `0` | Minimum spacing in milliseconds between consecutive calls. |
| `reservoir` | `number` | `undefined` | Number of tokens initially available. |
| `reservoirRefreshAmount` | `number` | `undefined` | Number of tokens refilled at each interval. |
| `reservoirRefreshInterval` | `number` | `undefined` | Refresh interval in milliseconds. |
| `highWater` | `number` | `undefined` | Maximum queue depth before load shedding strategy applies. |
| `strategy` | `Strategy` | `LEAK` | Queue overflow behavior: `LEAK`, `OVERFLOW`, `BLOCK`, `LEAK_PRIORITY`. |
| `rejectOnDrop` | `boolean` | `true` | Whether rejected/dropped tasks throw an error or resolve silently. |

### `Queue` Job Options

| Option | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `priority` | `number` | `0` | Execution priority (higher value runs first). |
| `delay` | `number` | `0` | Delay in milliseconds before job becomes available. |
| `attempts` | `number` | `3` | Max retry attempts before failing permanently. |
| `backoff` | `BackoffOptions` | `exponential` | Retry delay configuration (`type`, `delay`, `jitter`). |
| `lifo` | `boolean` | `false` | When true, pushes job to the front of waiting queue. |
| `timeout` | `number` | `30000` | Job lock duration before being treated as stalled. |

---

## Development & Testing

```bash
# Install dependencies
npm install

# Build native binary and compile TypeScript
npm run build

# Run test suites
npm test
npm run test:queue
npm run test:validation

# Run typecheck
npm run typecheck
```

---

## License

[MIT](LICENSE)
