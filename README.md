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

Benchmarked on Node.js v20 (Windows x86_64, Redis 8):

### Rate Limiter Throughput

| Engine Mode | Throughput | Avg Latency | V8 Heap Delta |
| :--- | :--- | :--- | :--- |
| **In-Memory** | **3,874,808 ops/sec** | 0.25 µs | +11.4 MB |
| **Redis (Lua + Lease)** | **2,069,002 ops/sec** | 0.48 µs | +51.7 MB |

Reproduce locally:
```bash
npm run benchmark:1m-speed
```

### Job Queue Throughput

| Operation | Concurrency | Throughput |
| :--- | :--- | :--- |
| **Enqueue** | Bulk (50,000 jobs) | 13,749 jobs/sec |
| **Worker Dispatch** | Concurrency 8 | 19,905 jobs/sec |

Reproduce locally:
```bash
npm run benchmark
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
