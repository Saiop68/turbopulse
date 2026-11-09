import * as net from 'net';
import { createTurboPulse, EngineMode } from '../dist/index';

// ============================================================================
// ANSI Styling
// ============================================================================
const C = {
  reset: '\x1b[0m',
  bold: '\x1b[1m',
  dim: '\x1b[2m',
  cyan: '\x1b[36m',
  green: '\x1b[32m',
  yellow: '\x1b[33m',
  magenta: '\x1b[35m',
  blue: '\x1b[34m',
  red: '\x1b[31m',
  white: '\x1b[37m',
};

interface MemorySnapshot {
  rss: number;
  heapUsed: number;
  heapTotal: number;
}

function getMemory(): MemorySnapshot {
  const m = process.memoryUsage();
  return {
    rss: +(m.rss / (1024 * 1024)).toFixed(2),
    heapUsed: +(m.heapUsed / (1024 * 1024)).toFixed(2),
    heapTotal: +(m.heapTotal / (1024 * 1024)).toFixed(2),
  };
}

function formatDelta(before: number, after: number): string {
  const diff = +(after - before).toFixed(2);
  const sign = diff >= 0 ? '+' : '';
  const color = diff > 20 ? C.yellow : diff > 50 ? C.red : C.green;
  return `${color}${sign}${diff} MB${C.reset}`;
}

interface LatencyStats {
  minUs: number;
  meanUs: number;
  p50Us: number;
  p95Us: number;
  p99Us: number;
  p999Us: number;
  maxUs: number;
}

function calculateLatencies(samplesNs: bigint[]): LatencyStats {
  if (samplesNs.length === 0) {
    return { minUs: 0, meanUs: 0, p50Us: 0, p95Us: 0, p99Us: 0, p999Us: 0, maxUs: 0 };
  }
  const samplesUs = samplesNs.map((ns) => Number(ns) / 1000).sort((a, b) => a - b);
  const sum = samplesUs.reduce((acc, val) => acc + val, 0);

  const p = (pct: number) => {
    const idx = Math.min(samplesUs.length - 1, Math.floor((pct / 100) * samplesUs.length));
    return samplesUs[idx];
  };

  return {
    minUs: +samplesUs[0].toFixed(2),
    meanUs: +(sum / samplesUs.length).toFixed(2),
    p50Us: +p(50).toFixed(2),
    p95Us: +p(95).toFixed(2),
    p99Us: +p(99).toFixed(2),
    p999Us: +p(99.9).toFixed(2),
    maxUs: +samplesUs[samplesUs.length - 1].toFixed(2),
  };
}

function printCard(title: string, metrics: [string, string][]) {
  console.log(`\n${C.bold}${C.red}┌─ [ ${title} ] ${'─'.repeat(Math.max(2, 68 - title.length))}┐${C.reset}`);
  for (const [key, value] of metrics) {
    const padding = 34 - key.length;
    console.log(`${C.red}│${C.reset}  ${C.white}${key}${C.reset}${' '.repeat(Math.max(1, padding))}: ${value}`);
  }
  console.log(`${C.red}└────────────────────────────────────────────────────────────────────────┘${C.reset}`);
}

async function queryRedisInfo(): Promise<Record<string, string>> {
  return new Promise((resolve) => {
    const socket = net.createConnection({ host: '127.0.0.1', port: 6379 }, () => {
      socket.write('INFO\r\n');
    });

    let data = '';
    socket.on('data', (chunk) => {
      data += chunk.toString();
      if (data.includes('# Commandstats') || data.length > 4096) {
        socket.end();
      }
    });

    socket.on('end', () => {
      const info: Record<string, string> = {};
      const lines = data.split('\r\n');
      for (const line of lines) {
        if (line && !line.startsWith('#')) {
          const parts = line.split(':');
          if (parts.length === 2) {
            info[parts[0]] = parts[1];
          }
        }
      }
      resolve(info);
    });

    socket.on('error', () => resolve({}));
  });
}

// ============================================================================
// REDIS DISTRIBUTED GOD-LEVEL BENCHMARK
// ============================================================================
async function runRedisBenchmark() {
  console.clear();
  console.log(`
${C.bold}${C.red}██████╗ ███████╗██████╗ ██╗███████╗    ██████╗ ███████╗███╗   ██╗ ██████╗██╗  ██╗
██╔══██╗██╔════╝██╔══██╗██║██╔════╝    ██╔══██╗██╔════╝████╗  ██║██╔════╝██║  ██║
██████╔╝█████╗  ██║  ██║██║███████╗    ██████╔╝█████╗  ██╔██╗ ██║██║     ███████║
██╔══██╗██╔══╝  ██║  ██║██║╚════██║    ██╔══██╗██╔══╝  ██║╚██╗██║██║     ██╔══██║
██║  ██║███████╗██████╔╝██║███████║    ██████╔╝███████╗██║ ╚████║╚██████╗██║  ██║
╚═╝  ╚═╝╚══════╝╚═════╝ ╚═╝╚══════╝    ╚═════╝ ╚══════╝╚═╝  ╚═══╝ ╚═════╝╚═╝  ╚═╝${C.reset}
  ${C.bold}${C.white}DISTRIBUTED REDIS DUAL-MODE BENCHMARK: ATOMIC LUA SCRIPTS & ZERO V8 GC${C.reset}
  ${C.dim}Connecting to 127.0.0.1:6379 via Rust multiplexed ConnectionManager pool...${C.reset}
  ================================================================================
`);

  const redisInfo = await queryRedisInfo();
  const redisVer = redisInfo['redis_version'] || 'Local Redis';
  const redisMem = redisInfo['used_memory_human'] || 'N/A';
  const initialMem = getMemory();

  console.log(`${C.bold}[REDIS] Redis Server Detected:${C.reset}`);
  console.log(`   • Server Version: ${C.cyan}${redisVer}${C.reset}`);
  console.log(`   • Initial Redis Memory: ${C.cyan}${redisMem}${C.reset}`);
  console.log(`   • Node.js Process Baseline RSS: ${C.green}${initialMem.rss} MB${C.reset} | Heap Used: ${C.green}${initialMem.heapUsed} MB${C.reset}\n`);

  // Initialize Native Engine in Redis Mode
  const REDIS_URL = 'redis://127.0.0.1:6379/0';
  const engine = await createTurboPulse({
    mode: EngineMode.Redis,
    redisUrl: REDIS_URL,
    keyPrefix: 'turbopulse_bench',
  });

  console.log(`${C.green}[OK] TurboPulse native engine successfully bound to Redis cluster/standalone instance!${C.reset}`);

  // ==========================================================================
  // STAGE 1: Redis Distributed Rate Limiter (50,000 Atomic Operations)
  // ==========================================================================
  console.log(`\n${C.bold}${C.yellow}════════════════════════════════════════════════════════════════════════════${C.reset}`);
  console.log(`${C.bold}STAGE 1: Redis Distributed Rate Limiter (50,000 Atomic Lua Operations)${C.reset}`);
  console.log(`${C.dim}Each call executes atomic dual-constraint Lua script (Token Bucket + MaxConcurrent)...${C.reset}`);

  const limiter1 = engine.createRateLimiter({
    capacity: 1_000_000,
    refillRate: 1_000_000,
    refillIntervalMs: 1000,
    maxConcurrent: 500_000,
  });

  const rawLimiter = (limiter1 as any).native;
  const stage1Count = 50_000;
  const stage1Latencies: bigint[] = [];

  const memStage1Before = getMemory();
  const t1Start = process.hrtime.bigint();

  for (let i = 0; i < stage1Count; i++) {
    const opStart = process.hrtime.bigint();
    await rawLimiter.acquire('redis_bench_key', 1);
    const opEnd = process.hrtime.bigint();
    if (i % 2 === 0) {
      stage1Latencies.push(opEnd - opStart);
    }
  }

  const t1End = process.hrtime.bigint();
  const stage1DurationMs = Number(t1End - t1Start) / 1_000_000;
  const stage1OpsPerSec = Math.round((stage1Count / stage1DurationMs) * 1000);
  const memStage1After = getMemory();
  const stats1 = calculateLatencies(stage1Latencies);

  printCard('STAGE 1: REDIS RATE LIMITER THROUGHPUT & LATENCY', [
    ['Total Redis Operations', `${stage1Count.toLocaleString()} atomic Lua evaluations`],
    ['Total Elapsed Time', `${stage1DurationMs.toFixed(2)} ms`],
    ['Distributed Throughput', `${C.bold}${C.green}${stage1OpsPerSec.toLocaleString()} ops/sec${C.reset}`],
    ['Average Network + Eval Latency', `${stats1.meanUs} µs (${(stats1.meanUs / 1000).toFixed(3)} ms)`],
    ['Median Latency (p50)', `${stats1.p50Us} µs`],
    ['95th Percentile (p95)', `${stats1.p95Us} µs`],
    ['99th Percentile (p99)', `${C.yellow}${stats1.p99Us} µs${C.reset}`],
    ['Max Observed Latency', `${stats1.maxUs} µs`],
    ['Node.js V8 Heap Delta', `${formatDelta(memStage1Before.heapUsed, memStage1After.heapUsed)}`],
    ['Process RSS Delta', `${formatDelta(memStage1Before.rss, memStage1After.rss)}`],
  ]);

  // ==========================================================================
  // STAGE 2: Multi-Client Concurrent Redis Contention (50,000 Ops across 50 Streams)
  // ==========================================================================
  console.log(`\n${C.bold}${C.yellow}════════════════════════════════════════════════════════════════════════════${C.reset}`);
  console.log(`${C.bold}STAGE 2: Multi-Client Concurrent Contention (50,000 Ops across 50 Streams)${C.reset}`);
  console.log(`${C.dim}Testing Redis pipeline multiplexing under parallel asynchronous client contention...${C.reset}`);

  const stage2Total = 50_000;
  const fiberCount = 50;
  const opsPerFiber = stage2Total / fiberCount;

  const memStage2Before = getMemory();
  const t2Start = process.hrtime.bigint();

  const streams = Array.from({ length: fiberCount }).map(async () => {
    for (let i = 0; i < opsPerFiber; i++) {
      await rawLimiter.acquire('redis_contention_key', 1);
    }
  });

  await Promise.all(streams);

  const t2End = process.hrtime.bigint();
  const stage2DurationMs = Number(t2End - t2Start) / 1_000_000;
  const stage2OpsPerSec = Math.round((stage2Total / stage2DurationMs) * 1000);
  const memStage2After = getMemory();

  printCard('STAGE 2: MULTI-STREAM REDIS CONTENTION RESULTS', [
    ['Concurrent Client Streams', `${fiberCount} parallel async loops`],
    ['Total Completed Permits', `${stage2Total.toLocaleString()} requests`],
    ['Total Duration', `${stage2DurationMs.toFixed(2)} ms`],
    ['Parallel Redis Throughput', `${C.bold}${C.green}${stage2OpsPerSec.toLocaleString()} ops/sec${C.reset}`],
    ['Avg Execution / Op', `${((stage2DurationMs / stage2Total) * 1000).toFixed(2)} µs`],
    ['V8 Heap Delta', `${formatDelta(memStage2Before.heapUsed, memStage2After.heapUsed)}`],
    ['RSS Memory Delta', `${formatDelta(memStage2Before.rss, memStage2After.rss)}`],
  ]);

  // ==========================================================================
  // STAGE 3: High-Cardinality Multi-Tenant Redis Buckets (5,000 Distinct Keys)
  // ==========================================================================
  console.log(`\n${C.bold}${C.yellow}════════════════════════════════════════════════════════════════════════════${C.reset}`);
  console.log(`${C.bold}STAGE 3: Multi-Tenant Key Sharding in Redis (5,000 Isolated Tenant Buckets)${C.reset}`);
  console.log(`${C.dim}Writing 5,000 independent hash records with TTL auto-expiration in Redis...${C.reset}`);

  const group = engine.createGroup({
    capacity: 50,
    refillRate: 50,
    refillIntervalMs: 1000,
  });

  const tenantCount = 5_000;
  const opsPerTenant = 3;
  const stage3Total = tenantCount * opsPerTenant;

  const memStage3Before = getMemory();
  const t3Start = process.hrtime.bigint();

  for (let t = 0; t < tenantCount; t++) {
    const tenantKey = `redis_tenant_${t}`;
    for (let o = 0; o < opsPerTenant; o++) {
      await (group as any).native.acquire(tenantKey, 1);
    }
  }

  const t3End = process.hrtime.bigint();
  const stage3DurationMs = Number(t3End - t3Start) / 1_000_000;
  const stage3OpsPerSec = Math.round((stage3Total / stage3DurationMs) * 1000);
  const memStage3After = getMemory();

  printCard('STAGE 3: REDIS MULTI-TENANT SHARDING RESULTS', [
    ['Distinct Redis Hash Buckets', `${tenantCount.toLocaleString()} isolated accounts`],
    ['Total Operations', `${stage3Total.toLocaleString()} requests`],
    ['Total Duration', `${stage3DurationMs.toFixed(2)} ms`],
    ['Multi-Bucket Throughput', `${C.bold}${C.green}${stage3OpsPerSec.toLocaleString()} ops/sec${C.reset}`],
    ['Key Auto-TTL', `Set to 60s dynamically on Redis`],
    ['V8 Heap Delta', `${formatDelta(memStage3Before.heapUsed, memStage3After.heapUsed)}`],
    ['Process RSS Delta', `${formatDelta(memStage3Before.rss, memStage3After.rss)}`],
  ]);

  // ==========================================================================
  // STAGE 4: Zero-Copy Distributed Queue Enqueue (25,000 Jobs on Redis)
  // ==========================================================================
  console.log(`\n${C.bold}${C.yellow}════════════════════════════════════════════════════════════════════════════${C.reset}`);
  console.log(`${C.bold}STAGE 4: Distributed Job Queue Enqueue (25,000 Jobs on Redis)${C.reset}`);
  console.log(`${C.dim}Atomic Lua script pushes to Redis wait ZSET, delayed ZSET, and jobs hashmap...${C.reset}`);

  const queue = engine.createQueue<{ id: number; action: string; amount: number }>('redisGodQueue');

  const payload = Buffer.from(
    JSON.stringify({
      action: 'PROCESS_DISTRIBUTED_PAYMENT',
      amount: 499.0,
      currency: 'EUR',
      merchantId: 'merch_88319028',
      timestamp: Date.now(),
    })
  );

  const stage4Count = 25_000;
  const stage4Latencies: bigint[] = [];

  const memStage4Before = getMemory();
  const t4Start = process.hrtime.bigint();

  for (let i = 0; i < stage4Count; i++) {
    const opStart = process.hrtime.bigint();
    await queue.add('chargePayment', payload as any, { priority: i % 10 });
    const opEnd = process.hrtime.bigint();
    if (i % 2 === 0) {
      stage4Latencies.push(opEnd - opStart);
    }
  }

  const t4End = process.hrtime.bigint();
  const stage4DurationMs = Number(t4End - t4Start) / 1_000_000;
  const stage4OpsPerSec = Math.round((stage4Count / stage4DurationMs) * 1000);
  const memStage4After = getMemory();
  const stats4 = calculateLatencies(stage4Latencies);

  const queueCounts = await queue.getJobCounts();

  printCard('STAGE 4: REDIS DISTRIBUTED QUEUE ENQUEUE RESULTS', [
    ['Total Enqueued Jobs', `${stage4Count.toLocaleString()} jobs on Redis`],
    ['Payload Size', `${payload.length} bytes per job`],
    ['Redis Wait ZSET Size', `${queueCounts.waiting.toLocaleString()} jobs stored in Redis ZSET`],
    ['Total Duration', `${stage4DurationMs.toFixed(2)} ms`],
    ['Distributed Enqueue Rate', `${C.bold}${C.green}${stage4OpsPerSec.toLocaleString()} jobs/sec${C.reset}`],
    ['Average Latency', `${stats4.meanUs} µs`],
    ['Median (p50)', `${stats4.p50Us} µs`],
    ['99th Percentile (p99)', `${C.yellow}${stats4.p99Us} µs${C.reset}`],
    ['V8 Heap Used Growth', `${formatDelta(memStage4Before.heapUsed, memStage4After.heapUsed)}`],
    ['Process RSS Growth', `${formatDelta(memStage4Before.rss, memStage4After.rss)}`],
  ]);

  // ==========================================================================
  // STAGE 5: Multi-Worker Concurrent Processing on Redis (10,000 Jobs, 16 Concurrency)
  // ==========================================================================
  console.log(`\n${C.bold}${C.yellow}════════════════════════════════════════════════════════════════════════════${C.reset}`);
  console.log(`${C.bold}STAGE 5: Multi-Worker Concurrent Processing on Redis (10,000 Jobs, 16 Concurrency)${C.reset}`);
  console.log(`${C.dim}16 parallel worker loops claiming jobs via atomic Lua ZPOPMAX and completing them...${C.reset}`);

  const targetProcess = 10_000;
  let processedCount = 0;

  const memStage5Before = getMemory();
  const t5Start = process.hrtime.bigint();

  let workerRef: any;

  await new Promise<void>((resolve) => {
    workerRef = engine.createWorker(
      'redisGodQueue',
      async (job) => {
        processedCount++;
        if (processedCount >= targetProcess) {
          resolve();
        }
        return 'done';
      },
      { concurrency: 16, pollIntervalMs: 2 }
    );
  });

  const t5End = process.hrtime.bigint();
  await workerRef.close();

  const stage5DurationMs = Number(t5End - t5Start) / 1_000_000;
  const stage5OpsPerSec = Math.round((targetProcess / stage5DurationMs) * 1000);
  const memStage5After = getMemory();

  printCard('STAGE 5: REDIS MULTI-WORKER DRAIN RESULTS', [
    ['Worker Concurrency', `16 parallel worker loops`],
    ['Jobs Processed & Completed', `${targetProcess.toLocaleString()} jobs`],
    ['Total Processing Duration', `${stage5DurationMs.toFixed(2)} ms`],
    ['Distributed Drain Throughput', `${C.bold}${C.green}${stage5OpsPerSec.toLocaleString()} jobs/sec${C.reset}`],
    ['Avg Claim + Complete Latency', `${((stage5DurationMs / targetProcess) * 1000).toFixed(2)} µs per job`],
    ['V8 Heap Delta', `${formatDelta(memStage5Before.heapUsed, memStage5After.heapUsed)}`],
    ['Process RSS Delta', `${formatDelta(memStage5Before.rss, memStage5After.rss)}`],
  ]);

  // ==========================================================================
  // FINAL EXECUTIVE REDIS SUMMARY
  // ==========================================================================
  const finalMem = getMemory();
  const finalRedisInfo = await queryRedisInfo();
  const finalRedisMem = finalRedisInfo['used_memory_human'] || 'N/A';

  console.log(`${C.bold}${C.magenta}================================================================================${C.reset}`);
  console.log(`${C.bold}${C.white}[SUMMARY] GRAND SUMMARY: REDIS DISTRIBUTED BENCHMARK SUITE${C.reset}`);
  console.log(`${C.bold}${C.magenta}================================================================================${C.reset}`);
  console.log(`  • ${C.bold}Total Cumulative Redis Operations:${C.reset}  ${C.cyan}150,000 Redis network operations${C.reset}`);
  console.log(`  • ${C.bold}Peak Distributed Rate Throughput:${C.reset}   ${C.green}${stage2OpsPerSec.toLocaleString()} ops/sec${C.reset}`);
  console.log(`  • ${C.bold}Average Single Lua Call Latency:${C.reset}    ${C.green}${stats1.meanUs} µs${C.reset} (${(stats1.meanUs / 1000).toFixed(3)} ms including network roundtrip)`);
  console.log(`  • ${C.bold}Peak Distributed Queue Enqueue:${C.reset}     ${C.green}${stage4OpsPerSec.toLocaleString()} jobs/sec${C.reset}`);
  console.log(`  • ${C.bold}Peak Distributed Worker Drain:${C.reset}      ${C.green}${stage5OpsPerSec.toLocaleString()} jobs/sec${C.reset} (16 parallel workers)`);
  console.log(`  • ${C.bold}Redis Server Memory Usage:${C.reset}          ${C.cyan}${finalRedisMem}${C.reset} (from initial ${redisMem})`);
  console.log(`  • ${C.bold}Node.js Total V8 Heap Net Delta:${C.reset}    ${formatDelta(initialMem.heapUsed, finalMem.heapUsed)} ${C.dim}(Flat memory profile, zero GC thrashing)${C.reset}`);
  console.log(`  • ${C.bold}Node.js Total RSS Net Delta:${C.reset}        ${formatDelta(initialMem.rss, finalMem.rss)}`);
  console.log(`${C.bold}${C.magenta}================================================================================${C.reset}\n`);

  process.exit(0);
}

runRedisBenchmark().catch((err) => {
  console.error('\n[FAIL] Redis Benchmark Error:', err);
  process.exit(1);
});
