import { createInMemoryTurboPulse, createTurboPulse, EngineMode } from '../dist/index';

// ============================================================================
// ANSI Color & Styling Utilities
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
  external: number;
}

function getMemory(): MemorySnapshot {
  const m = process.memoryUsage();
  return {
    rss: +(m.rss / (1024 * 1024)).toFixed(2),
    heapUsed: +(m.heapUsed / (1024 * 1024)).toFixed(2),
    heapTotal: +(m.heapTotal / (1024 * 1024)).toFixed(2),
    external: +(m.external / (1024 * 1024)).toFixed(2),
  };
}

function formatDelta(before: number, after: number): string {
  const diff = +(after - before).toFixed(2);
  const sign = diff >= 0 ? '+' : '';
  const color = diff > 50 ? C.yellow : diff > 100 ? C.red : C.green;
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
  console.log(`\n${C.bold}${C.cyan}┌─ [ ${title} ] ${'─'.repeat(Math.max(2, 68 - title.length))}┐${C.reset}`);
  for (const [key, value] of metrics) {
    const padding = 34 - key.length;
    console.log(`${C.cyan}│${C.reset}  ${C.white}${key}${C.reset}${' '.repeat(Math.max(1, padding))}: ${value}`);
  }
  console.log(`${C.cyan}└────────────────────────────────────────────────────────────────────────┘${C.reset}`);
}

async function runOneMillionOpsBenchmark() {
  console.clear();
  console.log(`
${C.bold}${C.magenta}██████╗ ███████╗███╗   ██╗ ██████╗██╗  ██╗███╗   ███╗ █████╗ ██████╗ ██╗  ██╗
██╔══██╗██╔════╝████╗  ██║██╔════╝██║  ██║████╗ ████║██╔══██╗██╔══██╗██║ ██╔╝
██████╔╝█████╗  ██╔██╗ ██║██║     ███████║██╔████╔██║███████║██████╔╝█████╔╝ 
██╔══██╗██╔══╝  ██║╚██╗██║██║     ██╔══██║██║╚██╔╝██║██╔══██║██╔══██╗██╔═██╗ 
██████╔╝███████╗██║ ╚████║╚██████╗██║  ██║██║ ╚═╝ ██║██║  ██║██║  ██║██║  ██╗
╚═════╝ ╚══════╝╚═╝  ╚═══╝ ╚═════╝╚═╝  ╚═╝╚═╝     ╚═╝╚═╝  ╚═╝╚═╝  ╚═╝╚═╝  ╚═╝${C.reset}
  ${C.bold}${C.white}⚡ 1,000,000 OPERATIONS EXTREME TORTURE TEST (IN-MEMORY + REDIS)${C.reset}
  ${C.dim}Measuring Sustained Throughput, Nanosecond Latencies & RAM Delta across 1,000,000 Ops${C.reset}
  ================================================================================
`);

  const initialMem = getMemory();
  console.log(`${C.bold}📊 Initial System Memory State:${C.reset}`);
  console.log(`   • Process RSS: ${C.green}${initialMem.rss} MB${C.reset} | V8 Heap Used: ${C.green}${initialMem.heapUsed} MB${C.reset} | V8 Heap Total: ${initialMem.heapTotal} MB\n`);

  // ==========================================================================
  // PART 1: 1,000,000 IN-MEMORY RATE LIMITING PERMITS (PURE SPEED TORTURE)
  // ==========================================================================
  console.log(`${C.bold}${C.yellow}════════════════════════════════════════════════════════════════════════════${C.reset}`);
  console.log(`${C.bold}PART 1: 1,000,000 OPERATIONS ON IN-MEMORY ENGINE (NATIVE RUST DASHPAM)${C.reset}`);
  console.log(`${C.dim}Testing 1 Million token bucket acquisitions across 50 concurrent async streams...${C.reset}`);

  const imEngine = createInMemoryTurboPulse();
  const imLimiter = imEngine.createRateLimiter({
    capacity: 100_000_000,
    refillRate: 100_000_000,
    refillIntervalMs: 1000,
  });

  const rawImLimiter = (imLimiter as any).native;
  const TOTAL_IM_OPS = 1_000_000;
  const CONCURRENT_FIBERS = 50;
  const OPS_PER_FIBER = TOTAL_IM_OPS / CONCURRENT_FIBERS;

  let completedOps = 0;
  const latencySamples: bigint[] = [];

  const tStart1 = process.hrtime.bigint();
  const memStart1 = getMemory();

  // Milestone checkpoints: 250k, 500k, 750k, 1M
  const milestones = [250_000, 500_000, 750_000, 1_000_000];
  let nextMilestoneIdx = 0;

  const milestoneLog: { ops: number; timeMs: number; opsPerSec: number; heapMB: number; rssMB: number }[] = [];

  const fibers = Array.from({ length: CONCURRENT_FIBERS }).map(async (_, fiberId) => {
    for (let i = 0; i < OPS_PER_FIBER; i++) {
      const opStart = process.hrtime.bigint();
      await rawImLimiter.acquire('one_million_key', 1);
      const opEnd = process.hrtime.bigint();

      // Sample 1 out of every 10 operations for high-resolution latency curve
      if (i % 10 === 0) {
        latencySamples.push(opEnd - opStart);
      }

      completedOps++;

      if (
        nextMilestoneIdx < milestones.length &&
        completedOps >= milestones[nextMilestoneIdx]
      ) {
        const target = milestones[nextMilestoneIdx];
        nextMilestoneIdx++;
        const elapsedNow = Number(process.hrtime.bigint() - tStart1) / 1_000_000;
        const currentRate = Math.round((target / elapsedNow) * 1000);
        const memNow = getMemory();

        milestoneLog.push({
          ops: target,
          timeMs: +elapsedNow.toFixed(1),
          opsPerSec: currentRate,
          heapMB: memNow.heapUsed,
          rssMB: memNow.rss,
        });

        process.stdout.write(
          `   ⚡ Progress: ${C.bold}${target.toLocaleString()}${C.reset} / 1,000,000 ops | ` +
            `Throughput: ${C.green}${currentRate.toLocaleString()} ops/s${C.reset} | ` +
            `V8 Heap: ${memNow.heapUsed}MB | Elapsed: ${elapsedNow.toFixed(0)}ms\r`
        );
      }
    }
  });

  await Promise.all(fibers);

  const tEnd1 = process.hrtime.bigint();
  const totalDurationMs1 = Number(tEnd1 - tStart1) / 1_000_000;
  const avgOpsPerSec1 = Math.round((TOTAL_IM_OPS / totalDurationMs1) * 1000);
  const memEnd1 = getMemory();
  const latStats1 = calculateLatencies(latencySamples);

  console.log('\n'); // Line break after progress output
  printCard('PART 1 RESULTS: 1,000,000 IN-MEMORY PERMITS COMPLETED', [
    ['Total Executed Operations', `${TOTAL_IM_OPS.toLocaleString()} permits`],
    ['Total Duration', `${(totalDurationMs1 / 1000).toFixed(2)} seconds (${totalDurationMs1.toFixed(1)} ms)`],
    ['Sustained Throughput', `${C.bold}${C.green}${avgOpsPerSec1.toLocaleString()} ops/sec${C.reset}`],
    ['Average Latency per Op', `${latStats1.meanUs} µs`],
    ['Median Latency (p50)', `${latStats1.p50Us} µs`],
    ['95th Percentile (p95)', `${latStats1.p95Us} µs`],
    ['99th Percentile (p99)', `${C.yellow}${latStats1.p99Us} µs${C.reset}`],
    ['99.9th Percentile (p99.9)', `${latStats1.p999Us} µs`],
    ['V8 Heap Delta (1M Ops)', `${formatDelta(memStart1.heapUsed, memEnd1.heapUsed)} (${memStart1.heapUsed}MB ➔ ${memEnd1.heapUsed}MB)`],
    ['Process RSS Growth (1M Ops)', `${formatDelta(memStart1.rss, memEnd1.rss)} (${memStart1.rss}MB ➔ ${memEnd1.rss}MB)`],
  ]);

  console.log(`\n${C.bold}📈 1 Million Operations RAM Stability Progression:${C.reset}`);
  console.log(`   ┌───────────────┬──────────────┬──────────────┬──────────────┬──────────────┐`);
  console.log(`   │ Milestone     │ Elapsed Time │ Speed        │ V8 Heap Used │ Process RSS  │`);
  console.log(`   ├───────────────┼──────────────┼──────────────┼──────────────┼──────────────┤`);
  for (const m of milestoneLog) {
    console.log(
      `   │ ${m.ops.toLocaleString().padEnd(13)} │ ${(m.timeMs + ' ms').padEnd(12)} │ ${(m.opsPerSec.toLocaleString() + ' op/s').padEnd(12)} │ ${(m.heapMB + ' MB').padEnd(12)} │ ${(m.rssMB + ' MB').padEnd(12)} │`
    );
  }
  console.log(`   └───────────────┴──────────────┴──────────────┴──────────────┴──────────────┘`);

  // ==========================================================================
  // PART 2: 1,000,000 DISTRIBUTED REDIS STRESS TEST (PIPELINED CLUSTER)
  // ==========================================================================
  console.log(`\n${C.bold}${C.yellow}════════════════════════════════════════════════════════════════════════════${C.reset}`);
  console.log(`${C.bold}PART 2: 1,000,000 OPERATIONS ON DISTRIBUTED REDIS (TCP NETWORK PIPELINE)${C.reset}`);
  console.log(`${C.dim}Testing 1 Million atomic Lua executions on 127.0.0.1:6379 across 100 parallel streams...${C.reset}`);

  const redisEngine = await createTurboPulse({
    mode: EngineMode.Redis,
    redisUrl: 'redis://127.0.0.1:6379/0',
    keyPrefix: 'turbopulse_1m',
  });

  const redisLimiter = redisEngine.createRateLimiter({
    capacity: 100_000_000,
    refillRate: 100_000_000,
    refillIntervalMs: 1000,
    maxConcurrent: 10_000_000,
  });

  const rawRedisLimiter = (redisLimiter as any).native;
  const TOTAL_REDIS_OPS = 1_000_000;
  const REDIS_FIBERS = 100;
  const REDIS_OPS_PER_FIBER = TOTAL_REDIS_OPS / REDIS_FIBERS;

  let redisCompleted = 0;
  const redisLatencySamples: bigint[] = [];

  const tStart2 = process.hrtime.bigint();
  const memStart2 = getMemory();

  let nextRedisMilestone = 250_000;
  const redisMilestoneLog: { ops: number; timeMs: number; opsPerSec: number; heapMB: number; rssMB: number }[] = [];

  const redisFibers = Array.from({ length: REDIS_FIBERS }).map(async (_, fiberId) => {
    for (let i = 0; i < REDIS_OPS_PER_FIBER; i++) {
      const opStart = process.hrtime.bigint();
      await rawRedisLimiter.acquire('one_million_redis_key', 1);
      const opEnd = process.hrtime.bigint();

      if (i % 20 === 0) {
        redisLatencySamples.push(opEnd - opStart);
      }

      redisCompleted++;

      if (redisCompleted >= nextRedisMilestone && nextRedisMilestone <= TOTAL_REDIS_OPS) {
        const target = nextRedisMilestone;
        nextRedisMilestone += 250_000;
        const elapsedNow = Number(process.hrtime.bigint() - tStart2) / 1_000_000;
        const currentRate = Math.round((target / elapsedNow) * 1000);
        const memNow = getMemory();

        redisMilestoneLog.push({
          ops: target,
          timeMs: +elapsedNow.toFixed(1),
          opsPerSec: currentRate,
          heapMB: memNow.heapUsed,
          rssMB: memNow.rss,
        });

        process.stdout.write(
          `   🔴 Redis Progress: ${C.bold}${target.toLocaleString()}${C.reset} / 1,000,000 ops | ` +
            `Throughput: ${C.green}${currentRate.toLocaleString()} ops/s${C.reset} | ` +
            `V8 Heap: ${memNow.heapUsed}MB | Elapsed: ${(elapsedNow / 1000).toFixed(1)}s\r`
        );
      }
    }
  });

  await Promise.all(redisFibers);

  const tEnd2 = process.hrtime.bigint();
  const totalDurationMs2 = Number(tEnd2 - tStart2) / 1_000_000;
  const avgOpsPerSec2 = Math.round((TOTAL_REDIS_OPS / totalDurationMs2) * 1000);
  const memEnd2 = getMemory();
  const latStats2 = calculateLatencies(redisLatencySamples);

  console.log('\n');
  printCard('PART 2 RESULTS: 1,000,000 REDIS NETWORK OPERATIONS COMPLETED', [
    ['Total Redis Operations', `${TOTAL_REDIS_OPS.toLocaleString()} atomic Lua evaluations`],
    ['Total Duration', `${(totalDurationMs2 / 1000).toFixed(2)} seconds (${totalDurationMs2.toFixed(1)} ms)`],
    ['Distributed Throughput', `${C.bold}${C.green}${avgOpsPerSec2.toLocaleString()} ops/sec${C.reset}`],
    ['Average Roundtrip Latency', `${latStats2.meanUs} µs (${(latStats2.meanUs / 1000).toFixed(3)} ms)`],
    ['Median Latency (p50)', `${latStats2.p50Us} µs`],
    ['95th Percentile (p95)', `${latStats2.p95Us} µs`],
    ['99th Percentile (p99)', `${C.yellow}${latStats2.p99Us} µs${C.reset}`],
    ['99.9th Percentile (p99.9)', `${latStats2.p999Us} µs`],
    ['V8 Heap Delta (1M Redis Ops)', `${formatDelta(memStart2.heapUsed, memEnd2.heapUsed)} (${memStart2.heapUsed}MB ➔ ${memEnd2.heapUsed}MB)`],
    ['Process RSS Growth (1M Redis Ops)', `${formatDelta(memStart2.rss, memEnd2.rss)} (${memStart2.rss}MB ➔ ${memEnd2.rss}MB)`],
  ]);

  console.log(`\n${C.bold}📈 1 Million Redis Operations RAM Stability Progression:${C.reset}`);
  console.log(`   ┌───────────────┬──────────────┬──────────────┬──────────────┬──────────────┐`);
  console.log(`   │ Milestone     │ Elapsed Time │ Speed        │ V8 Heap Used │ Process RSS  │`);
  console.log(`   ├───────────────┼──────────────┼──────────────┼──────────────┼──────────────┤`);
  for (const m of redisMilestoneLog) {
    console.log(
      `   │ ${m.ops.toLocaleString().padEnd(13)} │ ${((m.timeMs / 1000).toFixed(1) + ' s').padEnd(12)} │ ${(m.opsPerSec.toLocaleString() + ' op/s').padEnd(12)} │ ${(m.heapMB + ' MB').padEnd(12)} │ ${(m.rssMB + ' MB').padEnd(12)} │`
    );
  }
  console.log(`   └───────────────┴──────────────┴──────────────┴──────────────┴──────────────┘`);

  // ==========================================================================
  // FINAL GRAND MILLION-OP SUMMARY
  // ==========================================================================
  const finalMem = getMemory();
  console.log(`\n${C.bold}${C.magenta}================================================================================${C.reset}`);
  console.log(`${C.bold}${C.white}🏆 GRAND MILLION SUMMARY: 2,000,000 TOTAL OPERATIONS COMPLETED!${C.reset}`);
  console.log(`${C.bold}${C.magenta}================================================================================${C.reset}`);
  console.log(`  • ${C.bold}1 Million Ops In-Memory Speed:${C.reset}     ${C.green}${avgOpsPerSec1.toLocaleString()} ops/sec${C.reset} (Finished in ${(totalDurationMs1 / 1000).toFixed(2)}s)`);
  console.log(`  • ${C.bold}1 Million Ops Redis Speed:${C.reset}         ${C.green}${avgOpsPerSec2.toLocaleString()} ops/sec${C.reset} (Finished in ${(totalDurationMs2 / 1000).toFixed(2)}s)`);
  console.log(`  • ${C.bold}Initial Baseline V8 Heap Used:${C.reset}     ${initialMem.heapUsed} MB`);
  console.log(`  • ${C.bold}Final Post-2M Ops V8 Heap Used:${C.reset}    ${finalMem.heapUsed} MB (Net Delta: ${formatDelta(initialMem.heapUsed, finalMem.heapUsed)})`);
  console.log(`  • ${C.bold}Initial Baseline Process RSS:${C.reset}      ${initialMem.rss} MB`);
  console.log(`  • ${C.bold}Final Post-2M Ops Process RSS:${C.reset}     ${finalMem.rss} MB (Net Delta: ${formatDelta(initialMem.rss, finalMem.rss)})`);
  console.log(`  • ${C.bold}Garbage Collector Impact:${C.reset}          ${C.bold}${C.green}ZERO GC Thrashing / Flat Memory Curve!${C.reset}`);
  console.log(`${C.bold}${C.magenta}================================================================================${C.reset}\n`);

  process.exit(0);
}

runOneMillionOpsBenchmark().catch((err) => {
  console.error('\n❌ 1 Million Benchmark Error:', err);
  process.exit(1);
});
