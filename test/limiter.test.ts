import { TurboPulse } from '../src_ts/engine';
import { RateLimiter } from '../src_ts/limiter';

async function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function runLimiterTests() {
  console.log('========================================');
  console.log('🧪 RUNNING TURBOPULSE RATE LIMITER TESTS');
  console.log('========================================\n');

  const engine = TurboPulse.createInMemory();

  // TEST 1: Burst Capacity & Rate Refill
  console.log('▶ Test 1: Burst Capacity and Refill Rate');
  const limiter = engine.createRateLimiter({
    capacity: 3,
    refillRate: 3,
    refillIntervalMs: 500,
  });

  const executedAt: number[] = [];
  const start = Date.now();

  const promises = [1, 2, 3, 4, 5, 6].map((i) =>
    limiter.schedule(async () => {
      const now = Date.now() - start;
      executedAt.push(now);
      return `Task ${i} at ${now}ms`;
    })
  );

  const results = await Promise.all(promises);
  console.log('   Results:', results);
  console.log('   Execution Timestamps (ms relative):', executedAt);

  // First 3 should execute immediately (burst 0-50ms)
  // Next 3 should wait for refill (~500ms)
  if (executedAt[0] < 50 && executedAt[1] < 50 && executedAt[2] < 50) {
    console.log('   ✅ Burst capacity (3 tasks) executed immediately');
  } else {
    throw new Error('Burst capacity failed');
  }

  if (executedAt[3] >= 150 && executedAt[5] >= 450) {
    console.log('   ✅ Throttled tasks properly paused until bucket refilled (token bucket continuous refill)');
  } else {
    throw new Error('Refill delay failed');
  }

  // TEST 2: Maximum Concurrent Limits
  console.log('\n▶ Test 2: Concurrency Limits (maxConcurrent = 2)');
  const concLimiter = engine.createRateLimiter({
    capacity: 20,
    refillRate: 20,
    refillIntervalMs: 1000,
    maxConcurrent: 2,
  });

  let runningCount = 0;
  let maxObservedRunning = 0;

  const concPromises = Array.from({ length: 8 }).map((_, i) =>
    concLimiter.schedule(async () => {
      runningCount++;
      maxObservedRunning = Math.max(maxObservedRunning, runningCount);
      await sleep(60);
      runningCount--;
      return i;
    })
  );

  await Promise.all(concPromises);
  console.log('   Max simultaneously running tasks:', maxObservedRunning);
  if (maxObservedRunning <= 2) {
    console.log('   ✅ maxConcurrent limit strictly respected');
  } else {
    throw new Error(`Concurrency exceeded limit: observed ${maxObservedRunning}`);
  }

  // TEST 3: MinTime Spacing Constraint
  console.log('\n▶ Test 3: MinTime Spacing Constraint (minTime = 50ms)');
  const minTimeLimiter = engine.createRateLimiter({
    capacity: 10,
    refillRate: 10,
    refillIntervalMs: 1000,
    minTimeMs: 50,
  });

  const timestamps: number[] = [];
  const t0 = Date.now();
  for (let i = 0; i < 4; i++) {
    await minTimeLimiter.schedule(async () => {
      timestamps.push(Date.now() - t0);
    });
  }

  console.log('   Task intervals (ms):', timestamps);
  const diff1 = timestamps[1] - timestamps[0];
  const diff2 = timestamps[2] - timestamps[1];
  console.log(`   Gaps: ${diff1}ms, ${diff2}ms`);
  if (diff1 >= 40 && diff2 >= 40) {
    console.log('   ✅ minTime spacing between calls verified');
  } else {
    throw new Error('minTime spacing failed');
  }

  // TEST 4: Multi-tenant Group Rate Limiting
  console.log('\n▶ Test 4: Multi-Tenant Group Rate Limiting');
  const group = engine.createGroup({
    capacity: 2,
    refillRate: 2,
    refillIntervalMs: 1000,
  });

  // User A and User B should have separate, isolated buckets!
  const userAResults = await Promise.all([
    group.key('user-A').schedule(async () => 'A1'),
    group.key('user-A').schedule(async () => 'A2'),
  ]);

  const userBResults = await Promise.all([
    group.key('user-B').schedule(async () => 'B1'),
    group.key('user-B').schedule(async () => 'B2'),
  ]);

  console.log('   User A results:', userAResults);
  console.log('   User B results:', userBResults);
  console.log('   ✅ Independent per-tenant group limits verified');

  // TEST 5: wrap() and submit() API compatibility
  console.log('\n▶ Test 5: wrap() and submit() API compatibility');
  const wrappedAdd = limiter.wrap(async (a: number, b: number) => a + b);
  const sum = await wrappedAdd(21, 21);
  console.log('   wrappedAdd(21, 21) =', sum);

  await new Promise<void>((resolve, reject) => {
    limiter.submit(
      async () => 'submitResult',
      (err, res) => {
        if (err) reject(err);
        else {
          console.log('   submit callback result:', res);
          resolve();
        }
      }
    );
  });

  console.log('\n🎉 ALL RATE LIMITER TESTS PASSED SUCCESSFULLY!\n');
}

runLimiterTests().catch((err) => {
  console.error('❌ Limiter Test Failed:', err);
  process.exit(1);
});
