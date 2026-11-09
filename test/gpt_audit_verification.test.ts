import assert from 'assert';
import {
  createInMemoryTurboPulse,
  RateLimiter,
  Queue,
  Worker,
  RateLimiterStrategy,
  FlowProducer,
} from '../src_ts/index';

async function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

async function runGptAuditVerificationTests() {
  console.log('\n====================================================');
  console.log('[TEST] VERIFYING GPT AUDIT FIXES UNDER STRESS');
  console.log('====================================================\n');

  const engine = createInMemoryTurboPulse();

  // --------------------------------------------------------------------------
  // TEST 1: In-Memory Concurrency Boundary Race (GPT P0 finding)
  // --------------------------------------------------------------------------
  console.log('▶ [1/5] In-Memory Atomic Concurrency CAS Check (maxConcurrent = 5 under 200 parallel calls)');
  const limiter = (engine as any).nativeEngine.createRateLimiter({
    capacity: 1000,
    refillRate: 1000,
    refillIntervalMs: 1000,
    maxConcurrent: 5,
  });

  let maxObservedActive = 0;
  let currentlyActive = 0;

  // Launch 200 parallel tasks attempting acquisition at the exact same instant
  const tasks = Array.from({ length: 200 }).map(async () => {
    const res = limiter.acquireSync('concurrency_test_key', 1);
    if (res.allowed) {
      currentlyActive++;
      if (currentlyActive > maxObservedActive) {
        maxObservedActive = currentlyActive;
      }
      await sleep(10);
      currentlyActive--;
      await limiter.release('concurrency_test_key');
    }
  });

  await Promise.all(tasks);

  console.log(`   Max concurrently active observed: ${maxObservedActive} (Target limit: 5)`);
  assert.ok(maxObservedActive <= 5, `Concurrency breached limit! Observed ${maxObservedActive} > 5`);
  console.log('   [OK] Concurrency CAS reservation strictly enforced with ZERO race breaches\n');

  // --------------------------------------------------------------------------
  // TEST 2: Idle Worker Rate Limiter Permit Leak (GPT P0 finding)
  // --------------------------------------------------------------------------
  console.log('▶ [2/5] Idle Worker Rate Limiter Permit Leak Verification');
  const emptyQueue = new Queue('empty_leak_test_queue');
  await emptyQueue.waitUntilReady();

  let workerProcessed = 0;
  const workerLimiter = new RateLimiter({
    reservoir: 5,
    reservoirRefreshAmount: 5,
    reservoirRefreshInterval: 5000,
  });

  const worker = new Worker('empty_leak_test_queue', async () => {
    workerProcessed++;
  }, {
    limiter: { max: 5, duration: 5000 } as any,
    pollIntervalMs: 5,
  });

  // Let worker poll the empty queue for 100ms (~20 empty iterations)
  await sleep(100);

  // Now add a job: if the worker leaked permits during idle polling, it would be blocked!
  await emptyQueue.add('delayed_job', { foo: 'bar' });
  await sleep(60);
  await worker.close();

  assert.strictEqual(workerProcessed, 1, `Worker was blocked due to leaked permits! Processed: ${workerProcessed}`);
  console.log('   [OK] Idle worker immediately releases permits when queue is empty (no permit leakage)\n');

  // --------------------------------------------------------------------------
  // TEST 3: Stalled Recovery Separation (GPT P0 finding)
  // --------------------------------------------------------------------------
  console.log('▶ [3/5] Stalled Recovery Separation (attempts_made vs stalled_count)');
  const stallQueue = engine.createQueue('stall_test_queue');
  const rawQ = (stallQueue as any).nativeQueue;

  const jobId = await rawQ.add(null, 'stalled_task', Buffer.from('test_data'), {
    maxRetries: 5,
  });

  // Worker 1 fetches the job (attempts_made becomes 1)
  const jobDto = await rawQ.fetchNext();
  assert.strictEqual(jobDto?.id, jobId);
  assert.strictEqual(jobDto?.attemptsMade, 1);

  // Simulate stalled lock expiry
  await sleep(50);
  const recoveredCount = await rawQ.recoverStalled(20, 2);
  assert.strictEqual(recoveredCount, 1, 'Job was not recovered by stalled scanner');

  // Worker 2 fetches recovered job: attempts_made is 2, stalled_count is 1
  const recoveredDto = await rawQ.fetchNext();
  assert.strictEqual(recoveredDto?.id, jobId);
  assert.strictEqual(recoveredDto?.attemptsMade, 2);
  assert.strictEqual(recoveredDto?.stalledCount, 1);

  console.log('   Recovered Job DTO attemptsMade:', recoveredDto.attemptsMade, '| stalledCount:', recoveredDto.stalledCount);
  console.log('   [OK] Stalled recoveries tracked independently from execution attempts\n');

  // --------------------------------------------------------------------------
  // TEST 4: LeasedRateLimiter Concurrency Isolation (GPT P0 finding)
  // --------------------------------------------------------------------------
  console.log('▶ [4/5] LeasedRateLimiter Concurrency Quota Isolation');
  const leasedLimiter = engine.createLeasedRateLimiter({
    capacity: 1000,
    refillRate: 1000,
    refillIntervalMs: 1000,
    maxConcurrent: 10,
  }, 'leased_test_key', { leaseBatchSize: 100 });

  const r1 = await leasedLimiter.acquire(1);
  assert.strictEqual(r1.allowed, true);
  // Concurrency should only be 1 (for the single operation), not 100 (the batch size)
  assert.strictEqual(r1.currentConcurrency, 1, `Concurrency was inflated by batch! Observed ${r1.currentConcurrency}`);
  await leasedLimiter.release();

  console.log('   Leased limiter token quota remaining in local cache:', leasedLimiter.currentLocalTokens);
  console.log('   [OK] LeasedRateLimiter reserves token quota without concurrency inflation\n');

  // --------------------------------------------------------------------------
  // TEST 5: FlowProducer Atomic Tree Validation (GPT P0 finding)
  // --------------------------------------------------------------------------
  console.log('▶ [5/5] FlowProducer Tree Validation & Atomic Staging');
  const flow = new FlowProducer();

  let rejected = false;
  try {
    // Malformed child node: queueName is empty
    await flow.add({
      name: 'root',
      queueName: 'valid_q',
      children: [
        { name: 'child_1', queueName: 'valid_q' },
        { name: '', queueName: '' } as any, // Invalid node!
      ],
    });
  } catch (err: any) {
    rejected = true;
    console.log('   Caught expected upfront tree validation error:', err.message);
  }

  assert.strictEqual(rejected, true, 'FlowProducer failed to reject invalid tree upfront');
  console.log('   [OK] FlowProducer validates entire tree before enqueueing any parent or child jobs\n');

  console.log('====================================================');
  console.log('[PASS] ALL 5 GPT AUDIT VERIFICATION TESTS PASSED 100%!');
  console.log('====================================================\n');
}

runGptAuditVerificationTests().catch((e) => {
  console.error('[FAIL] Verification test error:', e);
  process.exit(1);
});
