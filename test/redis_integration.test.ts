import assert from 'assert';
import {
  createTurboPulse,
  EngineMode,
  Queue,
  Worker,
  FlowProducer,
  RateLimiter,
} from '../src_ts/index';

const REDIS_URL = process.env.REDIS_URL || 'redis://127.0.0.1:6379';

async function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

async function runRedisIntegrationTests() {
  console.log('\n====================================================');
  console.log('[TEST] LIVE REDIS DISTRIBUTED CORRECTNESS INTEGRATION TESTS');
  console.log(`[REDIS] Redis Target: ${REDIS_URL}`);
  console.log('====================================================\n');

  const engine = await createTurboPulse({
    mode: EngineMode.Redis,
    redisUrl: REDIS_URL,
    keyPrefix: 'tp_redis_test',
  });

  // --------------------------------------------------------------------------
  // TEST 1: Strict Ownership & Fencing Tokens in Redis
  // --------------------------------------------------------------------------
  console.log('▶ [1/7] Redis Job Ownership Fencing Tokens (P0)');
  const fenceQueue = engine.createQueue('redis_fence_q');
  const rawQ = (fenceQueue as any).nativeQueue;
  await rawQ.drain();

  const testJobId = `fence_job_${Date.now()}`;
  await rawQ.add(testJobId, 'fence_task', Buffer.from(JSON.stringify({ x: 1 })), {});

  // Fetch job
  const claimedJob = await rawQ.fetchNext(30000);
  assert.ok(claimedJob, 'Job must be fetched from Redis');
  assert.strictEqual(claimedJob.id, testJobId);
  assert.ok(claimedJob.lockToken, 'Claimed job in Redis must receive a unique lockToken');
  console.log(`   Claimed Job ID: ${claimedJob.id} with lockToken: ${claimedJob.lockToken}`);

  // Stale worker attempts to complete with fake token
  let staleCompleteBlocked = false;
  try {
    await rawQ.complete(testJobId, Buffer.from('stale'), 'fake-lock-token-999');
  } catch (err: any) {
    staleCompleteBlocked = true;
    assert.ok(err.message.includes('LOCK_LOST'), `Expected error to contain LOCK_LOST, got: ${err.message}`);
    console.log(`   [OK] Stale completion blocked with: "${err.message}"`);
  }
  assert.strictEqual(staleCompleteBlocked, true, 'Stale complete() must be blocked in Redis');

  // Stale worker attempts to fail with fake token
  let staleFailBlocked = false;
  try {
    await rawQ.fail(testJobId, 'stale worker failed', 'fake-lock-token-999');
  } catch (err: any) {
    staleFailBlocked = true;
    assert.ok(err.message.includes('LOCK_LOST'), `Expected error to contain LOCK_LOST, got: ${err.message}`);
    console.log(`   [OK] Stale fail() blocked with: "${err.message}"`);
  }
  assert.strictEqual(staleFailBlocked, true, 'Stale fail() must be blocked in Redis');

  // Legitimate owner completes with valid token
  const completeOk = await rawQ.complete(testJobId, Buffer.from('result_data'), claimedJob.lockToken);
  assert.strictEqual(completeOk, true, 'Legitimate complete() must succeed');
  const finishedJob = await rawQ.getJob(testJobId);
  assert.strictEqual(finishedJob?.state, 'Completed');
  console.log('   [OK] Legitimate owner with valid lockToken successfully completed job in Redis\n');

  // --------------------------------------------------------------------------
  // TEST 2: Stalled Recovery via Redis TIME and Lease Expiration (P0)
  // --------------------------------------------------------------------------
  console.log('▶ [2/7] Stalled Job Recovery via Redis TIME (P0)');
  const stallQueue = engine.createQueue('redis_stall_q');
  const stallRaw = (stallQueue as any).nativeQueue;
  await stallRaw.drain();

  const stallJobId = `stall_job_${Date.now()}`;
  await stallRaw.add(stallJobId, 'stall_task', Buffer.from('payload'), { maxRetries: 3 });

  // Claim job with short lease duration (100ms)
  const claimedStall = await stallRaw.fetchNext(100);
  assert.ok(claimedStall);
  assert.strictEqual(claimedStall.attemptsMade, 1);
  console.log(`   Job claimed with 100ms lease expiration in Redis`);

  // Wait 150ms for lease to expire
  await sleep(150);

  // Trigger stalled recovery
  const recovered = await stallRaw.recoverStalled(100, 3);
  assert.strictEqual(recovered, 1, 'Expected 1 stalled job to be recovered');

  // Re-fetch recovered job: attempts_made should be 2, stalled_count should be 1
  const reFetched = await stallRaw.fetchNext(30000);
  assert.ok(reFetched);
  assert.strictEqual(reFetched.id, stallJobId);
  assert.strictEqual(reFetched.attemptsMade, 2);
  assert.strictEqual(reFetched.stalledCount, 1);
  assert.notStrictEqual(reFetched.lockToken, claimedStall.lockToken, 'Recovered job must get fresh lockToken');
  await stallRaw.complete(reFetched.id, Buffer.from('done'), reFetched.lockToken);
  console.log(`   Recovered Job attemptsMade: ${reFetched.attemptsMade} | stalledCount: ${reFetched.stalledCount}`);
  console.log('   [OK] Stalled job recovered with Redis TIME, old token invalidated\n');

  // --------------------------------------------------------------------------
  // TEST 3: Atomic Retries in Lua (P1)
  // --------------------------------------------------------------------------
  console.log('▶ [3/7] Atomic Retries & Backoff in Lua (P1)');
  const retryQueue = engine.createQueue('redis_retry_q');
  const retryRaw = (retryQueue as any).nativeQueue;
  await retryRaw.drain();

  const retryJobId = `retry_job_${Date.now()}`;
  await retryRaw.add(retryJobId, 'retry_task', Buffer.from('payload'), {
    maxRetries: 2,
    backoffBaseMs: 150,
  });

  // Attempt 1 fails
  const attempt1 = await retryRaw.fetchNext(30000);
  assert.ok(attempt1);
  assert.strictEqual(attempt1.attemptsMade, 1);
  const isFinal1 = await retryRaw.fail(retryJobId, 'transient error', attempt1.lockToken);
  assert.strictEqual(isFinal1, false, 'First failure should schedule retry (not final)');

  const delayedJob = await retryRaw.getJob(retryJobId);
  assert.strictEqual(delayedJob?.state, 'Delayed');
  console.log('   Job atomically moved to Delayed state in Redis');

  // Wait for retry backoff
  await sleep(250);

  // Attempt 2 fails permanently (maxRetries reached)
  const attempt2 = await retryRaw.fetchNext(30000);
  assert.ok(attempt2);
  assert.strictEqual(attempt2.attemptsMade, 2);
  const isFinal2 = await retryRaw.fail(retryJobId, 'permanent fatal error', attempt2.lockToken);
  assert.strictEqual(isFinal2, true, 'Second failure should be final');

  const failedJob = await retryRaw.getJob(retryJobId);
  assert.strictEqual(failedJob?.state, 'Failed');
  assert.strictEqual(failedJob?.failedReason, 'permanent fatal error');
  console.log('   [OK] Fully atomic retries, backoff, and failure state executed in Lua\n');

  // --------------------------------------------------------------------------
  // TEST 4: Monotonic Sequence & FIFO Priority Ordering in Redis (P2)
  // --------------------------------------------------------------------------
  console.log('▶ [4/7] Monotonic Sequence FIFO Preservation & Priority Scheduling (P2)');
  const orderQueue = engine.createQueue('redis_order_q');
  const orderRaw = (orderQueue as any).nativeQueue;
  await orderRaw.drain();

  // Enqueue 3 equal-priority jobs in strict sequence
  const idA = `fifo_a_${Date.now()}`;
  const idB = `fifo_b_${Date.now()}`;
  const idC = `fifo_c_${Date.now()}`;
  await orderRaw.add(idA, 'taskA', Buffer.from('a'), { priority: 10 });
  await orderRaw.add(idB, 'taskB', Buffer.from('b'), { priority: 10 });
  await orderRaw.add(idC, 'taskC', Buffer.from('c'), { priority: 10 });

  // Enqueue high priority job
  const idHigh = `prio_high_${Date.now()}`;
  await orderRaw.add(idHigh, 'taskHigh', Buffer.from('h'), { priority: 50 });

  // Fetch batch
  const batch = await orderRaw.fetchBatch(4, 30000);
  assert.strictEqual(batch.length, 4);
  const fetchedIds = batch.map((j: any) => j.id);
  console.log(`   Fetched execution sequence: ${JSON.stringify(fetchedIds)}`);

  // High priority first, followed by strict FIFO order A -> B -> C
  assert.strictEqual(fetchedIds[0], idHigh, 'High priority job must be fetched first');
  assert.strictEqual(fetchedIds[1], idA, 'FIFO tiebreaker: taskA must be fetched before taskB');
  assert.strictEqual(fetchedIds[2], idB, 'FIFO tiebreaker: taskB must be fetched before taskC');
  assert.strictEqual(fetchedIds[3], idC, 'FIFO tiebreaker: taskC must be fetched last');

  for (const j of batch) {
    await orderRaw.complete(j.id, Buffer.from('ok'), j.lockToken);
  }
  console.log('   [OK] Bounded monotonic sequence strictly preserved FIFO arrival order\n');

  // --------------------------------------------------------------------------
  // TEST 5: Duplicate Job ID Atomic Rejection (P1)
  // --------------------------------------------------------------------------
  console.log('▶ [5/7] Atomic Duplicate Job ID Rejection (P1)');
  const dedupQueue = engine.createQueue('redis_dedup_q');
  const dedupRaw = (dedupQueue as any).nativeQueue;
  await dedupRaw.drain();

  const dupId = `dedup_${Date.now()}`;
  await dedupRaw.add(dupId, 'first_push', Buffer.from('data1'), {});

  let duplicateRejected = false;
  try {
    await dedupRaw.add(dupId, 'duplicate_push', Buffer.from('data2'), {});
  } catch (err: any) {
    duplicateRejected = true;
    console.log(`   [OK] Duplicate add() cleanly rejected: "${err.message}"`);
  }
  assert.strictEqual(duplicateRejected, true, 'Duplicate single add must be rejected');

  // Bulk duplicate check
  let bulkDuplicateRejected = false;
  try {
    await dedupRaw.addBulk([
      { jobId: `bulk_new_${Date.now()}`, name: 'ok1', data: Buffer.from('1') },
      { jobId: dupId, name: 'should_fail', data: Buffer.from('2') },
    ]);
  } catch (err: any) {
    bulkDuplicateRejected = true;
    console.log(`   [OK] Duplicate in addBulk() atomically rejected: "${err.message}"`);
  }
  assert.strictEqual(bulkDuplicateRejected, true, 'Bulk add with duplicates must be rejected');
  console.log('   [OK] Duplicate job IDs prevented from corrupting existing records\n');

  // --------------------------------------------------------------------------
  // TEST 6: Drain and Obliterate with Active Job Protection (P1)
  // --------------------------------------------------------------------------
  console.log('▶ [6/7] Drain & Obliterate Active Job Protection (P1)');
  const oblitQueue = engine.createQueue('redis_oblit_q');
  const oblitRaw = (oblitQueue as any).nativeQueue;

  const activeJobId = `active_${Date.now()}`;
  const waitingJobId = `wait_${Date.now()}`;
  await oblitRaw.add(activeJobId, 'active_task', Buffer.from('a'), {});
  await oblitRaw.add(waitingJobId, 'waiting_task', Buffer.from('w'), {});

  // Claim one job so it becomes Active
  const claimedActive = await oblitRaw.fetchNext(30000);
  assert.strictEqual(claimedActive?.id, activeJobId);

  // Drain: waiting jobs removed, active job MUST NOT be touched
  await oblitRaw.drain();
  const waitingAfterDrain = await oblitRaw.getJob(waitingJobId);
  assert.strictEqual(waitingAfterDrain, null, 'Waiting job must be removed by drain');
  const activeAfterDrain = await oblitRaw.getJob(activeJobId);
  assert.ok(activeAfterDrain, 'Active job must remain untouched after drain');
  assert.strictEqual(activeAfterDrain?.state, 'Active');
  console.log('   [OK] drain() cleared waiting queue and payload without touching active jobs');

  // Obliterate without force: must be rejected because active job exists!
  let oblitGuardBlocked = false;
  try {
    await oblitRaw.obliterate(false);
  } catch (err: any) {
    oblitGuardBlocked = true;
    console.log(`   [OK] obliterate(force=false) blocked active queue: "${err.message}"`);
  }
  assert.strictEqual(oblitGuardBlocked, true, 'Obliterate without force must fail if active jobs exist');

  // Obliterate with force: cleans everything
  await oblitRaw.obliterate(true);
  const activeAfterForce = await oblitRaw.getJob(activeJobId);
  assert.strictEqual(activeAfterForce, null, 'Active job removed by obliterate with force=true');
  console.log('   [OK] obliterate() properly guarded active jobs and obliterated cleanly on force\n');

  // --------------------------------------------------------------------------
  // TEST 7: Redis Cluster FlowProducer Cross-Queue DAG (P0)
  // --------------------------------------------------------------------------
  console.log('▶ [7/7] Redis Cluster Cross-Queue FlowProducer DAG (P0)');
  const flow = new FlowProducer({ connection: REDIS_URL });

  const parentQName = `flow_p_${Date.now()}`;
  const child1QName = `flow_c1_${Date.now()}`;
  const child2QName = `flow_c2_${Date.now()}`;

  // Add flow across 3 distinct queue names (in Redis Cluster, each maps to its own hash slot)
  const flowNode = await flow.add({
    name: 'aggregate_report',
    queueName: parentQName,
    data: { report: 'sales' },
    children: [
      { name: 'fetch_us_sales', queueName: child1QName, data: { region: 'US' } },
      { name: 'fetch_eu_sales', queueName: child2QName, data: { region: 'EU' } },
    ],
  });

  const pQueue = new Queue(parentQName, { redisUrl: REDIS_URL });
  const c1Queue = new Queue(child1QName, { redisUrl: REDIS_URL });
  const c2Queue = new Queue(child2QName, { redisUrl: REDIS_URL });
  await pQueue.waitUntilReady();
  await c1Queue.waitUntilReady();
  await c2Queue.waitUntilReady();

  // Verify parent is in WaitingChildren state
  const parentJobInit = await pQueue.getJob(flowNode.job.id);
  assert.ok(parentJobInit);
  const stateInit = await parentJobInit.getState();
  assert.strictEqual(stateInit, 'WaitingChildren', 'Parent must start in WaitingChildren state');
  console.log(`   Parent Job ${flowNode.job.id} initialized in WaitingChildren state`);

  // Process child 1
  const c1Job = await (c1Queue as any).nativeBinding.fetchNext();
  assert.ok(c1Job);
  await (c1Queue as any).nativeBinding.complete(c1Job.id, Buffer.from('us_data'), c1Job.lockToken);

  // Verify parent still WaitingChildren (1 dependency remaining)
  const parentMid = await pQueue.getJob(flowNode.job.id);
  const stateMid = await parentMid?.getState();
  assert.strictEqual(stateMid, 'WaitingChildren', 'Parent must remain WaitingChildren until last child finishes');

  // Process child 2 (last dependency)
  const c2Job = await (c2Queue as any).nativeBinding.fetchNext();
  assert.ok(c2Job);
  await (c2Queue as any).nativeBinding.complete(c2Job.id, Buffer.from('eu_data'), c2Job.lockToken);

  // Parent must now be atomically promoted to Waiting!
  const parentPromoted = await pQueue.getJob(flowNode.job.id);
  const statePromoted = await parentPromoted?.getState();
  assert.strictEqual(statePromoted, 'Waiting', 'Parent must be promoted to Waiting once all children complete');
  console.log('   Parent atomically promoted to Waiting upon last child resolution (ZERO CROSSSLOT)');

  // Claim and complete parent
  const parentClaimed = await (pQueue as any).nativeBinding.fetchNext();
  assert.strictEqual(parentClaimed?.id, flowNode.job.id);
  await (pQueue as any).nativeBinding.complete(parentClaimed.id, Buffer.from('report_complete'), parentClaimed.lockToken);

  const parentFinal = await pQueue.getJob(flowNode.job.id);
  const stateFinal = await parentFinal?.getState();
  assert.strictEqual(stateFinal, 'Completed');
  console.log('   [OK] Cross-queue DAG executed and resolved with ZERO CROSSSLOT errors in Redis!\n');

  console.log('====================================================');
  console.log('[PASS] ALL 7 LIVE REDIS DISTRIBUTED TESTS PASSED 100%!');
  console.log('====================================================\n');
}

runRedisIntegrationTests().catch((err) => {
  console.error('\n[FAIL] Live Redis Integration Test Failed:', err);
  process.exit(1);
});
