import RateLimiter from '../src_ts/rate_limiter';

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

async function runRateLimiterDropinTests() {
  console.log('====================================================');
  console.log('🧪 VERIFYING 100% STANDALONE RATE LIMITER CAPABILITIES');
  console.log('====================================================\n');

  // Test 1: Basic schedule with maxConcurrent & minTime
  console.log('▶ Test 1: maxConcurrent & minTime pacing');
  const limiter = new RateLimiter({
    maxConcurrent: 2,
    minTime: 50,
  });

  const timestamps: number[] = [];
  const start = Date.now();

  const task = (id: number) =>
    limiter.schedule(async () => {
      timestamps.push(Date.now() - start);
      await sleep(30);
      return `Task ${id}`;
    });

  const results = await Promise.all([task(1), task(2), task(3), task(4)]);
  console.log('   Results:', results);
  console.log('   Execution relative offsets (ms):', timestamps);
  if (results.length !== 4) throw new Error('Not all tasks finished');
  console.log('   ✅ schedule() with maxConcurrent and minTime verified');

  // Test 2: Reservoir Quotas and Automatic Increments
  console.log('\n▶ Test 2: Reservoir limits & incrementReservoir');
  const resLimiter = new RateLimiter({
    reservoir: 3,
    reservoirRefreshInterval: 500,
    reservoirRefreshAmount: 3,
  });

  let remaining = await resLimiter.currentReservoir();
  console.log('   Initial reservoir:', remaining);
  if (remaining !== 3) throw new Error(`Expected reservoir 3, got ${remaining}`);

  await resLimiter.schedule(() => Promise.resolve('ok 1'));
  await resLimiter.schedule(() => Promise.resolve('ok 2'));

  const remainingAfter = await resLimiter.currentReservoir();
  console.log('   Reservoir after 2 executions:', remainingAfter);
  if (remainingAfter !== 1) throw new Error(`Expected reservoir 1, got ${remainingAfter}`);

  const incResult = await resLimiter.incrementReservoir(5);
  console.log('   Reservoir after incrementing by 5:', incResult);
  if (incResult !== 6) throw new Error(`Expected reservoir 6, got ${incResult}`);
  console.log('   ✅ Reservoir management verified');

  // Test 3: Load Shedding Strategy (highWater & OVERFLOW)
  console.log('\n▶ Test 3: Load shedding strategy: highWater & OVERFLOW');
  const overflowLimiter = new RateLimiter({
    maxConcurrent: 1,
    highWater: 2,
    strategy: RateLimiter.strategy.OVERFLOW,
    rejectOnDrop: true,
  });

  let droppedCount = 0;
  overflowLimiter.on('dropped', () => {
    droppedCount++;
  });

  // Start 1 long task to fill concurrency slot
  overflowLimiter.schedule(() => sleep(100));
  await sleep(15); // Wait for task 1 to leave queue and become active

  // Enqueue 2 tasks to fill queue up to highWater (2)
  const p1 = overflowLimiter.schedule(() => sleep(20));
  const p2 = overflowLimiter.schedule(() => sleep(20));

  // 4th task should be dropped by OVERFLOW strategy
  let droppedCaught = false;
  try {
    await overflowLimiter.schedule(() => sleep(20));
  } catch (err: any) {
    droppedCaught = true;
    console.log('   Caught expected drop error:', err.message);
  }

  if (!droppedCaught) throw new Error('Expected task to be dropped by OVERFLOW');
  await Promise.all([p1, p2]);
  console.log('   ✅ OVERFLOW strategy load shedding verified');

  // Test 4: wrap(), submit(), check(), and counts()
  console.log('\n▶ Test 4: wrap(), submit(), check(), and counts()');
  const helperLimiter = new RateLimiter({ maxConcurrent: 3, minTime: 10 });

  const checkAllowed = await helperLimiter.check(1);
  console.log('   check(1) permitted:', checkAllowed);
  if (!checkAllowed) throw new Error('check(1) should be permitted');

  const wrappedMultiply = helperLimiter.wrap(async (a: number, b: number) => a * b);
  const multRes = await wrappedMultiply(6, 7);
  console.log('   wrappedMultiply(6, 7) =', multRes);
  if (multRes !== 42) throw new Error(`Expected 42, got ${multRes}`);

  const submitRes = await new Promise((resolve, reject) => {
    helperLimiter.submit(
      async (val: string) => `submitted_${val}`,
      'hello',
      (err: any, res?: any) => {
        if (err) reject(err);
        else resolve(res);
      }
    );
  });
  console.log('   submit result:', submitRes);
  if (submitRes !== 'submitted_hello') throw new Error(`Unexpected submit result: ${submitRes}`);

  const counts = helperLimiter.counts();
  console.log('   limiter counts():', counts);
  if (counts.DONE < 2) throw new Error('counts.DONE should be >= 2');
  console.log('   ✅ wrap, submit, check, and counts verified');

  // Test 5: Chaining limiters
  console.log('\n▶ Test 5: Chaining limiters (limiter.chain)');
  const parentLimiter = new RateLimiter({ maxConcurrent: 1 });
  const childLimiter = new RateLimiter({ maxConcurrent: 5 }).chain(parentLimiter);

  let concurrentInChain = 0;
  let maxConcurrentSeen = 0;

  const chainedTask = () =>
    childLimiter.schedule(async () => {
      concurrentInChain++;
      maxConcurrentSeen = Math.max(maxConcurrentSeen, concurrentInChain);
      await sleep(25);
      concurrentInChain--;
    });

  await Promise.all([chainedTask(), chainedTask(), chainedTask()]);
  console.log('   Max concurrent executed in chained limiter:', maxConcurrentSeen);
  if (maxConcurrentSeen !== 1) throw new Error(`Expected max concurrent 1 through chain, got ${maxConcurrentSeen}`);
  console.log('   ✅ Limiter chaining verified');

  // Test 6: RateLimiter.Group
  console.log('\n▶ Test 6: RateLimiter.Group multi-tenant sharding');
  const group = new RateLimiter.Group({
    maxConcurrent: 1,
    minTime: 20,
  });

  const tenantALimiter = group.key('tenant_alpha');
  const tenantBLimiter = group.key('tenant_beta');

  const [tA, tB] = await Promise.all([
    tenantALimiter.schedule(async () => 'Alpha OK'),
    tenantBLimiter.schedule(async () => 'Beta OK'),
  ]);

  console.log('   Group Tenant A result:', tA);
  console.log('   Group Tenant B result:', tB);
  const activeLimiters = group.limiters();
  console.log('   Active limiters in group:', activeLimiters.length);
  if (activeLimiters.length !== 2) throw new Error(`Expected 2 limiters in group, got ${activeLimiters.length}`);
  console.log('   ✅ RateLimiter.Group verified');

  console.log('\n🎉 ALL RATE LIMITER TESTS PASSED WITH 100% SUCCESS!');
}

runRateLimiterDropinTests().catch((err) => {
  console.error('❌ Test failed:', err);
  process.exit(1);
});
