import { RateLimiter, RateLimiterGroup } from '../src_ts/rate_limiter';
import { Queue, Worker, QueueEvents, FlowProducer } from '../src_ts/distributed_queue';

function assertThrows(fn: () => any, expectedName: string, description: string): void {
  try {
    const res = fn();
    if (res && typeof res.then === 'function') {
      throw new Error(`Expected synchronous throw for "${description}", but returned a Promise.`);
    }
    throw new Error(`Failed: "${description}" did not throw as expected.`);
  } catch (err: any) {
    if (err.name !== expectedName && !err.message.includes('Expected')) {
      throw new Error(`Failed: "${description}" threw ${err.name} (${err.message}) instead of ${expectedName}`);
    }
    console.log(`   [OK] Correctly caught ${err.name}: "${err.message}"`);
  }
}

async function assertRejects(fn: () => Promise<any>, expectedName: string, description: string): Promise<void> {
  try {
    await fn();
    throw new Error(`Failed: "${description}" did not reject as expected.`);
  } catch (err: any) {
    if (err.name !== expectedName && !err.message.includes('Expected')) {
      throw new Error(`Failed: "${description}" rejected with ${err.name} (${err.message}) instead of ${expectedName}`);
    }
    console.log(`   [OK] Correctly caught rejection ${err.name}: "${err.message}"`);
  }
}

async function runValidationTests() {
  console.log('====================================================');
  console.log('[GUARD] TESTING USER-FACING INPUT VALIDATION & ERROR GUARDS');
  console.log('====================================================\n');

  // ----------------------------------------------------
  // SECTION 1: RateLimiterGroup input validation
  // ----------------------------------------------------
  console.log('▶ Section 1: RateLimiterGroup guards');
  const group = new RateLimiterGroup();

  assertThrows(() => (group as any).key(), 'TypeError', 'group.key() with no arguments');
  assertThrows(() => group.key(''), 'TypeError', 'group.key("") with empty string');
  assertThrows(() => (group as any).key(null), 'TypeError', 'group.key(null)');
  assertThrows(() => (group as any).key(1234), 'TypeError', 'group.key(1234)');

  await assertRejects(() => (group as any).deleteKey(''), 'TypeError', 'group.deleteKey("")');
  assertThrows(() => (group as any).updateSettings(null), 'TypeError', 'group.updateSettings(null)');

  await assertRejects(() => (group as any).schedule(''), 'TypeError', 'group.schedule("") with empty key');
  await assertRejects(() => (group as any).schedule('tenant1', null), 'TypeError', 'group.schedule(key, null)');
  assertThrows(() => (group as any).wrap('tenant1', null), 'TypeError', 'group.wrap(key, null)');
  await assertRejects(() => (group as any).getTicket(''), 'TypeError', 'group.getTicket("") with empty key');
  await assertRejects(() => (group as any).tryTicket('tenant1', -5), 'TypeError', 'group.tryTicket(key, -5)');

  // ----------------------------------------------------
  // SECTION 2: RateLimiter input validation
  // ----------------------------------------------------
  console.log('\n▶ Section 2: RateLimiter guards');
  assertThrows(() => new RateLimiter('invalid' as any), 'TypeError', 'new RateLimiter("invalid")');
  assertThrows(() => new RateLimiter({ minTime: -10 }), 'TypeError', 'new RateLimiter({ minTime: -10 })');
  assertThrows(() => new RateLimiter({ maxConcurrent: 0 }), 'TypeError', 'new RateLimiter({ maxConcurrent: 0 })');

  const limiter = new RateLimiter({ minTime: 10 });
  await limiter.waitUntilReady();

  await assertRejects(() => (limiter as any).schedule(), 'TypeError', 'limiter.schedule() with no arguments');
  await assertRejects(() => (limiter as any).schedule({ priority: 1 }), 'TypeError', 'limiter.schedule({ priority }) without fn');
  await assertRejects(() => (limiter as any).schedule(null), 'TypeError', 'limiter.schedule(null)');

  assertThrows(() => (limiter as any).wrap(null), 'TypeError', 'limiter.wrap(null)');
  assertThrows(() => (limiter as any).submit(), 'TypeError', 'limiter.submit() without callback');

  // Callback receives error if fn is invalid
  await new Promise<void>((resolve, reject) => {
    limiter.submit(null as any, (err: any) => {
      if (err instanceof TypeError) {
        console.log(`   [OK] limiter.submit callback received expected TypeError: "${err.message}"`);
        resolve();
      } else {
        reject(new Error(`limiter.submit callback did not receive TypeError: ${err}`));
      }
    });
  });

  await assertRejects(() => (limiter as any).getTicket(-1), 'TypeError', 'limiter.getTicket(-1)');
  await assertRejects(() => (limiter as any).tryTicket(0), 'TypeError', 'limiter.tryTicket(0)');
  await assertRejects(() => (limiter as any).check(-5), 'TypeError', 'limiter.check(-5)');
  await assertRejects(() => (limiter as any).incrementReservoir('five' as any), 'TypeError', 'limiter.incrementReservoir("five")');
  assertThrows(() => (limiter as any).chain({}), 'TypeError', 'limiter.chain({}) without schedule method');
  assertThrows(() => (limiter as any).updateSettings(null), 'TypeError', 'limiter.updateSettings(null)');

  // ----------------------------------------------------
  // SECTION 3: Queue input validation
  // ----------------------------------------------------
  console.log('\n▶ Section 3: Queue guards');
  assertThrows(() => new (Queue as any)(), 'TypeError', 'new Queue() with no arguments');
  assertThrows(() => new Queue(''), 'TypeError', 'new Queue("") with empty name');
  assertThrows(() => new (Queue as any)(null), 'TypeError', 'new Queue(null)');

  const queue = new Queue('validation-test-queue');
  await queue.waitUntilReady();

  await assertRejects(() => (queue as any).add(), 'TypeError', 'queue.add() with no arguments');
  await assertRejects(() => (queue as any).add('', { foo: 'bar' }), 'TypeError', 'queue.add("") with empty job name');
  await assertRejects(() => (queue as any).add(null as any, {}), 'TypeError', 'queue.add(null, {})');

  await assertRejects(() => (queue as any).addBulk(null), 'TypeError', 'queue.addBulk(null)');
  await assertRejects(() => (queue as any).addBulk([{ name: '' } as any]), 'TypeError', 'queue.addBulk([{ name: "" }])');
  await assertRejects(() => (queue as any).getJob(''), 'TypeError', 'queue.getJob("")');
  await assertRejects(() => (queue as any).clean(-100), 'TypeError', 'queue.clean(-100)');

  // ----------------------------------------------------
  // SECTION 4: Worker input validation
  // ----------------------------------------------------
  console.log('\n▶ Section 4: Worker guards');
  assertThrows(() => new (Worker as any)(), 'TypeError', 'new Worker() with no arguments');
  assertThrows(() => new (Worker as any)(''), 'TypeError', 'new Worker("") with empty queue name');
  assertThrows(() => new (Worker as any)('my-queue'), 'TypeError', 'new Worker("my-queue") with missing processor');
  assertThrows(() => new (Worker as any)('my-queue', 'not-a-func'), 'TypeError', 'new Worker("my-queue", "not-a-func")');
  assertThrows(
    () => new Worker('my-queue', async () => {}, { concurrency: 0, autoStart: false }),
    'TypeError',
    'new Worker with concurrency: 0'
  );

  // ----------------------------------------------------
  // SECTION 5: FlowProducer & QueueEvents input validation
  // ----------------------------------------------------
  console.log('\n▶ Section 5: FlowProducer and QueueEvents guards');
  const flow = new FlowProducer();
  await assertRejects(() => (flow as any).add(null), 'TypeError', 'flow.add(null)');
  await assertRejects(() => (flow as any).add({}), 'TypeError', 'flow.add({}) with missing queueName');
  await assertRejects(() => (flow as any).add({ queueName: 'q' }), 'TypeError', 'flow.add({ queueName }) with missing job name');

  assertThrows(() => new (QueueEvents as any)(), 'TypeError', 'new QueueEvents() with no arguments');
  assertThrows(() => new QueueEvents(''), 'TypeError', 'new QueueEvents("") with empty queue name');

  const qEvents = new QueueEvents('my-test-events');
  await assertRejects(() => (qEvents as any).waitForJob(''), 'TypeError', 'qEvents.waitForJob("") with empty jobId');
  await assertRejects(() => (qEvents as any).waitForJob('job-1', 'completed', -1), 'TypeError', 'qEvents.waitForJob with negative timeout');

  console.log('\n[PASS] ALL USER ERROR GUARDS VALIDATED SUCCESSFULLY!');
  process.exit(0);
}

runValidationTests().catch((err) => {
  console.error('Validation test failed with unhandled error:', err);
  process.exit(1);
});
