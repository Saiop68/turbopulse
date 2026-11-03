import { createInMemoryTurboPulse, JobState } from '../index';

async function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function runQueueTests() {
  console.log('========================================');
  console.log('🧪 RUNNING TURBOPULSE JOB QUEUE TESTS');
  console.log('========================================\n');

  const engine = createInMemoryTurboPulse();

  // TEST 1: Enqueue & Worker Processing with Concurrency
  console.log('▶ Test 1: Worker Execution & Concurrency');
  const queue1 = engine.createQueue<{ num: number }, number>('mathQueue');

  const processedOrder: number[] = [];
  const worker1 = engine.createWorker<{ num: number }, number>(
    'mathQueue',
    async (job) => {
      await sleep(20);
      processedOrder.push(job.data.num);
      return job.data.num * 2;
    },
    { concurrency: 2 }
  );

  // Add 4 jobs
  const jobPromises = [1, 2, 3, 4].map((i) =>
    queue1.add('compute', { num: i })
  );
  await Promise.all(jobPromises);

  // Wait for jobs to complete
  await sleep(150);
  await worker1.close();

  console.log('   Processed jobs:', processedOrder);
  if (processedOrder.length === 4) {
    console.log('   ✅ All 4 jobs completed successfully');
  } else {
    throw new Error(`Expected 4 jobs processed, got ${processedOrder.length}`);
  }

  const counts1 = await queue1.getJobCounts();
  console.log('   Queue Counts:', counts1);
  if (counts1.completed === 4 && counts1.waiting === 0) {
    console.log('   ✅ Queue counts updated properly');
  } else {
    throw new Error('Counts mismatch');
  }

  // TEST 2: Priority Ordering
  console.log('\n▶ Test 2: Priority Queue Scheduling');
  const queue2 = engine.createQueue<{ task: string }, string>('prioQueue');

  // Enqueue jobs with varying priorities while worker is not running
  await queue2.add('low', { task: 'LOW_PRIO' }, { priority: 0 });
  await queue2.add('high', { task: 'HIGH_PRIO' }, { priority: 100 });
  await queue2.add('medium', { task: 'MED_PRIO' }, { priority: 50 });

  const prioExecuted: string[] = [];
  const worker2 = engine.createWorker<{ task: string }, string>(
    'prioQueue',
    async (job) => {
      prioExecuted.push(job.data.task);
      return 'ok';
    },
    { concurrency: 1 } // concurrency 1 ensures strict sequence
  );

  await sleep(100);
  await worker2.close();

  console.log('   Priority execution order:', prioExecuted);
  if (
    prioExecuted[0] === 'HIGH_PRIO' &&
    prioExecuted[1] === 'MED_PRIO' &&
    prioExecuted[2] === 'LOW_PRIO'
  ) {
    console.log('   ✅ Jobs executed in exact priority order (100 -> 50 -> 0)');
  } else {
    throw new Error(`Priority ordering failed: ${prioExecuted.join(' -> ')}`);
  }

  // TEST 3: Automated Retries with Backoff
  console.log('\n▶ Test 3: Automated Retries & Exponential Backoff');
  const queue3 = engine.createQueue<{ name: string }, string>('retryQueue');

  let attempts = 0;
  const worker3 = engine.createWorker<{ name: string }, string>(
    'retryQueue',
    async (job) => {
      attempts++;
      if (attempts < 3) {
        throw new Error(`Temporary failure on attempt ${attempts}`);
      }
      return 'recovered';
    },
    { concurrency: 1, pollIntervalMs: 10 }
  );

  const retryJob = await queue3.add(
    'flakyJob',
    { name: 'payment' },
    {
      attempts: 4,
      backoff: {
        type: 'fixed',
        delay: 50,
        jitter: false,
      },
    }
  );

  // Wait for retries
  await sleep(250);
  await worker3.close();

  console.log(`   Job made ${attempts} attempts before succeeding`);
  const finalJobState = await queue3.getJob(retryJob.id);
  console.log('   Final job state:', finalJobState?.state);
  console.log('   Final job return value:', finalJobState?.returnvalue);

  if (attempts === 3 && finalJobState?.state === JobState.Completed) {
    console.log('   ✅ Automated retry and eventual recovery succeeded');
  } else {
    throw new Error('Retry test failed');
  }

  // TEST 4: Delayed Jobs
  console.log('\n▶ Test 4: Delayed Jobs');
  const queue4 = engine.createQueue<{ msg: string }, string>('delayQueue');

  const delayedExecuted: number[] = [];
  const startDelay = Date.now();

  const worker4 = engine.createWorker<{ msg: string }, string>(
    'delayQueue',
    async (job) => {
      delayedExecuted.push(Date.now() - startDelay);
      return 'ok';
    },
    { concurrency: 1, pollIntervalMs: 10 }
  );

  await queue4.add('delayedMsg', { msg: 'hello future' }, { delay: 100 });

  await sleep(40);
  const countBeforeDelay = delayedExecuted.length;
  console.log('   Delayed jobs running at 40ms:', countBeforeDelay);
  if (countBeforeDelay === 0) {
    console.log('   ✅ Job not executed before delay interval');
  } else {
    throw new Error('Delayed job ran prematurely');
  }

  // Wait for 100ms delay + polling to trigger
  await sleep(200);
  await worker4.close();

  const finalDelayedCount = delayedExecuted.length;
  console.log('   Job executed at:', delayedExecuted[0], 'ms');
  if (finalDelayedCount === 1 && delayedExecuted[0] >= 90) {
    console.log('   ✅ Delayed job executed after scheduled time');
  } else {
    throw new Error('Delay test failed');
  }

  // TEST 5: Progress Reporting
  console.log('\n▶ Test 5: Progress Reporting');
  const queue5 = engine.createQueue<{ item: string }, string>('progressQueue');

  const progressEvents: number[] = [];
  const worker5 = engine.createWorker<{ item: string }, string>(
    'progressQueue',
    async (job) => {
      await job.updateProgress(25);
      progressEvents.push(job.progress);
      await job.updateProgress(50);
      progressEvents.push(job.progress);
      await job.updateProgress(100);
      progressEvents.push(job.progress);
      return 'done';
    },
    { concurrency: 1 }
  );

  await queue5.add('upload', { item: 'video.mp4' });
  await sleep(100);
  await worker5.close();

  console.log('   Progress states:', progressEvents);
  if (progressEvents.join(',') === '25,50,100') {
    console.log('   ✅ Job progress reporting verified');
  } else {
    throw new Error('Progress reporting failed');
  }

  console.log('\n🎉 ALL QUEUE TESTS PASSED SUCCESSFULLY!\n');
}

runQueueTests().catch((err) => {
  console.error('❌ Queue Test Failed:', err);
  process.exit(1);
});
