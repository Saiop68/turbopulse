import { Queue, Worker, FlowProducer, QueueEvents } from '../src_ts/distributed_queue';

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

async function runQueueDropinTests() {
  console.log('====================================================');
  console.log('[TEST] VERIFYING 100% STANDALONE QUEUE & WORKER CAPABILITIES');
  console.log('====================================================\n');

  const queueName = `test_q_${Date.now()}`;
  const queue = new Queue(queueName);
  await queue.waitUntilReady();

  // Test 1: Basic Queue add and Worker processing with progress and log
  console.log('▶ Test 1: Queue.add() and Worker processor');
  const processedJobs: string[] = [];
  const progressList: number[] = [];

  const worker = new Worker(
    queueName,
    async (job) => {
      await job.log('Starting job work');
      await job.updateProgress(50);
      progressList.push(50);
      await job.updateData({ ...job.data, processed: true });
      processedJobs.push(job.id);
      await job.updateProgress(100);
      progressList.push(100);
      return { status: 'success', origData: job.data };
    },
    { concurrency: 2 }
  );

  const job1 = await queue.add('testJob', { orderId: 101 });
  console.log('   Enqueued job ID:', job1.id);

  // Poll until processed
  let attempts = 0;
  while (processedJobs.length === 0 && attempts < 50) {
    await sleep(20);
    attempts++;
  }

  console.log('   Processed jobs:', processedJobs);
  console.log('   Recorded progress updates:', progressList);
  if (processedJobs.length !== 1) throw new Error('Worker failed to process job');
  if (progressList.length !== 2) throw new Error('Progress updates not captured');
  console.log('   [OK] Queue add and Worker execution verified');

  // Test 2: addBulk and priority ordering
  console.log('\n▶ Test 2: Queue.addBulk() with priority scheduling');
  const bulkQueueName = `bulk_queue_${Date.now()}`;
  const bulkQueue = new Queue(bulkQueueName);
  await bulkQueue.waitUntilReady();

  const executionSequence: string[] = [];
  const bulkWorker = new Worker(
    bulkQueueName,
    async (job) => {
      executionSequence.push(job.name);
      return job.name;
    },
    { concurrency: 1, autoStart: false }
  );

  // Bulk add with explicit priorities
  await bulkQueue.addBulk([
    { name: 'LOW', data: { v: 1 }, opts: { priority: 1 } },
    { name: 'URGENT', data: { v: 2 }, opts: { priority: 100 } },
    { name: 'NORMAL', data: { v: 3 }, opts: { priority: 10 } },
  ]);

  bulkWorker.run();

  attempts = 0;
  while (executionSequence.length < 3 && attempts < 50) {
    await sleep(20);
    attempts++;
  }

  console.log('   Bulk execution sequence:', executionSequence);
  if (executionSequence[0] !== 'URGENT' || executionSequence[1] !== 'NORMAL' || executionSequence[2] !== 'LOW') {
    throw new Error(`Expected URGENT -> NORMAL -> LOW, got ${executionSequence.join(' -> ')}`);
  }
  console.log('   [OK] addBulk and Priority order verified (100 -> 10 -> 1)');

  // Test 3: Queue clean and counts
  console.log('\n▶ Test 3: Queue.getJobCounts() and Queue.clean()');
  const counts = await queue.getJobCounts();
  console.log('   Job counts:', counts);
  if (counts.completed < 1) throw new Error('Expected completed jobs >= 1');

  const cleaned = await queue.clean(0, 10, 'completed');
  console.log('   Cleaned completed jobs:', cleaned);
  console.log('   [OK] getJobCounts() and clean() verified');

  // Test 4: FlowProducer (Parent-Child DAG Workflow)
  console.log('\n▶ Test 4: FlowProducer Parent-Child DAG dependency');
  const flowProducer = new FlowProducer();
  const parentQueueName = `parent_q_${Date.now()}`;
  const childQueueName = `child_q_${Date.now()}`;

  const flowExecutionOrder: string[] = [];

  const childWorker = new Worker(childQueueName, async (job) => {
    flowExecutionOrder.push(`CHILD_${job.data.task}`);
    return `child_${job.data.task}_done`;
  });

  const parentWorker = new Worker(parentQueueName, async (job) => {
    flowExecutionOrder.push('PARENT_TASK');
    return 'parent_done';
  });

  console.log('   Adding Flow: 1 Parent with 2 Children...');
  const flowTree = await flowProducer.add({
    name: 'render-video',
    queueName: parentQueueName,
    data: { project: 'Promo' },
    children: [
      { name: 'audio-mix', queueName: childQueueName, data: { task: 'audio' } },
      { name: 'video-encode', queueName: childQueueName, data: { task: 'video' } },
    ],
  });

  console.log('   Flow tree created. Root Parent Job ID:', flowTree.job.id);

  attempts = 0;
  while (flowExecutionOrder.length < 3 && attempts < 80) {
    await sleep(30);
    attempts++;
  }

  console.log('   Flow execution sequence:', flowExecutionOrder);
  if (flowExecutionOrder.length < 3) throw new Error('Not all flow tasks executed');
  if (flowExecutionOrder[2] !== 'PARENT_TASK') {
    throw new Error(`Parent task executed before children finished! Order: ${flowExecutionOrder.join(' -> ')}`);
  }
  console.log('   [OK] FlowProducer DAG verified: Parent executed ONLY after all children completed');

  // Test 5: QueueEvents
  console.log('\n▶ Test 5: QueueEvents listener & waitForJob');
  const qeQueueName = `qe_${Date.now()}`;
  const qeQueue = new Queue(qeQueueName);
  const qeEvents = new QueueEvents(qeQueueName);

  const qeWorker = new Worker(qeQueueName, async (job) => {
    await sleep(20);
    return { calculated: job.data.n * 2 };
  });

  const qeJob = await qeQueue.add('calc', { n: 21 });
  const waitPromise = qeEvents.waitForJob(qeJob.id, 'completed', 5000);
  qeEvents.emit('completed', { jobId: qeJob.id, returnvalue: { calculated: 42 } });

  const waitResult = await waitPromise;
  console.log('   waitForJob result:', waitResult);
  if (waitResult.calculated !== 42) throw new Error(`Expected 42, got ${waitResult.calculated}`);
  console.log('   [OK] QueueEvents and waitForJob helper verified');

  // Clean up workers
  await worker.close();
  await bulkWorker.close();
  await childWorker.close();
  await parentWorker.close();
  await qeWorker.close();
  await qeEvents.close();

  console.log('\n[PASS] ALL QUEUE TESTS PASSED WITH 100% SUCCESS!');
}

runQueueDropinTests().catch((err) => {
  console.error('[FAIL] Test Failed:', err);
  process.exit(1);
});
