import { EventEmitter } from 'events';
import {
  QueueJobOptions,
  FlowJob,
  FlowNode,
  JobCounts,
  JobState,
  WorkerOptions,
  EngineMode,
  JobConfig,
  BackoffType,
  JobDto,
  NativeQueueBinding,
  NativeRateLimiterBinding,
} from './types';
import { TurboPulse, getSharedRedisEngine } from './engine';

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * High-performance Zero-Copy Job representation.
 */
export class Job<DataType = any, ResultType = any, NameType extends string = string> {
  public id: string;
  public name: NameType;
  public data: DataType;
  public opts: QueueJobOptions;
  public progress: number = 0;
  public attemptsMade: number = 0;
  public stalledCount: number = 0;
  public lockToken?: string;
  public returnvalue: ResultType = null as any;

  public get returnValue(): ResultType {
    return this.returnvalue;
  }

  public set returnValue(val: ResultType) {
    this.returnvalue = val;
  }

  public failedReason?: string;
  public stacktrace: string[] = [];
  public timestamp: number;
  public processedOn?: number;
  public finishedOn?: number;
  public parent?: { id: string; queue: string };
  public repeatJobKey?: string;

  private queueBinding: NativeQueueBinding;
  private queueName: string;

  constructor(
    queueBinding: NativeQueueBinding,
    queueName: string,
    id: string,
    name: NameType,
    data: DataType,
    opts: QueueJobOptions = {},
    dto?: JobDto
  ) {
    this.queueBinding = queueBinding;
    this.queueName = queueName;
    this.id = id;
    this.name = name;
    this.data = data;
    this.opts = opts;
    this.timestamp = dto?.createdAt ?? Date.now();

    if (dto) {
      this.attemptsMade = dto.attemptsMade;
      this.stalledCount = dto.stalledCount ?? 0;
      this.lockToken = dto.lockToken;
      this.progress = dto.progress;
      this.processedOn = dto.processedAt;
      this.finishedOn = dto.finishedAt;
      this.failedReason = dto.failedReason;
      if (dto.returnValue && dto.returnValue.length > 0) {
        try {
          this.returnvalue = JSON.parse(dto.returnValue.toString('utf-8'));
        } catch {
          this.returnvalue = dto.returnValue as any;
        }
      }
    }
  }

  /**
   * Updates job progress percentage (0 - 100)
   */
  public async updateProgress(progress: number): Promise<void> {
    if (typeof progress !== 'number' || isNaN(progress)) {
      throw new TypeError(`Job.updateProgress: progress must be a number, received ${progress}`);
    }
    this.progress = Math.min(100, Math.max(0, progress));
    await this.queueBinding.updateProgress(this.id, this.progress);
  }

  /**
   * Updates job data in-place
   */
  public async updateData(data: DataType): Promise<void> {
    this.data = data;
    const buf = Buffer.from(JSON.stringify(data), 'utf-8');
    await this.queueBinding.updateData(this.id, buf);
  }

  /**
   * Appends execution log entry
   */
  public async log(row: string): Promise<void> {
    this.stacktrace.push(`[${new Date().toISOString()}] ${row}`);
  }

  /**
   * Promotes delayed job to waiting queue immediately
   */
  public async promote(): Promise<boolean> {
    return this.queueBinding.promote(this.id);
  }

  public async retry(): Promise<void> {
    await this.promote();
  }

  /**
   * Discards the job permanently
   */
  public async discard(): Promise<void> {
    await this.queueBinding.fail(this.id, 'Discarded by user');
  }

  /**
   * Fetches current state
   */
  public async getState(): Promise<JobState | undefined> {
    const dto = await this.queueBinding.getJob(this.id);
    return dto ? dto.state : undefined;
  }

  /**
   * Checks if job completed
   */
  public async isCompleted(): Promise<boolean> {
    const s = await this.getState();
    return s === JobState.Completed;
  }

  /**
   * Checks if job failed
   */
  public async isFailed(): Promise<boolean> {
    const s = await this.getState();
    return s === JobState.Failed;
  }
}

/**
 * Production-grade Queue: High-Throughput Native Rust + Redis Distributed Queue
 */
export class Queue<DataType = any, ResultType = any, NameType extends string = string> {
  public name: string;
  public defaultJobOptions: QueueJobOptions;

  private nativeBinding: NativeQueueBinding = null as any;
  private readyPromise: Promise<void>;

  constructor(
    name: string,
    opts: {
      connection?: any;
      client?: any;
      defaultJobOptions?: QueueJobOptions;
      redisUrl?: string;
      clusterNodes?: string[];
      nodes?: string[];
    } = {}
  ) {
    if (typeof name !== 'string' || name.trim().length === 0) {
      throw new TypeError(
        `Queue: A non-empty string is required for queue name, received ${
          name === null ? 'null' : typeof name
        }`
      );
    }
    if (opts != null && typeof opts !== 'object') {
      throw new TypeError(`Queue: Expected options to be an object, received ${typeof opts}`);
    }
    this.name = name;
    this.defaultJobOptions = opts.defaultJobOptions || {};
    this.readyPromise = this.init(opts);
    this.readyPromise.catch(() => {});
  }

  private async init(opts: any): Promise<void> {
    const isRedis = Boolean(
      opts.connection ||
      opts.client ||
      opts.redisUrl ||
      opts.clusterNodes ||
      opts.nodes
    );
    if (isRedis) {
      const target =
        opts.connection ||
        opts.client ||
        opts.redisUrl ||
        opts.clusterNodes ||
        opts.nodes;
      const engine = await getSharedRedisEngine(target);
      this.nativeBinding = (engine as any).nativeEngine.createQueue(this.name);
    } else {
      const engine = TurboPulse.createSync({
        mode: EngineMode.InMemory,
      });
      this.nativeBinding = (engine as any).nativeEngine.createQueue(this.name);
    }
  }

  public get binding(): NativeQueueBinding {
    return this.nativeBinding;
  }

  public async waitUntilReady(): Promise<void> {
    await this.readyPromise;
  }

  /**
   * Adds a single job to the queue
   */
  public async add(name: NameType, data: DataType, opts?: QueueJobOptions): Promise<Job<DataType, ResultType, NameType>> {
    if (typeof name !== 'string' || (name as string).trim().length === 0) {
      throw new TypeError(
        `Queue.add: A non-empty string is required for job name, received ${
          name === null ? 'null' : typeof name
        }`
      );
    }
    if (opts != null && typeof opts !== 'object') {
      throw new TypeError(`Queue.add: Expected opts to be an object, received ${typeof opts}`);
    }
    await this.readyPromise;
    const mergedOpts = { ...this.defaultJobOptions, ...opts };
    const rawData = Buffer.from(JSON.stringify(data ?? null), 'utf-8');

    const config: JobConfig = {
      priority: mergedOpts.priority ?? 0,
      delayMs: mergedOpts.delay ?? 0,
      maxRetries: mergedOpts.attempts ?? 3,
      timeoutMs: mergedOpts.timeout ?? 30000,
      lifo: mergedOpts.lifo ?? false,
      removeOnComplete: Boolean(mergedOpts.removeOnComplete),
      removeOnFail: Boolean(mergedOpts.removeOnFail),
      repeatEveryMs: mergedOpts.repeat?.every,
      repeatLimit: mergedOpts.repeat?.limit,
      parentId: mergedOpts.parent?.id,
      dedupKey: mergedOpts.deduplication?.id,
    };

    if (mergedOpts.backoff) {
      if (typeof mergedOpts.backoff === 'number') {
        config.backoffType = BackoffType.Fixed;
        config.backoffBaseMs = mergedOpts.backoff;
      } else {
        const typeStr = mergedOpts.backoff.type || 'exponential';
        config.backoffType =
          typeStr === 'linear'
            ? BackoffType.Linear
            : typeStr === 'fixed'
            ? BackoffType.Fixed
            : BackoffType.Exponential;
        config.backoffBaseMs = mergedOpts.backoff.delay ?? 1000;
        config.backoffMaxMs = mergedOpts.backoff.maxDelay ?? 60000;
        config.jitter = mergedOpts.backoff.jitter ?? true;
      }
    }

    const assignedId = await this.nativeBinding.add(
      mergedOpts.jobId || undefined,
      name,
      rawData,
      config
    );

    return new Job<DataType, ResultType, NameType>(
      this.nativeBinding,
      this.name,
      assignedId,
      name,
      data,
      mergedOpts
    );
  }

  /**
   * Atomically adds multiple jobs in a single batch
   */
  public async addBulk(
    jobs: { name: NameType; data: DataType; opts?: QueueJobOptions }[]
  ): Promise<Job<DataType, ResultType, NameType>[]> {
    if (!Array.isArray(jobs)) {
      throw new TypeError(
        `Queue.addBulk: Expected an array of jobs, received ${jobs === null ? 'null' : typeof jobs}`
      );
    }
    if (jobs.length === 0) return [];

    for (let i = 0; i < jobs.length; i++) {
      const j = jobs[i];
      if (!j || typeof j !== 'object') {
        throw new TypeError(
          `Queue.addBulk: Job at index ${i} must be an object, received ${j === null ? 'null' : typeof j}`
        );
      }
      if (typeof j.name !== 'string' || (j.name as string).trim().length === 0) {
        throw new TypeError(
          `Queue.addBulk: Job at index ${i} requires a non-empty string for name, received ${
            j.name === null ? 'null' : typeof j.name
          }`
        );
      }
      if (j.opts != null && typeof j.opts !== 'object') {
        throw new TypeError(`Queue.addBulk: Job at index ${i} opts must be an object, received ${typeof j.opts}`);
      }
    }

    await this.readyPromise;

    const bulkInputs = jobs.map((j) => {
      const mergedOpts = { ...this.defaultJobOptions, ...j.opts };
      const rawData = Buffer.from(JSON.stringify(j.data ?? null), 'utf-8');
      const config: JobConfig = {
        priority: mergedOpts.priority ?? 0,
        delayMs: mergedOpts.delay ?? 0,
        maxRetries: mergedOpts.attempts ?? 3,
        timeoutMs: mergedOpts.timeout ?? 30000,
        lifo: mergedOpts.lifo ?? false,
        removeOnComplete: Boolean(mergedOpts.removeOnComplete),
        removeOnFail: Boolean(mergedOpts.removeOnFail),
        repeatEveryMs: mergedOpts.repeat?.every,
        repeatLimit: mergedOpts.repeat?.limit,
        parentId: mergedOpts.parent?.id,
        dedupKey: mergedOpts.deduplication?.id,
      };
      return {
        jobId: mergedOpts.jobId || undefined,
        name: j.name,
        data: rawData,
        config,
      };
    });

    const ids = await this.nativeBinding.addBulk(bulkInputs);
    return ids.map((id, idx) => {
      return new Job<DataType, ResultType, NameType>(
        this.nativeBinding,
        this.name,
        id,
        jobs[idx].name,
        jobs[idx].data,
        { ...this.defaultJobOptions, ...jobs[idx].opts }
      );
    });
  }

  private isPausedFlag: boolean = false;

  public async pause(): Promise<void> {
    await this.readyPromise;
    this.isPausedFlag = true;
    await this.nativeBinding.pause();
  }

  public async resume(): Promise<void> {
    await this.readyPromise;
    this.isPausedFlag = false;
    await this.nativeBinding.resume();
  }

  public async isPaused(): Promise<boolean> {
    return this.isPausedFlag;
  }

  public async getJobCounts(): Promise<JobCounts> {
    await this.readyPromise;
    return this.nativeBinding.getCounts();
  }

  public async getJob(jobId: string): Promise<Job<DataType, ResultType, NameType> | null> {
    if (typeof jobId !== 'string' || jobId.trim().length === 0) {
      throw new TypeError(
        `Queue.getJob: A non-empty string is required for jobId, received ${
          jobId === null ? 'null' : typeof jobId
        }`
      );
    }
    await this.readyPromise;
    const dto = await this.nativeBinding.getJob(jobId);
    if (!dto) return null;
    let parsed: any;
    try {
      parsed = JSON.parse(dto.data.toString('utf-8'));
    } catch {
      parsed = dto.data;
    }
    return new Job<DataType, ResultType, NameType>(
      this.nativeBinding,
      this.name,
      dto.id,
      dto.name as NameType,
      parsed,
      {},
      dto
    );
  }

  public async clean(graceMs: number, limit: number = 1000, type: 'completed' | 'failed' | 'wait' = 'completed'): Promise<number> {
    if (typeof graceMs !== 'number' || isNaN(graceMs) || graceMs < 0) {
      throw new TypeError(`Queue.clean: Expected graceMs to be a non-negative number, received ${graceMs}`);
    }
    await this.readyPromise;
    return this.nativeBinding.clean(graceMs, limit, type);
  }

  public async drain(): Promise<void> {
    await this.readyPromise;
    await this.nativeBinding.clean(0, 1000000, 'wait');
  }

  public async obliterate(): Promise<void> {
    await this.readyPromise;
    await this.nativeBinding.clean(0, 1000000, 'completed');
    await this.nativeBinding.clean(0, 1000000, 'failed');
    await this.nativeBinding.clean(0, 1000000, 'wait');
  }

  public async close(): Promise<void> {}
}

/**
 * Production-grade Worker: Zero-latency native dispatching, heartbeats, and rate limiting
 */
export class Worker<DataType = any, ResultType = any, NameType extends string = string> extends EventEmitter {
  public name: string;
  private processor: (job: Job<DataType, ResultType, NameType>) => Promise<ResultType> | ResultType;
  private options: WorkerOptions & {
    autorun?: boolean;
    autoStart?: boolean;
    lockDuration?: number;
    stalledInterval?: number;
    maxStalledCount?: number;
  };
  private queueBinding: NativeQueueBinding = null as any;
  private rateLimiter?: NativeRateLimiterBinding;
  private isRunningState: boolean = false;
  private isPausedState: boolean = false;
  private activeConcurrency: number = 0;
  private loopTimer: NodeJS.Timeout | null = null;
  private stalledTimer: NodeJS.Timeout | null = null;
  private readyPromise: Promise<void>;
  private movingAvgDurationMs: number = 0;
  private totalCompletedCount: number = 0;
  private adaptiveBatchSize: number = 1;
  private isFetching: boolean = false;

  constructor(
    queueName: string,
    processor: (job: Job<DataType, ResultType, NameType>) => Promise<ResultType> | ResultType,
    opts: WorkerOptions = {}
  ) {
    super();
    if (typeof queueName !== 'string' || queueName.trim().length === 0) {
      throw new TypeError(
        `Worker: A non-empty string is required for queue name, received ${
          queueName === null ? 'null' : typeof queueName
        }`
      );
    }
    if (typeof processor !== 'function') {
      throw new TypeError(
        `Worker: Expected a processor function, received ${
          processor === null ? 'null' : typeof processor
        }`
      );
    }
    if (opts != null && typeof opts !== 'object') {
      throw new TypeError(`Worker: Expected opts to be an object, received ${typeof opts}`);
    }
    if (opts?.concurrency != null && (typeof opts.concurrency !== 'number' || opts.concurrency < 1 || isNaN(opts.concurrency))) {
      throw new TypeError(`Worker: concurrency must be a positive integer, received ${opts.concurrency}`);
    }

    this.name = queueName;
    this.processor = processor;
    this.options = {
      concurrency: 1,
      pollIntervalMs: 20,
      autoStart: true,
      autorun: true,
      lockDuration: 30000,
      stalledInterval: 30000,
      maxStalledCount: 3,
      ...opts,
    };

    this.readyPromise = this.init();
    this.readyPromise.catch(() => {});
  }

  private async init(): Promise<void> {
    const isRedis = Boolean(
      this.options.connection ||
      this.options.client ||
      this.options.redisUrl ||
      this.options.clusterNodes ||
      this.options.nodes
    );
    if (isRedis) {
      const target =
        this.options.connection ||
        this.options.client ||
        this.options.redisUrl ||
        this.options.clusterNodes ||
        this.options.nodes;
      const engine = await getSharedRedisEngine(target);
      this.queueBinding = (engine as any).nativeEngine.createQueue(this.name);
      if (this.options.limiter) {
        const limConf = this.options.limiter as any;
        if (typeof limConf.waitUntilReady === 'function') {
          await limConf.waitUntilReady();
        }
        if (limConf.binding) {
          this.rateLimiter = limConf.binding;
        } else if (typeof limConf.acquire === 'function') {
          this.rateLimiter = limConf;
        } else {
          this.rateLimiter = (engine as any).nativeEngine.createRateLimiter({
            capacity: limConf.max ?? limConf.capacity ?? 1000,
            refillRate: limConf.max ?? limConf.refillRate ?? 1000,
            refillIntervalMs: limConf.duration ?? limConf.refillIntervalMs ?? 1000,
          });
        }
      }
    } else {
      const engine = TurboPulse.createSync({ mode: EngineMode.InMemory });
      this.queueBinding = (engine as any).nativeEngine.createQueue(this.name);
      if (this.options.limiter) {
        const limConf = this.options.limiter as any;
        if (typeof limConf.waitUntilReady === 'function') {
          await limConf.waitUntilReady();
        }
        if (limConf.binding) {
          this.rateLimiter = limConf.binding;
        } else if (typeof limConf.acquire === 'function') {
          this.rateLimiter = limConf;
        } else {
          this.rateLimiter = (engine as any).nativeEngine.createRateLimiter({
            capacity: limConf.max ?? limConf.capacity ?? 1000,
            refillRate: limConf.max ?? limConf.refillRate ?? 1000,
            refillIntervalMs: limConf.duration ?? limConf.refillIntervalMs ?? 1000,
          });
        }
      }
    }

    const start = this.options.autoStart !== false && this.options.autorun !== false;
    if (start) {
      this.run();
    }

    // Start stalled recovery heartbeat loop
    this.stalledTimer = setInterval(async () => {
      try {
        const recovered = await this.queueBinding.recoverStalled(
          this.options.lockDuration || 30000,
          this.options.maxStalledCount || 3
        );
        if (recovered > 0) {
          this.emit('stalled', recovered);
        }
      } catch (err) {
        this.emit('error', err);
      }
    }, this.options.stalledInterval || 30000);
  }

  public async waitUntilReady(): Promise<void> {
    await this.readyPromise;
  }

  public run(): void {
    if (this.isRunningState) return;
    this.isRunningState = true;
    const maxConcurrency = Math.max(1, this.options.concurrency || 1);
    for (let i = 0; i < maxConcurrency; i++) {
      this.runSlot(i);
    }
  }

  public async pause(): Promise<void> {
    this.isPausedState = true;
    this.emit('paused');
  }

  public async resume(): Promise<void> {
    this.isPausedState = false;
    this.emit('resumed');
  }

  public isPaused(): boolean {
    return this.isPausedState;
  }

  public isRunning(): boolean {
    return this.isRunningState;
  }

  public async close(): Promise<void> {
    this.isRunningState = false;
    this.emit('closing');
    if (this.stalledTimer) clearInterval(this.stalledTimer);
    this.emit('closed');
  }

  private async runSlot(_slotId: number): Promise<void> {
    await this.readyPromise;

    while (this.isRunningState) {
      if (this.isPausedState) {
        await sleep(50);
        continue;
      }

      if (this.rateLimiter) {
        const check = await this.rateLimiter.acquire(this.name, 1);
        if (!check.allowed) {
          await sleep(check.retryAfterMs || 20);
          continue;
        }
      }

      let dto: JobDto | null = null;
      try {
        dto = await this.queueBinding.fetchNext();
      } catch (err) {
        this.emit('error', err);
      }

      if (!dto) {
        if (this.rateLimiter) {
          try {
            if (typeof (this.rateLimiter as any).refund === 'function') {
              await (this.rateLimiter as any).refund(this.name, 1);
            } else {
              await this.rateLimiter.release(this.name);
            }
          } catch {}
        }
        if (this.activeConcurrency === 0) {
          this.emit('drained');
        }
        await sleep(this.options.pollIntervalMs || 10);
        continue;
      }

      this.activeConcurrency++;
      try {
        await this.processJob(dto);
      } finally {
        this.activeConcurrency--;
      }
    }
  }

  private async processJob(dto: JobDto): Promise<void> {
    const jobStartTime = Date.now();
    let parsedData: any = null;
    try {
      parsedData = JSON.parse(dto.data.toString('utf-8'));
    } catch {
      parsedData = dto.data;
    }

    const job = new Job<DataType, ResultType, NameType>(
      this.queueBinding,
      this.name,
      dto.id,
      dto.name as NameType,
      parsedData,
      {},
      dto
    );

    this.emit('active', job);

    const lockDuration = this.options.lockDuration || 30000;
    const lockRenewInterval = this.options.lockRenewTime || Math.max(100, Math.floor(lockDuration / 3));
    let heartbeatTimer: NodeJS.Timeout | null = null;
    let lockLost = false;

    if (job.lockToken) {
      heartbeatTimer = setInterval(async () => {
        try {
          const renewed = await this.queueBinding.renewLock(job.id, job.lockToken!, lockDuration);
          if (!renewed) {
            lockLost = true;
            if (heartbeatTimer) clearInterval(heartbeatTimer);
            this.emit('error', new Error(`LOCK_LOST: Worker lost lock for job ${job.id}`));
          }
        } catch {
          lockLost = true;
          if (heartbeatTimer) clearInterval(heartbeatTimer);
        }
      }, lockRenewInterval);
    }

    try {
      const result = await this.processor(job);
      job.returnvalue = result;

      if (lockLost) {
        throw new Error(`LOCK_LOST: Job ${job.id} was recovered by another worker while executing`);
      }

      const resBuf = result !== undefined ? Buffer.from(JSON.stringify(result), 'utf-8') : null;
      await this.queueBinding.complete(job.id, resBuf, job.lockToken || null);

      this.emit('completed', job, result);
    } catch (err: any) {
      const errMsg = err?.message || String(err);
      job.failedReason = errMsg;

      if (!lockLost) {
        try {
          const permanentlyFailed = await this.queueBinding.fail(job.id, errMsg, job.lockToken || null);
          if (permanentlyFailed) {
            this.emit('failed', job, err);
          }
        } catch (failErr) {
          this.emit('failed', job, err);
        }
      } else {
        this.emit('failed', job, err);
      }
    } finally {
      if (heartbeatTimer) {
        clearInterval(heartbeatTimer);
      }
      if (this.rateLimiter) {
        try {
          await this.rateLimiter.release(this.name);
        } catch {}
      }
    }
  }
}

/**
 * FlowProducer: Parent-Child DAG dependency management
 */
export class FlowProducer {
  private options: { connection?: any };

  constructor(opts: { connection?: any } = {}) {
    if (opts != null && typeof opts !== 'object') {
      throw new TypeError(`FlowProducer: Expected options to be an object, received ${typeof opts}`);
    }
    this.options = opts || {};
  }

  private validateTree(node: FlowJob): void {
    if (!node || typeof node !== 'object') {
      throw new TypeError(`FlowProducer.add: Expected a flow job configuration object, received ${node === null ? 'null' : typeof node}`);
    }
    if (typeof node.queueName !== 'string' || node.queueName.trim().length === 0) {
      throw new TypeError(`FlowProducer.add: Flow job requires a non-empty string for queueName, received ${typeof node.queueName}`);
    }
    if (typeof node.name !== 'string' || node.name.trim().length === 0) {
      throw new TypeError(`FlowProducer.add: Flow job requires a non-empty string for name, received ${typeof node.name}`);
    }
    if (node.children && Array.isArray(node.children)) {
      for (const child of node.children) {
        this.validateTree(child);
      }
    }
  }

  /**
   * Adds a hierarchical flow of jobs. Parent will not be dispatched until all children finish.
   */
  public async add<T = any>(flow: FlowJob<T>): Promise<FlowNode<T>> {
    this.validateTree(flow);

    const parentQueue = new Queue<T>(flow.queueName, { connection: this.options.connection });
    await parentQueue.waitUntilReady();

    const totalChildren = flow.children?.length || 0;
    const parentJob = await parentQueue.add(
      flow.name as any,
      flow.data as any,
      {
        ...flow.opts,
        delay: totalChildren > 0 ? 86400000 : flow.opts?.delay,
      }
    );

    const childNodes: FlowNode[] = [];

    if (totalChildren > 0) {
      await parentQueue.binding.registerParentChildren(parentJob.id, totalChildren);

      for (const childFlow of flow.children!) {
        const childOpts: QueueJobOptions = {
          ...childFlow.opts,
          parent: { id: parentJob.id, queue: flow.queueName },
        };
        const childNode = await this.add({ ...childFlow, opts: childOpts });
        childNodes.push(childNode);
      }
    }

    return {
      job: parentJob,
      children: childNodes,
    };
  }

  public async close(): Promise<void> {}
}

/**
 * QueueEvents: Real-time event listener
 */
export class QueueEvents extends EventEmitter {
  public queueName: string;

  constructor(queueName: string, opts: { connection?: any } = {}) {
    super();
    if (typeof queueName !== 'string' || queueName.trim().length === 0) {
      throw new TypeError(
        `QueueEvents: A non-empty string is required for queue name, received ${
          queueName === null ? 'null' : typeof queueName
        }`
      );
    }
    this.queueName = queueName;
  }

  /**
   * Waits for a job to complete or fail
   */
  public async waitForJob(jobId: string, event: 'completed' | 'failed' = 'completed', timeoutMs: number = 30000): Promise<any> {
    if (typeof jobId !== 'string' || jobId.trim().length === 0) {
      throw new TypeError(
        `QueueEvents.waitForJob: A non-empty string is required for jobId, received ${
          jobId === null ? 'null' : typeof jobId
        }`
      );
    }
    if (timeoutMs != null && (typeof timeoutMs !== 'number' || isNaN(timeoutMs) || timeoutMs < 0)) {
      throw new TypeError(`QueueEvents.waitForJob: timeoutMs must be a non-negative number, received ${timeoutMs}`);
    }
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        cleanup();
        reject(new Error(`Timed out waiting for job ${jobId} to emit ${event}`));
      }, timeoutMs);

      const onCompleted = (args: any) => {
        if (args.jobId === jobId) {
          cleanup();
          resolve(args.returnvalue);
        }
      };

      const onFailed = (args: any) => {
        if (args.jobId === jobId) {
          cleanup();
          reject(new Error(args.failedReason || 'Job failed'));
        }
      };

      const cleanup = () => {
        clearTimeout(timer);
        this.removeListener('completed', onCompleted);
        this.removeListener('failed', onFailed);
      };

      this.on('completed', onCompleted);
      this.on('failed', onFailed);
    });
  }

  public async close(): Promise<void> {
    this.removeAllListeners();
  }
}
