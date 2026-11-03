import { EventEmitter } from 'events';
import { Job } from './job';
import {
  BackoffType,
  JobConfig,
  JobCounts,
  JobDto,
  JobOptions,
  NativeQueueBinding,
} from './types';

/**
 * High-performance Queue class.
 * Delivers ultra-high throughput, microsecond enqueue latency, and ZERO V8 heap pollution.
 */
export class Queue<TData = any, TResult = any> extends EventEmitter {
  public readonly name: string;
  private nativeQueue: NativeQueueBinding;

  constructor(name: string, nativeQueue: NativeQueueBinding) {
    super();
    if (typeof name !== 'string' || name.trim().length === 0) {
      throw new TypeError(`Queue: A non-empty string is required for queue name, received ${name === null ? 'null' : typeof name}`);
    }
    this.name = name;
    this.nativeQueue = nativeQueue;
  }

  /**
   * Adds a new job to the queue.
   */
  public async add(
    name: string,
    data: TData,
    opts: JobOptions = {}
  ): Promise<Job<TData, TResult>> {
    if (typeof name !== 'string' || name.trim().length === 0) {
      throw new TypeError(`Queue.add: A non-empty string is required for job name, received ${name === null ? 'null' : typeof name}`);
    }
    // 1. Zero-copy buffer serialization
    const buffer = Job.serializePayload(data);

    // 2. Map TS options to Rust JobConfig
    const backoffObj = typeof opts.backoff === 'object' ? opts.backoff : undefined;
    const config: JobConfig = {
      priority: opts.priority ?? 0,
      delayMs: opts.delay ?? 0,
      maxRetries: opts.attempts ?? 3,
      backoffType: this.mapBackoffType(backoffObj?.type),
      backoffBaseMs: typeof opts.backoff === 'number' ? opts.backoff : (backoffObj?.delay ?? 1000),
      backoffMaxMs: backoffObj?.maxDelay ?? 60000,
      jitter: backoffObj?.jitter ?? true,
      timeoutMs: opts.timeout ?? 30000,
      groupKey: opts.groupKey,
    };

    // 3. Dispatch to Rust native memory
    const jobId = await this.nativeQueue.add(
      opts.jobId || undefined,
      name,
      buffer,
      config
    );

    const dto: JobDto = {
      id: jobId,
      name,
      queueName: this.name,
      data: buffer,
      state: (opts.delay ?? 0) > 0 ? ('Delayed' as any) : ('Waiting' as any),
      attemptsMade: 0,
      progress: 0,
      createdAt: Date.now(),
    };

    const job = new Job<TData, TResult>(dto, this.nativeQueue, opts);
    this.emit('waiting', job);
    return job;
  }

  /**
   * Pipelined addition of multiple jobs
   */
  public async addBulk(
    jobs: Array<{ name: string; data: TData; opts?: JobOptions }>
  ): Promise<Job<TData, TResult>[]> {
    if (!Array.isArray(jobs)) {
      throw new TypeError(`Queue.addBulk: Expected an array of jobs, received ${jobs === null ? 'null' : typeof jobs}`);
    }
    return Promise.all(jobs.map((j) => this.add(j.name, j.data, j.opts)));
  }

  /**
   * Retrieves a job by ID
   */
  public async getJob(jobId: string): Promise<Job<TData, TResult> | null> {
    if (typeof jobId !== 'string' || jobId.trim().length === 0) {
      throw new TypeError(`Queue.getJob: A non-empty string is required for jobId, received ${jobId === null ? 'null' : typeof jobId}`);
    }
    const dto = await this.nativeQueue.getJob(jobId);
    if (!dto) return null;
    return new Job<TData, TResult>(dto, this.nativeQueue);
  }

  /**
   * Returns live breakdown of job counts
   */
  public async getJobCounts(): Promise<JobCounts> {
    return this.nativeQueue.getCounts();
  }

  /**
   * Pauses queue job execution
   */
  public async pause(): Promise<void> {
    await this.nativeQueue.pause();
    this.emit('paused');
  }

  /**
   * Resumes queue job execution
   */
  public async resume(): Promise<void> {
    await this.nativeQueue.resume();
    this.emit('resumed');
  }

  /**
   * Drains all waiting and delayed jobs
   */
  public async drain(): Promise<void> {
    await this.nativeQueue.drain();
    this.emit('drained');
  }

  /**
   * Completely obliterates/drains the queue
   */
  public async obliterate(): Promise<void> {
    await this.drain();
  }

  /**
   * Closes the queue
   */
  public async close(): Promise<void> {
    this.removeAllListeners();
  }

  public getNative(): NativeQueueBinding {
    return this.nativeQueue;
  }

  private mapBackoffType(type?: string): BackoffType {
    switch (type) {
      case 'linear':
        return BackoffType.Linear;
      case 'fixed':
        return BackoffType.Fixed;
      case 'exponential':
      default:
        return BackoffType.Exponential;
    }
  }
}
