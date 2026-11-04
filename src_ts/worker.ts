import { EventEmitter } from 'events';
import { Job } from './job';
import { RateLimiter } from './limiter';
import {
  WorkerOptions,
  NativeQueueBinding,
  NativeRateLimiterBinding,
  RateLimiterConfig,
} from './types';

export type JobProcessor<TData = any, TResult = any> = (
  job: Job<TData, TResult>
) => Promise<TResult>;

/**
 * Production-grade Worker.
 * Provides multi-threaded concurrency, integrated rate limiting,
 * automated exponential backoff retries, and zero V8 GC stalling.
 */
export class Worker<TData = any, TResult = any> extends EventEmitter {
  public readonly name: string;
  private processor: JobProcessor<TData, TResult>;
  private nativeQueue: NativeQueueBinding;
  private nativeLimiter?: NativeRateLimiterBinding;
  private rateLimiter?: RateLimiter;
  private concurrency: number;
  private pollIntervalMs: number;
  private isRunning: boolean = false;
  private isPaused: boolean = false;
  private activeJobs: number = 0;
  private closePromise: Promise<void> | null = null;
  private closeResolve: (() => void) | null = null;

  constructor(
    queueName: string,
    processor: JobProcessor<TData, TResult>,
    nativeQueue: NativeQueueBinding,
    options: WorkerOptions = {},
    nativeLimiter?: NativeRateLimiterBinding
  ) {
    super();
    if (typeof queueName !== 'string' || queueName.trim().length === 0) {
      throw new TypeError(`Worker: A non-empty string is required for queue name, received ${queueName === null ? 'null' : typeof queueName}`);
    }
    if (typeof processor !== 'function') {
      throw new TypeError(`Worker: Expected a processor function, received ${processor === null ? 'null' : typeof processor}`);
    }
    this.name = queueName;
    this.processor = processor;
    this.nativeQueue = nativeQueue;
    this.concurrency = Math.max(1, options.concurrency ?? 1);
    this.pollIntervalMs = options.pollIntervalMs ?? 30;

    if (options.limiter && nativeLimiter) {
      this.nativeLimiter = nativeLimiter;
      const lim = options.limiter as any;
      const fullLimiterConfig: RateLimiterConfig = {
        capacity: lim.capacity ?? lim.max ?? 10,
        refillRate: lim.refillRate ?? lim.max ?? 10,
        refillIntervalMs: lim.refillIntervalMs ?? lim.duration ?? 1000,
        maxConcurrent: lim.maxConcurrent,
        minTimeMs: lim.minTimeMs,
        coolDownMs: lim.coolDownMs,
      };
      this.rateLimiter = new RateLimiter(
        nativeLimiter,
        fullLimiterConfig,
        options.groupKey || `worker:${queueName}`
      );
    }

    if (options.autoStart !== false) {
      this.start();
    }
  }

  /**
   * Starts the worker processing loop
   */
  public start(): void {
    if (this.isRunning) return;
    this.isRunning = true;
    for (let i = 0; i < this.concurrency; i++) {
      this.spawnLoop();
    }
  }

  /**
   * Pauses job fetching
   */
  public pause(): void {
    this.isPaused = true;
    this.emit('paused');
  }

  /**
   * Resumes job fetching
   */
  public resume(): void {
    this.isPaused = false;
    this.emit('resumed');
  }

  /**
   * Gracefully shuts down the worker, waiting for in-flight jobs to finish
   */
  public async close(): Promise<void> {
    if (!this.isRunning) return;
    this.isRunning = false;

    if (this.rateLimiter) {
      await this.rateLimiter.stop();
    }

    if (this.activeJobs === 0) {
      this.emit('closed');
      return;
    }

    this.closePromise = new Promise<void>((resolve) => {
      this.closeResolve = resolve;
    });

    return this.closePromise;
  }

  /**
   * Continuous loop executed for each concurrency worker slot
   */
  private async spawnLoop(): Promise<void> {
    while (this.isRunning) {
      if (this.isPaused) {
        await this.sleep(100);
        continue;
      }

      try {
        // If worker has an integrated rate limiter, acquire token before fetching
        if (this.rateLimiter) {
          const res = await this.nativeLimiter!.acquire(`worker:${this.name}`, 1);
          if (!res.allowed) {
            const delay = Math.max(res.retryAfterMs || 25, 10);
            await this.sleep(delay);
            continue;
          }
        }

        const dto = await this.nativeQueue.fetchNext();
        if (!dto) {
          // Queue is empty or paused: back off briefly
          if (this.rateLimiter && this.nativeLimiter) {
            try {
              if (typeof (this.nativeLimiter as any).refund === 'function') {
                await (this.nativeLimiter as any).refund(`worker:${this.name}`, 1);
              } else {
                await this.nativeLimiter.release(`worker:${this.name}`);
              }
            } catch {}
          }
          await this.sleep(this.pollIntervalMs);
          continue;
        }

        // Job received! Execute processor
        this.activeJobs++;
        const job = new Job<TData, TResult>(dto, this.nativeQueue);
        this.emit('active', job);

        const lockDuration = 30000;
        const lockRenewInterval = Math.max(100, Math.floor(lockDuration / 3));
        let heartbeatTimer: NodeJS.Timeout | null = null;
        let lockLost = false;

        if (job.lockToken) {
          heartbeatTimer = setInterval(async () => {
            try {
              const renewed = await this.nativeQueue.renewLock(job.id, job.lockToken!, lockDuration);
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
          if (lockLost) {
            throw new Error(`LOCK_LOST: Job ${job.id} was recovered by another worker while executing`);
          }
          const returnBuf = Job.serializePayload(result);
          await this.nativeQueue.complete(job.id, returnBuf, job.lockToken || null);
          this.emit('completed', job, result);
        } catch (err: any) {
          const errorMsg = err?.message || String(err);
          if (!lockLost) {
            try {
              const permanentlyFailed = await this.nativeQueue.fail(job.id, errorMsg, job.lockToken || null);
              if (permanentlyFailed) {
                this.emit('failed', job, err);
              } else {
                this.emit('retry', job, err);
              }
            } catch {
              this.emit('failed', job, err);
            }
          } else {
            this.emit('failed', job, err);
          }
        } finally {
          if (heartbeatTimer) clearInterval(heartbeatTimer);
          this.activeJobs--;
          if (this.rateLimiter && this.nativeLimiter) {
            await this.nativeLimiter.release(`worker:${this.name}`);
          }

          if (!this.isRunning && this.activeJobs === 0 && this.closeResolve) {
            this.closeResolve();
            this.emit('closed');
          }
        }
      } catch (err) {
        this.emit('error', err);
        await this.sleep(this.pollIntervalMs);
      }
    }
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}
