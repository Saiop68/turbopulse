import { EventEmitter } from 'events';
import { RateLimiterConfig, RateLimiterStatus, RateLimitResult, NativeRateLimiterBinding } from './types';
import { RateLimitExceededError, ConcurrencyLimitError } from './errors';

interface QueuedTask<T> {
  id: string;
  key: string;
  priority: number;
  tokens: number;
  fn: () => Promise<T>;
  resolve: (value: T | PromiseLike<T>) => void;
  reject: (reason?: any) => void;
  enqueuedAt: number;
}

export interface ScheduleOptions {
  priority?: number;
  weight?: number;
  id?: string;
  key?: string;
  expiration?: number;
}

/**
 * Production-grade RateLimiter.
 * Uses native Rust DashMap or Redis atomic Lua scripts for zero-latency,
 * zero V8 GC overhead rate limiting and concurrency management.
 */
export class RateLimiter extends EventEmitter {
  private native: NativeRateLimiterBinding;
  private config: RateLimiterConfig;
  private defaultKey: string;
  private queue: QueuedTask<any>[] = [];
  private activeCount: number = 0;
  private isDraining: boolean = false;
  private drainTimer: NodeJS.Timeout | null = null;
  private isStopped: boolean = false;

  constructor(native: NativeRateLimiterBinding, config: RateLimiterConfig, defaultKey: string = 'default') {
    super();
    this.native = native;
    this.config = config;
    this.defaultKey = defaultKey;
  }

  /**
   * Schedules a task for rate-limited execution.
   * If rate or concurrency limits are exceeded, queues task and retries automatically.
   */
  public async schedule<T>(fn: () => Promise<T>): Promise<T>;
  public async schedule<T>(options: ScheduleOptions, fn: () => Promise<T>): Promise<T>;
  public async schedule<T>(arg1: ScheduleOptions | (() => Promise<T>), arg2?: () => Promise<T>): Promise<T> {
    if (this.isStopped) {
      throw new Error('RateLimiter is stopped. Cannot accept new tasks.');
    }

    const opts: ScheduleOptions = typeof arg1 === 'function' ? {} : (arg1 || {});
    const taskFn: () => Promise<T> = typeof arg1 === 'function' ? arg1 : arg2!;

    if (typeof taskFn !== 'function') {
      throw new TypeError(`RateLimiter.schedule: Expected an executable function, received ${taskFn === null ? 'null' : typeof taskFn}`);
    }

    const key = opts.key || this.defaultKey;
    const priority = opts.priority ?? 0;
    const tokens = opts.weight ?? 1;

    return new Promise<T>((resolve, reject) => {
      const task: QueuedTask<T> = {
        id: opts.id || Math.random().toString(36).substring(2, 9),
        key,
        priority,
        tokens,
        fn: taskFn,
        resolve,
        reject,
        enqueuedAt: Date.now(),
      };

      // Insert sorted by priority (descending: higher priority runs first)
      const idx = this.queue.findIndex((item) => item.priority < priority);
      if (idx === -1) {
        this.queue.push(task);
      } else {
        this.queue.splice(idx, 0, task);
      }

      this.processQueue();
    });
  }

  /**
   * Wraps an async function so all invocations pass through this rate limiter.
   */
  public wrap<TArgs extends any[], TReturn>(
    fn: (...args: TArgs) => Promise<TReturn>,
    opts?: ScheduleOptions
  ): (...args: TArgs) => Promise<TReturn> {
    if (typeof fn !== 'function') {
      throw new TypeError(`RateLimiter.wrap: Expected a function to wrap, received ${fn === null ? 'null' : typeof fn}`);
    }
    return (...args: TArgs) => this.schedule(opts || {}, () => fn(...args));
  }

  /**
   * Callback submission API
   */
  public submit<T>(fn: () => Promise<T>, cb: (err: any, result?: T) => void): void;
  public submit<T>(options: ScheduleOptions, fn: () => Promise<T>, cb: (err: any, result?: T) => void): void;
  public submit<T>(...args: any[]): void {
    let opts: ScheduleOptions = {};
    let fn: () => Promise<T>;
    let cb: (err: any, result?: T) => void;

    if (args.length === 2) {
      fn = args[0];
      cb = args[1];
    } else {
      opts = args[0] || {};
      fn = args[1];
      cb = args[2];
    }

    if (typeof cb !== 'function') {
      throw new TypeError(`RateLimiter.submit: Expected a callback function, received ${cb === null ? 'null' : typeof cb}`);
    }

    if (typeof fn !== 'function') {
      cb(new TypeError(`RateLimiter.submit: Expected an executable function, received ${fn === null ? 'null' : typeof fn}`));
      return;
    }

    this.schedule(opts, fn)
      .then((res) => cb(null, res))
      .catch((err) => cb(err));
  }

  /**
   * Fetches current tokens remaining in bucket
   */
  public async currentReservoir(key?: string): Promise<number> {
    const status = await this.native.status(key || this.defaultKey);
    return status.remainingTokens;
  }

  /**
   * Returns current active concurrency count
   */
  public async currentActive(key?: string): Promise<number> {
    const status = await this.native.status(key || this.defaultKey);
    return status.activeConcurrency;
  }

  /**
   * Resets tokens and active counters
   */
  public async reset(key?: string): Promise<void> {
    await this.native.reset(key || this.defaultKey);
  }

  /**
   * Gracefully stop the rate limiter
   */
  public async stop(): Promise<void> {
    this.isStopped = true;
    if (this.drainTimer) {
      clearTimeout(this.drainTimer);
      this.drainTimer = null;
    }
    // Reject remaining queued tasks
    while (this.queue.length > 0) {
      const task = this.queue.shift();
      if (task) {
        task.reject(new Error('RateLimiter stopped'));
        this.emit('dropped', task);
      }
    }
  }

  /**
   * Internal queue processor loop: acquires native token and executes tasks
   */
  private async processQueue(): Promise<void> {
    if (this.isDraining || this.isStopped || this.queue.length === 0) {
      if (this.queue.length === 0 && this.activeCount === 0) {
        this.emit('idle');
      }
      return;
    }

    this.isDraining = true;

    try {
      while (this.queue.length > 0 && !this.isStopped) {
        const nextTask = this.queue[0]; // peek
        const res = await this.native.acquire(nextTask.key, nextTask.tokens);

        if (res.allowed) {
          // Token and concurrency granted by native backend!
          this.queue.shift(); // remove from queue
          this.activeCount++;

          // Execute task asynchronously
          this.executeTask(nextTask);

          if (this.queue.length === 0) {
            this.emit('empty');
          }
        } else {
          // Throttled: schedule next attempt after recommended wait time
          this.emit('depleted', res);
          const waitTime = Math.max(res.retryAfterMs || 20, 10);

          if (this.drainTimer) {
            clearTimeout(this.drainTimer);
          }

          this.drainTimer = setTimeout(() => {
            this.drainTimer = null;
            this.processQueue();
          }, waitTime);

          break; // Stop loop, wait for timer
        }
      }
    } catch (err) {
      this.emit('error', err);
    } finally {
      this.isDraining = false;
    }
  }

  private async executeTask<T>(task: QueuedTask<T>): Promise<void> {
    try {
      const result = await task.fn();
      task.resolve(result);
    } catch (err) {
      task.reject(err);
    } finally {
      this.activeCount--;
      try {
        await this.native.release(task.key);
      } catch (e) {
        this.emit('error', e);
      }
      // Re-trigger queue processing when slot frees up
      if (this.queue.length > 0 || this.activeCount === 0) {
        setImmediate(() => this.processQueue());
      }
    }
  }
}

/**
 * Multi-tenant RateLimiter Group.
 * Manages separate rate limits per user, route, or API key with shared configuration.
 */
export class Group {
  private native: NativeRateLimiterBinding;
  private config: RateLimiterConfig;
  private limitersMap: Map<string, RateLimiter> = new Map();

  constructor(native: NativeRateLimiterBinding, config: RateLimiterConfig) {
    this.native = native;
    this.config = config;
  }

  public key(id: string): RateLimiter {
    if (typeof id !== 'string' || id.trim().length === 0) {
      throw new TypeError(`Group.key: Expected a non-empty string identifier for key, received ${id === null ? 'null' : typeof id}`);
    }
    let limiter = this.limitersMap.get(id);
    if (!limiter) {
      limiter = new RateLimiter(this.native, this.config, id);
      this.limitersMap.set(id, limiter);
    }
    return limiter;
  }

  public async deleteKey(id: string): Promise<void> {
    if (typeof id !== 'string' || id.trim().length === 0) {
      throw new TypeError(`Group.deleteKey: Expected a non-empty string identifier for key, received ${id === null ? 'null' : typeof id}`);
    }
    const limiter = this.limitersMap.get(id);
    if (limiter) {
      await limiter.stop();
      await this.native.reset(id);
      this.limitersMap.delete(id);
    }
  }

  public limiters(): Map<string, RateLimiter> {
    return new Map(this.limitersMap);
  }
}
