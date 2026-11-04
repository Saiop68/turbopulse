import { EventEmitter } from 'events';
import {
  LimiterOptions,
  LimiterJobOptions,
  RateLimiterStrategy,
  EngineMode,
  RateLimiterConfig,
  Ticket,
} from './types';
import { TurboPulse, getSharedRedisEngine, extractRedisUrl } from './engine';

/**
 * Drop-in compatible IORedis connection manager.
 * Eliminates duplicate subscriber connections via native zero-overhead multiplexing.
 */
export class IORedisConnection extends EventEmitter {
  public datastore = 'ioredis';
  public client: any;
  public ready: Promise<any>;
  public redisUrl: string;

  constructor(options: { client?: any; clientOptions?: any; clusterNodes?: any } = {}) {
    super();
    this.client = options.client;
    this.redisUrl = extractRedisUrl(options.client || options.clientOptions || options);
    this.ready = getSharedRedisEngine(this.redisUrl);
  }

  public async disconnect(flush: boolean = true): Promise<void> {}
}

/**
 * Drop-in compatible Redis connection manager.
 */
export class RedisConnection extends EventEmitter {
  public datastore = 'redis';
  public client: any;
  public ready: Promise<any>;
  public redisUrl: string;

  constructor(options: { client?: any; clientOptions?: any } = {}) {
    super();
    this.client = options.client;
    this.redisUrl = extractRedisUrl(options.client || options.clientOptions || options);
    this.ready = getSharedRedisEngine(this.redisUrl);
  }

  public async disconnect(flush: boolean = true): Promise<void> {}
}
/**
 * Global Hierarchical Timer Wheel for all RateLimiter instances.
 * Eliminates timer proliferation (e.g. 50,000 timers in multi-tenant groups)
 * by multiplexing all pending limiter drains down to a single master event loop timer.
 */
class GlobalLimiterWheel {
  private pending = new Map<RateLimiter, number>();
  private masterTimer: NodeJS.Timeout | null = null;
  private nextDeadline: number = Infinity;

  public schedule(limiter: RateLimiter, delayMs: number): void {
    const deadline = Date.now() + Math.max(1, delayMs);
    this.pending.set(limiter, deadline);

    if (deadline < this.nextDeadline) {
      this.reschedule(deadline);
    }
  }

  public cancel(limiter: RateLimiter): void {
    this.pending.delete(limiter);
    if (this.pending.size === 0 && this.masterTimer) {
      clearTimeout(this.masterTimer);
      this.masterTimer = null;
      this.nextDeadline = Infinity;
    }
  }

  private reschedule(earliest: number): void {
    if (this.masterTimer) {
      clearTimeout(this.masterTimer);
    }
    this.nextDeadline = earliest;
    const delay = Math.max(0, earliest - Date.now());
    this.masterTimer = setTimeout(() => this.tick(), delay);
  }

  private tick(): void {
    this.masterTimer = null;
    this.nextDeadline = Infinity;
    const now = Date.now();
    const readyToDrain: RateLimiter[] = [];

    for (const [limiter, deadline] of this.pending.entries()) {
      if (deadline <= now) {
        this.pending.delete(limiter);
        readyToDrain.push(limiter);
      }
    }

    for (let i = 0; i < readyToDrain.length; i++) {
      readyToDrain[i].triggerDrain();
    }

    if (this.pending.size > 0) {
      let earliest = Infinity;
      for (const deadline of this.pending.values()) {
        if (deadline < earliest) {
          earliest = deadline;
        }
      }
      if (earliest !== Infinity) {
        this.reschedule(earliest);
      }
    }
  }
}

const globalLimiterWheel = new GlobalLimiterWheel();

/**
 * High-performance Rate Limiter Group.
 * Provides fine-grained multi-tenant rate limiting and concurrency management.
 */
export class RateLimiterGroup {
  public options: LimiterOptions;
  public instances: Map<string, RateLimiter> = new Map();

  constructor(options: LimiterOptions = {}) {
    if (options != null && typeof options !== 'object') {
      throw new TypeError(`RateLimiterGroup constructor: Expected options to be an object, received ${typeof options}`);
    }
    this.options = options || {};
  }

  public key(id: string): RateLimiter {
    if (typeof id !== 'string' || id.trim().length === 0) {
      throw new TypeError(
        `RateLimiterGroup.key: Expected a non-empty string identifier for key, received ${
          id === null ? 'null' : typeof id
        }`
      );
    }
    let instance = this.instances.get(id);
    if (!instance) {
      instance = new RateLimiter({ ...this.options, id });
      this.instances.set(id, instance);
    }
    return instance;
  }

  public async deleteKey(id: string): Promise<void> {
    if (typeof id !== 'string' || id.trim().length === 0) {
      throw new TypeError(
        `RateLimiterGroup.deleteKey: Expected a non-empty string identifier for key, received ${
          id === null ? 'null' : typeof id
        }`
      );
    }
    const instance = this.instances.get(id);
    if (instance) {
      await instance.stop();
      this.instances.delete(id);
    }
  }

  public limiters(): { key: string; limiter: RateLimiter }[] {
    return Array.from(this.instances.entries()).map(([key, limiter]) => ({ key, limiter }));
  }

  public updateSettings(options: LimiterOptions): void {
    if (options == null || typeof options !== 'object') {
      throw new TypeError(
        `RateLimiterGroup.updateSettings: Expected an options object, received ${
          options === null ? 'null' : typeof options
        }`
      );
    }
    this.options = { ...this.options, ...options };
    for (const instance of this.instances.values()) {
      instance.updateSettings(options);
    }
  }

  public async schedule<T>(key: string, fn: () => Promise<T> | T): Promise<T>;
  public async schedule<T, A extends any[]>(key: string, fn: (...args: A) => Promise<T> | T, ...args: A): Promise<T>;
  public async schedule<T>(key: string, options: LimiterJobOptions, fn: () => Promise<T> | T): Promise<T>;
  public async schedule<T, A extends any[]>(key: string, options: LimiterJobOptions, fn: (...args: A) => Promise<T> | T, ...args: A): Promise<T>;
  public async schedule<T>(key: string, ...args: any[]): Promise<T> {
    if (typeof key !== 'string' || key.trim().length === 0) {
      throw new TypeError(
        `RateLimiterGroup.schedule: Expected a non-empty string for key, received ${
          key === null ? 'null' : typeof key
        }`
      );
    }
    return (this.key(key) as any).schedule(...args);
  }

  public wrap<TArgs extends any[], TReturn>(
    key: string,
    fn: (...args: TArgs) => Promise<TReturn> | TReturn,
    jobOpts?: LimiterJobOptions
  ): (...args: TArgs) => Promise<TReturn> {
    if (typeof key !== 'string' || key.trim().length === 0) {
      throw new TypeError(
        `RateLimiterGroup.wrap: Expected a non-empty string for key, received ${
          key === null ? 'null' : typeof key
        }`
      );
    }
    if (typeof fn !== 'function') {
      throw new TypeError(
        `RateLimiterGroup.wrap: Expected a function to wrap, received ${
          fn === null ? 'null' : typeof fn
        }`
      );
    }
    return this.key(key).wrap(fn, jobOpts);
  }

  public async getTicket(key: string, options?: LimiterJobOptions | number): Promise<Ticket> {
    if (typeof key !== 'string' || key.trim().length === 0) {
      throw new TypeError(
        `RateLimiterGroup.getTicket: Expected a non-empty string for key, received ${
          key === null ? 'null' : typeof key
        }`
      );
    }
    return this.key(key).getTicket(options);
  }

  public async tryTicket(key: string, weight: number = 1): Promise<Ticket> {
    if (typeof key !== 'string' || key.trim().length === 0) {
      throw new TypeError(
        `RateLimiterGroup.tryTicket: Expected a non-empty string for key, received ${
          key === null ? 'null' : typeof key
        }`
      );
    }
    return this.key(key).tryTicket(weight);
  }
}

/**
 * Production-grade RateLimiter powered by TurboPulse native Rust engine.
 * Delivers multi-million ops/sec throughput in-memory and on Redis with zero V8 GC pauses.
 */
export class RateLimiter extends EventEmitter {
  public static strategy = {
    LEAK: RateLimiterStrategy.LEAK,
    OVERFLOW: RateLimiterStrategy.OVERFLOW,
    BLOCK: RateLimiterStrategy.BLOCK,
    LEAK_PRIORITY: RateLimiterStrategy.LEAK_PRIORITY,
  };

  public static Group = RateLimiterGroup;
  public static IORedisConnection = IORedisConnection;
  public static RedisConnection = RedisConnection;

  private options: LimiterOptions;
  private id: string;
  private queue: Array<{
    id: string;
    priority: number;
    weight: number;
    fn: () => Promise<any>;
    resolve: (res: any) => void;
    reject: (err: any) => void;
    enqueuedAt: number;
    expiration?: number;
  }> = [];
  private activeCount: number = 0;
  private executedCount: number = 0;
  private failedCount: number = 0;
  private receivedCount: number = 0;
  private isProcessing: boolean = false;
  private timer: NodeJS.Timeout | null = null;
  private isStopped: boolean = false;
  private chainedParent: RateLimiter | null = null;
  private nativeLimiter: any = null;
  private readyPromise: Promise<void>;

  constructor(options: LimiterOptions = {}) {
    super();
    if (options != null && typeof options !== 'object') {
      throw new TypeError(`RateLimiter constructor: Expected options to be an object, received ${typeof options}`);
    }
    if (options.minTime != null && (typeof options.minTime !== 'number' || options.minTime < 0 || isNaN(options.minTime))) {
      throw new TypeError(`RateLimiter constructor: minTime must be a non-negative number, received ${options.minTime}`);
    }
    if (options.maxConcurrent != null && (typeof options.maxConcurrent !== 'number' || options.maxConcurrent < 1 || isNaN(options.maxConcurrent))) {
      throw new TypeError(`RateLimiter constructor: maxConcurrent must be a positive number, received ${options.maxConcurrent}`);
    }
    if (options.highWater != null && (typeof options.highWater !== 'number' || options.highWater < 0 || isNaN(options.highWater))) {
      throw new TypeError(`RateLimiter constructor: highWater must be a non-negative number, received ${options.highWater}`);
    }
    if (options.reservoir != null && (typeof options.reservoir !== 'number' || options.reservoir < 0 || isNaN(options.reservoir))) {
      throw new TypeError(`RateLimiter constructor: reservoir must be a non-negative number, received ${options.reservoir}`);
    }

    this.options = {
      maxConcurrent: null as any,
      minTime: 0,
      highWater: null as any,
      strategy: RateLimiterStrategy.LEAK,
      penalty: 0,
      reservoir: null as any,
      reservoirRefreshInterval: null as any,
      reservoirRefreshAmount: null as any,
      reservoirIncreaseInterval: null as any,
      reservoirIncreaseAmount: null as any,
      reservoirIncreaseMaximum: null as any,
      rejectOnDrop: true,
      ...options,
    };

    this.id = this.options.id || `limiter_${Math.random().toString(36).substring(2, 9)}`;
    this.readyPromise = this.initEngine();
    this.readyPromise.catch(() => {});
  }

  private mapLimiterStrategy(s: any): any {
    if (s == null) return undefined;
    if (s === 0 || s === '0' || s === 'Leak' || s === 'LEAK') return 'Leak';
    if (s === 1 || s === '1' || s === 'Overflow' || s === 'OVERFLOW') return 'Overflow';
    if (s === 2 || s === '2' || s === 'Block' || s === 'BLOCK') return 'Block';
    if (s === 3 || s === '3' || s === 'LeakPriority' || s === 'LEAK_PRIORITY') return 'LeakPriority';
    return 'Leak';
  }

  private async initEngine(): Promise<void> {
    const isRedis =
      this.options.datastore === 'redis' ||
      this.options.datastore === 'ioredis' ||
      Boolean(
        this.options.redisUrl ||
        this.options.connection ||
        this.options.client ||
        this.options.clientOptions ||
        this.options.clusterNodes ||
        this.options.nodes
      );

    const config: RateLimiterConfig = {
      capacity: this.options.reservoir ?? 1_000_000,
      refillRate: this.options.reservoirRefreshAmount ?? 1_000_000,
      refillIntervalMs: this.options.reservoirRefreshInterval ?? 1000,
      maxConcurrent: this.options.maxConcurrent ?? undefined,
      minTimeMs: this.options.minTime ?? undefined,
      coolDownMs: this.options.penalty ?? undefined,
      highWater: this.options.highWater ?? undefined,
      strategy: this.mapLimiterStrategy(this.options.strategy),
      reservoir: this.options.reservoir ?? undefined,
      reservoirRefreshIntervalMs: this.options.reservoirRefreshInterval ?? undefined,
      reservoirRefreshAmount: this.options.reservoirRefreshAmount ?? undefined,
      reservoirIncreaseIntervalMs: this.options.reservoirIncreaseInterval ?? undefined,
      reservoirIncreaseAmount: this.options.reservoirIncreaseAmount ?? undefined,
      reservoirIncreaseMaximum: this.options.reservoirIncreaseMaximum ?? undefined,
    };

    if (isRedis) {
      const target =
        this.options.connection ||
        this.options.client ||
        this.options.clientOptions ||
        this.options.redisUrl ||
        this.options.clusterNodes ||
        this.options.nodes;
      const engine = await getSharedRedisEngine(target);
      this.nativeLimiter = (engine as any).nativeEngine.createRateLimiter(config);
    } else {
      const engine = TurboPulse.createSync({
        mode: EngineMode.InMemory,
      });
      this.nativeLimiter = (engine as any).nativeEngine.createRateLimiter(config);
    }
  }

  /**
   * Schedule an async function to execute through the limiter
   */
  public async schedule<T>(fn: () => Promise<T> | T): Promise<T>;
  public async schedule<T, A extends any[]>(fn: (...args: A) => Promise<T> | T, ...args: A): Promise<T>;
  public async schedule<T>(options: LimiterJobOptions, fn: () => Promise<T> | T): Promise<T>;
  public async schedule<T, A extends any[]>(options: LimiterJobOptions, fn: (...args: A) => Promise<T> | T, ...args: A): Promise<T>;
  public async schedule<T>(...args: any[]): Promise<T> {
    let jobOpts: LimiterJobOptions = {};
    let fn: (...a: any[]) => Promise<T>;
    let passedArgs: any[] = [];

    if (typeof args[0] === 'function') {
      fn = args[0];
      passedArgs = args.slice(1);
    } else if (args[0] && typeof args[0] === 'object' && typeof args[1] === 'function') {
      jobOpts = args[0];
      fn = args[1];
      passedArgs = args.slice(2);
    } else if (args[0] && typeof args[0] === 'object') {
      jobOpts = args[0];
      fn = args[1];
      passedArgs = args.slice(2);
    } else {
      fn = args[0];
      passedArgs = args.slice(1);
    }

    if (typeof fn !== 'function') {
      const received = fn === null ? 'null' : typeof fn;
      throw new TypeError(`RateLimiter.schedule: Expected an executable function as target, received ${received}`);
    }

    await this.readyPromise;

    if (this.isStopped) {
      const err = new Error('This limiter is stopped');
      if (this.options.rejectOnDrop) throw err;
      return undefined as any;
    }

    this.receivedCount++;
    const priority = jobOpts.priority ?? 5; // Default priority is 5 (0 highest, 9 lowest)
    const weight = jobOpts.weight ?? 1;
    const expiration = jobOpts.expiration;
    const jobId = jobOpts.id || Math.random().toString(36).substring(2, 9);

    return new Promise<T>((resolve, reject) => {
      // Check highWater & strategy
      if (this.options.highWater != null && this.queue.length >= this.options.highWater) {
        const strategy = Number(this.options.strategy ?? RateLimiterStrategy.LEAK);
        if (strategy === RateLimiterStrategy.OVERFLOW) {
          this.emit('dropped', { id: jobId, priority, weight });
          if (this.options.rejectOnDrop) {
            return reject(new Error('This job has been dropped by OVERFLOW strategy'));
          }
          return;
        } else if (strategy === RateLimiterStrategy.BLOCK) {
          while (this.queue.length > 0) {
            const dropped = this.queue.shift();
            if (dropped) {
              this.emit('dropped', dropped);
              if (this.options.rejectOnDrop) dropped.reject(new Error('Job dropped by BLOCK strategy'));
            }
          }
          this.emit('dropped', { id: jobId, priority, weight });
          if (this.options.rejectOnDrop) {
            return reject(new Error('Limiter entered BLOCKED mode'));
          }
          return;
        } else if (strategy === RateLimiterStrategy.LEAK || strategy === RateLimiterStrategy.LEAK_PRIORITY) {
          let lowestIdx = 0;
          for (let i = 1; i < this.queue.length; i++) {
            if (this.queue[i].priority > this.queue[lowestIdx].priority) {
              lowestIdx = i;
            }
          }
          if (this.queue[lowestIdx].priority >= priority) {
            const dropped = this.queue.splice(lowestIdx, 1)[0];
            this.emit('dropped', dropped);
            if (this.options.rejectOnDrop) dropped.reject(new Error('Job dropped by LEAK strategy'));
          } else {
            this.emit('dropped', { id: jobId, priority, weight });
            if (this.options.rejectOnDrop) {
              return reject(new Error('Job dropped by LEAK strategy'));
            }
            return;
          }
        }
      }

      const task = {
        id: jobId,
        priority,
        weight,
        fn: () => fn(...passedArgs),
        resolve,
        reject,
        enqueuedAt: Date.now(),
        expiration,
        retries: jobOpts.retries,
        retryDelay: jobOpts.retryDelay,
        retryCount: 0,
      };

      const idx = this.queue.findIndex((item) => item.priority > priority);
      if (idx === -1) {
        this.queue.push(task);
      } else {
        this.queue.splice(idx, 0, task);
      }

      this.processQueue();
    });
  }

  /**
   * Wrap a function to execute via this limiter
   */
  public wrap<TArgs extends any[], TReturn>(
    fn: (...args: TArgs) => Promise<TReturn> | TReturn,
    jobOpts?: LimiterJobOptions
  ): (...args: TArgs) => Promise<TReturn> {
    if (typeof fn !== 'function') {
      throw new TypeError(`RateLimiter.wrap: Expected a function to wrap, received ${fn === null ? 'null' : typeof fn}`);
    }
    return (...args: TArgs) => this.schedule(jobOpts || {}, () => fn(...args));
  }

  /**
   * Submit callback-style job
   */
  public submit<T>(...args: any[]): void {
    let opts: LimiterJobOptions = {};
    let fn: any;
    let cb: (err: any, res?: T) => void;
    let passedArgs: any[] = [];

    if (typeof args[0] === 'function') {
      fn = args[0];
      cb = args[args.length - 1];
      passedArgs = args.slice(1, -1);
    } else if (args[0] && typeof args[0] === 'object') {
      opts = args[0];
      fn = args[1];
      cb = args[args.length - 1];
      passedArgs = args.slice(2, -1);
    } else {
      fn = args[0];
      cb = args[args.length - 1];
    }

    if (typeof cb !== 'function') {
      throw new TypeError(
        `RateLimiter.submit: Expected a callback function as the last argument, received ${
          cb === null ? 'null' : typeof cb
        }`
      );
    }

    if (typeof fn !== 'function') {
      cb(new TypeError(`RateLimiter.submit: Expected an executable function, received ${fn === null ? 'null' : typeof fn}`));
      return;
    }

    this.schedule(opts, () => fn(...passedArgs))
      .then((res: any) => cb(null, res))
      .catch((err) => cb(err));
  }

  /**
   * Acquires an execution ticket from the limiter.
   * Suspends and waits in line until rate and concurrency capacity become available.
   * Call `ticket.release()` (or `ticket.done()`) when execution is finished.
   */
  public async getTicket(options?: LimiterJobOptions | number): Promise<Ticket> {
    if (typeof options === 'number' && (isNaN(options) || options <= 0)) {
      throw new TypeError(`RateLimiter.getTicket: Expected weight to be a positive number, received ${options}`);
    }
    await this.readyPromise;
    const opts: LimiterJobOptions = typeof options === 'number' ? { weight: options } : options || {};
    return new Promise<Ticket>((resolve, reject) => {
      this.schedule(opts, () => {
        return new Promise<void>((taskResolve) => {
          let released = false;
          let expirationTimer: NodeJS.Timeout | null = null;

          const releaseTicket = () => {
            if (!released) {
              released = true;
              if (expirationTimer) clearTimeout(expirationTimer);
              taskResolve();
            }
          };

          if (opts.expiration && opts.expiration > 0) {
            expirationTimer = setTimeout(releaseTicket, opts.expiration);
          }

          const ticket: Ticket = {
            id: opts.id || Math.random().toString(36).substring(2, 9),
            allowed: true,
            release: async () => { releaseTicket(); },
            done: async () => { releaseTicket(); },
          };
          resolve(ticket);
        });
      }).catch(reject);
    });
  }

  /**
   * Non-blocking ticket acquisition (ideal for HTTP middleware).
   * Checks and consumes immediately without waiting in queue.
   * Returns `{ allowed: false, retryAfterMs }` if rate limit is exceeded.
   */
  public async tryTicket(weight: number = 1): Promise<Ticket> {
    if (typeof weight !== 'number' || isNaN(weight) || weight <= 0) {
      throw new TypeError(`RateLimiter.tryTicket: Expected weight to be a positive number, received ${weight}`);
    }
    await this.readyPromise;
    if (this.nativeLimiter && typeof this.nativeLimiter.acquire === 'function') {
      const res = await this.nativeLimiter.acquire(this.id, weight);
      let released = false;
      const releaseTicket = async () => {
        if (!released) {
          released = true;
          if (res.allowed) {
            try {
              await this.nativeLimiter.release(this.id);
            } catch {}
          }
        }
      };

      return {
        id: Math.random().toString(36).substring(2, 9),
        allowed: res.allowed,
        remaining: res.remaining,
        retryAfterMs: res.retryAfterMs,
        release: releaseTicket,
        done: releaseTicket,
      };
    }
    return {
      id: Math.random().toString(36).substring(2, 9),
      allowed: true,
      remaining: 1,
      retryAfterMs: 0,
      release: async () => {},
      done: async () => {},
    };
  }

  /**
   * Checks if a request of weight N would be allowed right now
   */
  public async check(weight: number = 1): Promise<boolean> {
    if (typeof weight !== 'number' || isNaN(weight) || weight <= 0) {
      throw new TypeError(`RateLimiter.check: Expected weight to be a positive number, received ${weight}`);
    }
    await this.readyPromise;
    if (this.nativeLimiter && typeof this.nativeLimiter.check === 'function') {
      return this.nativeLimiter.check(this.id, weight);
    }
    return true;
  }

  /**
   * Returns current reservoir count
   */
  public async currentReservoir(): Promise<number | null> {
    await this.readyPromise;
    if (this.nativeLimiter && typeof this.nativeLimiter.currentReservoir === 'function') {
      return this.nativeLimiter.currentReservoir(this.id);
    }
    return this.options.reservoir ?? null;
  }

  /**
   * Increments or decrements reservoir
   */
  public async incrementReservoir(amount: number): Promise<number> {
    if (typeof amount !== 'number' || isNaN(amount)) {
      throw new TypeError(
        `RateLimiter.incrementReservoir: Expected amount to be a number, received ${
          amount === null ? 'null' : typeof amount
        }`
      );
    }
    await this.readyPromise;
    if (this.nativeLimiter && typeof this.nativeLimiter.incrementReservoir === 'function') {
      return this.nativeLimiter.incrementReservoir(this.id, amount);
    }
    this.options.reservoir = (this.options.reservoir ?? 0) + amount;
    return this.options.reservoir;
  }

  /**
   * Dynamically update limiter options at runtime
   */
  public updateSettings(options: LimiterOptions): void {
    if (options == null || typeof options !== 'object') {
      throw new TypeError(
        `RateLimiter.updateSettings: Expected an options object, received ${
          options === null ? 'null' : typeof options
        }`
      );
    }
    if (options.minTime != null && (typeof options.minTime !== 'number' || options.minTime < 0 || isNaN(options.minTime))) {
      throw new TypeError(`RateLimiter.updateSettings: minTime must be a non-negative number, received ${options.minTime}`);
    }
    if (options.maxConcurrent != null && (typeof options.maxConcurrent !== 'number' || options.maxConcurrent < 1 || isNaN(options.maxConcurrent))) {
      throw new TypeError(`RateLimiter.updateSettings: maxConcurrent must be a positive number, received ${options.maxConcurrent}`);
    }
    this.options = { ...this.options, ...options };
    if (this.nativeLimiter && typeof this.nativeLimiter.updateSettings === 'function') {
      this.nativeLimiter.updateSettings({
        capacity: this.options.reservoir ?? 1_000_000,
        refillRate: this.options.reservoirRefreshAmount ?? 1_000_000,
        refillIntervalMs: this.options.reservoirRefreshInterval ?? 1000,
        maxConcurrent: this.options.maxConcurrent ?? undefined,
        minTimeMs: this.options.minTime ?? undefined,
        coolDownMs: this.options.penalty ?? undefined,
        highWater: this.options.highWater ?? undefined,
        strategy: this.mapLimiterStrategy(this.options.strategy),
        reservoir: this.options.reservoir ?? undefined,
        reservoirRefreshIntervalMs: this.options.reservoirRefreshInterval ?? undefined,
        reservoirRefreshAmount: this.options.reservoirRefreshAmount ?? undefined,
        reservoirIncreaseIntervalMs: this.options.reservoirIncreaseInterval ?? undefined,
        reservoirIncreaseAmount: this.options.reservoirIncreaseAmount ?? undefined,
        reservoirIncreaseMaximum: this.options.reservoirIncreaseMaximum ?? undefined,
      });
    }
    this.processQueue();
  }

  /**
   * Chain this limiter to a parent limiter
   */
  public chain(parentLimiter: RateLimiter): RateLimiter {
    if (!parentLimiter || typeof (parentLimiter as any).schedule !== 'function') {
      throw new TypeError(`RateLimiter.chain: Expected a valid RateLimiter instance as parent`);
    }
    this.chainedParent = parentLimiter;
    return this;
  }

  /**
   * Returns queue counts: EXECUTING, QUEUED, RUNNING, RECEIVED, DONE, FAILED
   */
  public counts(): {
    EXECUTING: number;
    QUEUED: number;
    RUNNING: number;
    RECEIVED: number;
    DONE: number;
    FAILED: number;
  } {
    return {
      EXECUTING: this.activeCount,
      QUEUED: this.queue.length,
      RUNNING: this.activeCount,
      RECEIVED: this.receivedCount,
      DONE: this.executedCount,
      FAILED: this.failedCount,
    };
  }

  /**
   * Gracefully stop the limiter
   */
  public async stop(options: { dropWaitingJobs?: boolean; enqueueErrorMessage?: string } = {}): Promise<void> {
    this.isStopped = true;
    globalLimiterWheel.cancel(this);
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }

    if (options.dropWaitingJobs !== false) {
      const msg = options.enqueueErrorMessage || 'This limiter has been stopped';
      while (this.queue.length > 0) {
        const item = this.queue.shift();
        if (item) {
          item.reject(new Error(msg));
          this.emit('dropped', item);
        }
      }
    }
  }

  private async processQueue(): Promise<void> {
    if (this.isProcessing || this.isStopped || this.queue.length === 0) {
      if (this.queue.length === 0 && this.activeCount === 0) {
        this.emit('idle');
      }
      return;
    }

    this.isProcessing = true;

    try {
      while (this.queue.length > 0 && !this.isStopped) {
        const task = this.queue[0];

        // Check task expiration
        if (task.expiration && Date.now() - task.enqueuedAt > task.expiration) {
          this.queue.shift();
          task.reject(new Error('This job has expired'));
          this.emit('dropped', task);
          continue;
        }

        // If chained to parent, acquire parent first
        let parentAcquired = false;
        if (this.chainedParent) {
          const parentRes = await (this.chainedParent as any).nativeLimiter.acquire(
            (this.chainedParent as any).id,
            task.weight
          );
          if (!parentRes.allowed) {
            const waitTime = Math.max(parentRes.retryAfterMs || 25, 10);
            this.scheduleDrain(waitTime);
            break;
          }
          parentAcquired = true;
        }

        // Try acquire from native backend
        const res = await this.nativeLimiter.acquire(this.id, task.weight);

        if (res.allowed) {
          this.queue.shift();
          this.activeCount++;
          this.executeTask(task);

          if (this.queue.length === 0) {
            this.emit('empty');
          }
        } else {
          if (parentAcquired && this.chainedParent) {
            await (this.chainedParent as any).nativeLimiter.release((this.chainedParent as any).id);
          }
          this.emit('depleted', res);
          const waitTime = Math.max(res.retryAfterMs || 25, 10);
          this.scheduleDrain(waitTime);
          break;
        }
      }
    } catch (err) {
      this.emit('error', err);
    } finally {
      this.isProcessing = false;
    }
  }

  public triggerDrain(): void {
    if (!this.isStopped) {
      this.processQueue();
    }
  }

  private scheduleDrain(ms: number): void {
    globalLimiterWheel.schedule(this, ms);
  }

  private async executeTask(task: any): Promise<void> {
    try {
      const result = await task.fn();
      this.executedCount++;
      task.resolve(result);
    } catch (err) {
      task.retryCount = (task.retryCount || 0) + 1;
      let retryDelayMs: number | null = null;

      const failedListeners = this.listeners('failed');
      if (failedListeners.length > 0) {
        try {
          const res = await Promise.resolve(
            failedListeners[0](err, {
              id: task.id,
              retryCount: task.retryCount - 1,
              options: { priority: task.priority, weight: task.weight, id: task.id },
            })
          );
          if (typeof res === 'number') {
            retryDelayMs = res;
          }
        } catch (listenerErr) {
          this.emit('error', listenerErr);
        }
      }

      if (retryDelayMs == null && task.retries != null && task.retryCount <= task.retries) {
        retryDelayMs = task.retryDelay ?? 1000;
      }

      if (retryDelayMs != null && retryDelayMs >= 0) {
        setTimeout(() => {
          if (!this.isStopped) {
            this.queue.unshift(task);
            this.processQueue();
          } else {
            this.failedCount++;
            task.reject(err);
          }
        }, retryDelayMs);
        return;
      }

      this.failedCount++;
      task.reject(err);
    } finally {
      this.activeCount--;
      if (this.chainedParent) {
        try {
          await (this.chainedParent as any).nativeLimiter.release((this.chainedParent as any).id);
        } catch {}
      }
      try {
        await this.nativeLimiter.release(this.id);
      } catch (e) {
        this.emit('error', e);
      }
      if (this.queue.length > 0 || this.activeCount === 0) {
        setImmediate(() => this.processQueue());
      }
    }
  }

  public get binding(): any {
    return this.nativeLimiter;
  }

  public async waitUntilReady(): Promise<void> {
    await this.readyPromise;
  }
}

export default RateLimiter;
