import { RateLimiter, Group } from './limiter';
import { Queue } from './queue';
import { Worker, JobProcessor } from './worker';
import {
  EngineConfig,
  EngineMode,
  RateLimiterConfig,
  WorkerOptions,
  NativeRateLimiterBinding,
  NativeQueueBinding,
} from './types';

export interface NativeEngineBinding {
  createRateLimiter(config?: RateLimiterConfig | null): NativeRateLimiterBinding;
  createQueue(name: string): NativeQueueBinding;
  mode: EngineMode;
}

export interface NativeEngineConstructor {
  new (config?: EngineConfig | null): NativeEngineBinding;
  create(config: EngineConfig): Promise<NativeEngineBinding>;
}

/**
 * TurboPulse: High-performance, zero-copy distributed rate limiting and job queue engine
 * powered by a native Rust backend via napi-rs.
 */
import { getNativeEngine } from './native_loader';

let sharedInMemoryEngine: any = null;
const sharedRedisEngines = new Map<string, Promise<TurboPulse>>();

export function getSharedInMemoryNativeEngine(): any {
  if (!sharedInMemoryEngine) {
    const EngineClass = getNativeEngine();
    sharedInMemoryEngine = new EngineClass({ mode: EngineMode.InMemory });
  }
  return sharedInMemoryEngine;
}

/**
 * Normalizes any Redis connection target into a standard connection URL.
 * Supports:
 * - Connection string (e.g., 'redis://127.0.0.1:6379')
 * - IORedis client instances (new Redis(...))
 * - node-redis client instances (createClient(...))
 * - Options objects ({ host, port, password, username, db, tls })
 */
export function extractRedisUrl(target: any): string {
  if (!target) {
    return 'redis://127.0.0.1:6379';
  }

  if (typeof target === 'string') {
    return target;
  }

  if (target.redisUrl && typeof target.redisUrl === 'string') {
    return target.redisUrl;
  }

  // Handle ioredis / node-redis instances, connection classes, or raw options
  const opts =
    target.client?.options ||
    target.options ||
    target.clientOptions ||
    (target.connector && target.connector.options) ||
    target;

  // 1. Direct url property
  if (typeof opts.url === 'string') {
    return opts.url;
  }

  // 2. Protocol (rediss:// for TLS/SSL)
  const isTls = Boolean(opts.tls || opts.ssl || (opts.socket && opts.socket.tls));
  const protocol = isTls ? 'rediss' : 'redis';

  // 3. Auth
  const username = opts.username || (opts.socket && opts.socket.username) || '';
  const password = opts.password || (opts.socket && opts.socket.password) || '';
  let authPart = '';
  if (password) {
    authPart = username
      ? `${encodeURIComponent(username)}:${encodeURIComponent(password)}@`
      : `:${encodeURIComponent(password)}@`;
  }

  // 4. Host & Port
  const host = opts.host || (opts.socket && opts.socket.host) || '127.0.0.1';
  const port = opts.port || (opts.socket && opts.socket.port) || 6379;

  // 5. Database path
  const db = opts.db ?? opts.database ?? (opts.socket && opts.socket.database);
  const dbPart = db !== undefined && db !== null && db !== '' ? `/${db}` : '';

  return `${protocol}://${authPart}${host}:${port}${dbPart}`;
}

export function getSharedRedisEngine(target: any = 'redis://127.0.0.1:6379'): Promise<TurboPulse> {
  const redisUrl = extractRedisUrl(target);
  let existing = sharedRedisEngines.get(redisUrl);
  if (!existing) {
    existing = TurboPulse.create({
      mode: EngineMode.Redis,
      redisUrl,
      keyPrefix: 'turbopulse',
    });
    sharedRedisEngines.set(redisUrl, existing);
  }
  return existing;
}

export class TurboPulse {
  private nativeEngine: NativeEngineBinding;
  private defaultLimiterBinding?: NativeRateLimiterBinding;

  constructor(nativeEngine: NativeEngineBinding) {
    this.nativeEngine = nativeEngine;
  }

  /**
   * Initializes TurboPulse asynchronously (supports Redis and In-Memory modes)
   */
  public static async create(
    arg1?: NativeEngineConstructor | EngineConfig,
    arg2?: EngineConfig
  ): Promise<TurboPulse> {
    let EngineClass: NativeEngineConstructor;
    let config: EngineConfig;

    if (typeof arg1 === 'function') {
      EngineClass = arg1;
      config = arg2 || { mode: EngineMode.InMemory };
    } else {
      EngineClass = getNativeEngine();
      config = arg1 || { mode: EngineMode.InMemory };
    }

    const native = await EngineClass.create(config);
    return new TurboPulse(native);
  }

  /**
   * Synchronous initialization for In-Memory mode
   */
  public static createInMemory(NativeEngineClass?: NativeEngineConstructor): TurboPulse {
    if (!NativeEngineClass) {
      return new TurboPulse(getSharedInMemoryNativeEngine());
    }
    const native = new NativeEngineClass({ mode: EngineMode.InMemory });
    return new TurboPulse(native);
  }

  /**
   * Synchronous alias for createInMemory
   */
  public static createSync(config: EngineConfig = { mode: EngineMode.InMemory }): TurboPulse {
    if (config.mode === EngineMode.InMemory) {
      return new TurboPulse(getSharedInMemoryNativeEngine());
    }
    const EngineClass = getNativeEngine();
    const native = new EngineClass(config);
    return new TurboPulse(native);
  }

  /**
   * Creates a RateLimiter
   */
  public createRateLimiter(
    config: Partial<RateLimiterConfig> = {},
    key: string = 'default'
  ): RateLimiter {
    const fullConfig: RateLimiterConfig = {
      capacity: config.capacity ?? 10,
      refillRate: config.refillRate ?? 10,
      refillIntervalMs: config.refillIntervalMs ?? 1000,
      maxConcurrent: config.maxConcurrent,
      minTimeMs: config.minTimeMs,
      coolDownMs: config.coolDownMs,
    };

    const nativeBinding = this.nativeEngine.createRateLimiter(fullConfig);
    return new RateLimiter(nativeBinding, fullConfig, key);
  }

  /**
   * Creates a high-throughput Leased Rate Limiter capable of 1,000,000+ ops/sec
   * with distributed quota synchronization across a Redis cluster.
   */
  public createLeasedRateLimiter(
    config: Partial<RateLimiterConfig> = {},
    key: string = 'default',
    opts?: import('./leased_limiter').LeasedLimiterOptions
  ): import('./leased_limiter').LeasedRateLimiter {
    const fullConfig: RateLimiterConfig = {
      capacity: config.capacity ?? 1_000_000,
      refillRate: config.refillRate ?? 1_000_000,
      refillIntervalMs: config.refillIntervalMs ?? 1000,
      maxConcurrent: config.maxConcurrent,
      minTimeMs: config.minTimeMs,
      coolDownMs: config.coolDownMs,
    };

    const nativeBinding = this.nativeEngine.createRateLimiter(fullConfig);
    const { LeasedRateLimiter } = require('./leased_limiter');
    return new LeasedRateLimiter(nativeBinding, fullConfig, key, opts);
  }

  /**
   * Creates a multi-tenant RateLimiter Group
   */
  public createGroup(config: Partial<RateLimiterConfig> = {}): Group {
    const fullConfig: RateLimiterConfig = {
      capacity: config.capacity ?? 10,
      refillRate: config.refillRate ?? 10,
      refillIntervalMs: config.refillIntervalMs ?? 1000,
      maxConcurrent: config.maxConcurrent,
      minTimeMs: config.minTimeMs,
      coolDownMs: config.coolDownMs,
    };

    const nativeBinding = this.nativeEngine.createRateLimiter(fullConfig);
    return new Group(nativeBinding, fullConfig);
  }

  /**
   * Creates a high-throughput Queue
   */
  public createQueue<TData = any, TResult = any>(name: string): Queue<TData, TResult> {
    const nativeQueue = this.nativeEngine.createQueue(name);
    return new Queue<TData, TResult>(name, nativeQueue);
  }

  /**
   * Creates a Worker with integrated concurrency and rate limiting
   */
  public createWorker<TData = any, TResult = any>(
    queueName: string,
    processor: JobProcessor<TData, TResult>,
    options: WorkerOptions = {}
  ): Worker<TData, TResult> {
    const nativeQueue = this.nativeEngine.createQueue(queueName);
    let nativeLimiter: NativeRateLimiterBinding | undefined;

    if (options.limiter) {
      const lim = options.limiter as any;
      const conf: RateLimiterConfig = {
        capacity: lim.capacity ?? lim.max ?? 10,
        refillRate: lim.refillRate ?? lim.max ?? 10,
        refillIntervalMs: lim.refillIntervalMs ?? lim.duration ?? 1000,
        maxConcurrent: lim.maxConcurrent,
        minTimeMs: lim.minTimeMs,
        coolDownMs: lim.coolDownMs,
      };
      nativeLimiter = this.nativeEngine.createRateLimiter(conf);
    }

    return new Worker<TData, TResult>(
      queueName,
      processor,
      nativeQueue,
      options,
      nativeLimiter
    );
  }

  public get mode(): EngineMode {
    return this.nativeEngine.mode;
  }
}
