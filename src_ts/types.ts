/**
 * TurboPulse: Core TypeScript Definitions
 * Ultra-High Performance, Zero-Copy Rate Limiter and Queue Engine
 */

export enum EngineMode {
  InMemory = 'InMemory',
  Redis = 'Redis',
}

export enum BackoffType {
  Fixed = 'Fixed',
  Exponential = 'Exponential',
  Linear = 'Linear',
}

export enum JobState {
  Waiting = 'Waiting',
  Delayed = 'Delayed',
  Active = 'Active',
  Completed = 'Completed',
  Failed = 'Failed',
}

export enum RateLimiterStrategy {
  LEAK = 0,
  OVERFLOW = 1,
  BLOCK = 2,
  LEAK_PRIORITY = 3,
}

export interface EngineConfig {
  mode: EngineMode;
  redisUrl?: string;
  redisPoolSize?: number;
  keyPrefix?: string;
}

export interface RateLimiterConfig {
  /** Maximum burst capacity in tokens */
  capacity: number;
  /** Tokens added per refill interval */
  refillRate: number;
  /** Refill window interval in milliseconds (default: 1000) */
  refillIntervalMs: number;
  /** Maximum parallel operations allowed at any moment */
  maxConcurrent?: number;
  /** Minimum spacing in ms between consecutive executions */
  minTimeMs?: number;
  /** Extra penalty/cool-down sleep when rate limit is exceeded */
  coolDownMs?: number;
  /** Maximum queue length before strategy sheds load */
  highWater?: number;
  /** Shedding strategy: LEAK (0), OVERFLOW (1), BLOCK (2), LEAK_PRIORITY (3) */
  strategy?: RateLimiterStrategy | string;
  /** Initial reservoir tokens quota */
  reservoir?: number;
  /** Milliseconds between reservoir refreshes */
  reservoirRefreshIntervalMs?: number;
  /** Value reservoir resets to at each refresh interval */
  reservoirRefreshAmount?: number;
  /** Milliseconds between automatic reservoir increments */
  reservoirIncreaseIntervalMs?: number;
  /** Amount to add to reservoir at each increment interval */
  reservoirIncreaseAmount?: number;
  /** Maximum ceiling cap for reservoir increments */
  reservoirIncreaseMaximum?: number;
}

export interface LimiterOptions {
  maxConcurrent?: number;
  minTime?: number;
  highWater?: number;
  strategy?: RateLimiterStrategy | number;
  penalty?: number;
  reservoir?: number;
  reservoirRefreshInterval?: number;
  reservoirRefreshAmount?: number;
  reservoirIncreaseInterval?: number;
  reservoirIncreaseAmount?: number;
  reservoirIncreaseMaximum?: number;
  rejectOnDrop?: boolean;
  id?: string;
  datastore?: 'local' | 'redis' | 'ioredis';
  connection?: any;
  client?: any;
  clientOptions?: any;
  redisUrl?: string;
  clusterNodes?: string[];
  nodes?: string[];
  clearDatastore?: boolean;
  trackDoneStatus?: boolean;
}

export interface LimiterJobOptions {
  priority?: number;
  weight?: number;
  expiration?: number;
  id?: string;
  retries?: number;
  retryDelay?: number;
}

export interface Ticket {
  id: string;
  allowed: boolean;
  remaining?: number;
  retryAfterMs?: number;
  release: () => Promise<void>;
  done: () => Promise<void>;
}

export interface RateLimitResult {
  allowed: boolean;
  remaining: number;
  retryAfterMs: number;
  currentConcurrency: number;
  reason: 'OK' | 'RATE_LIMIT' | 'CONCURRENCY_LIMIT' | 'MIN_TIME_LIMIT' | 'RESERVOIR_EMPTY' | string;
}

export interface RateLimiterStatus {
  remainingTokens: number;
  activeConcurrency: number;
  capacity: number;
}

export interface JobConfig {
  priority?: number;
  delayMs?: number;
  maxRetries?: number;
  backoffType?: BackoffType;
  backoffBaseMs?: number;
  backoffMaxMs?: number;
  jitter?: boolean;
  timeoutMs?: number;
  groupKey?: string;
  lifo?: boolean;
  removeOnComplete?: boolean;
  removeOnFail?: boolean;
  repeatEveryMs?: number;
  repeatLimit?: number;
  parentId?: string;
  dedupKey?: string;
  expirationMs?: number;
  weight?: number;
}

export interface JobCounts {
  waiting: number;
  active: number;
  delayed: number;
  completed: number;
  failed: number;
}

export interface JobDto {
  id: string;
  name: string;
  queueName: string;
  data: Buffer;
  state: JobState;
  attemptsMade: number;
  progress: number;
  createdAt: number;
  processedAt?: number;
  finishedAt?: number;
  failedReason?: string;
  returnValue?: Buffer;
}

export interface QueueJobOptions {
  priority?: number;
  delay?: number;
  attempts?: number;
  backoff?: number | {
    type?: 'fixed' | 'exponential' | 'linear';
    delay?: number;
    maxDelay?: number;
    jitter?: boolean;
  };
  lifo?: boolean;
  timeout?: number;
  jobId?: string;
  removeOnComplete?: boolean | number;
  removeOnFail?: boolean | number;
  repeat?: {
    every?: number;
    limit?: number;
    cron?: string;
  };
  parent?: {
    id: string;
    queue: string;
  };
  deduplication?: {
    id: string;
  };
  groupKey?: string;
}

export type JobOptions = QueueJobOptions;

export interface FlowJob<T = any> {
  name: string;
  queueName: string;
  data?: T;
  opts?: QueueJobOptions;
  children?: FlowJob[];
}

export interface FlowNode<T = any> {
  job: any;
  children?: FlowNode[];
}

export interface WorkerOptions {
  /** Number of concurrent jobs to process in parallel (default: 1) */
  concurrency?: number;
  /** Polling interval in ms when queue is idle (default: 20) */
  pollIntervalMs?: number;
  /** Optional rate limiter configuration applied to this worker */
  limiter?: RateLimiterConfig | { max: number; duration: number };
  /** Group key for rate-limiting multi-tenant queues */
  groupKey?: string;
  /** Auto-start the worker processing loop (default: true) */
  autoStart?: boolean;
  autorun?: boolean;
  /** Lease lock duration in ms for active jobs (default: 30000) */
  lockDuration?: number;
  /** How often in ms to renew the lock heartbeat (default: 15000) */
  lockRenewTime?: number;
  /** How often in ms to check for stalled jobs (default: 30000) */
  stalledInterval?: number;
  /** Max times a stalled job is recovered before failing permanently (default: 3) */
  maxStalledCount?: number;
  connection?: any;
  client?: any;
  redisUrl?: string;
  clusterNodes?: string[];
  nodes?: string[];
}

export interface NativeRateLimiterBinding {
  acquire(key: string, tokens?: number): Promise<RateLimitResult>;
  acquireSync(key: string, tokens?: number): RateLimitResult;
  release(key: string): Promise<number>;
  status(key: string): Promise<RateLimiterStatus>;
  reset(key: string): Promise<void>;
  cleanup(ttlMs?: number): number;
  check(key: string, tokens?: number): boolean;
  currentReservoir(key: string): number | null;
  incrementReservoir(key: string, amount: number): number;
  updateSettings(config: any): void;
}

export interface BulkJobInput {
  jobId?: string | null;
  name: string;
  data: Buffer;
  config?: any;
}

export interface NativeQueueBinding {
  add(jobId: string | null | undefined, name: string, data: Buffer, config?: any): Promise<string>;
  addBulk(items: BulkJobInput[]): Promise<string[]>;
  fetchNext(): Promise<JobDto | null>;
  fetchBatch(count: number): Promise<JobDto[]>;
  complete(jobId: string, returnValue?: Buffer | null): Promise<void>;
  fail(jobId: string, errorMsg: string): Promise<boolean>;
  updateProgress(jobId: string, progress: number): Promise<void>;
  getJob(jobId: string): Promise<JobDto | null>;
  getCounts(): Promise<JobCounts>;
  pause(): Promise<void>;
  resume(): Promise<void>;
  drain(): Promise<void>;
  clean(graceMs: number, limit?: number, state?: string): Promise<number>;
  promote(jobId: string): Promise<boolean>;
  recoverStalled(lockDurationMs: number, maxStalled?: number): Promise<number>;
  updateData(jobId: string, data: Buffer): Promise<void>;
  registerParentChildren(parentId: string, count: number): void;
}

