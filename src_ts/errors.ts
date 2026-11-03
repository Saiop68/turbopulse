export class TurboPulseError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'TurboPulseError';
  }
}

export class RateLimitExceededError extends TurboPulseError {
  public readonly retryAfterMs: number;
  public readonly key: string;

  constructor(key: string, retryAfterMs: number) {
    super(`Rate limit exceeded for key "${key}". Retry after ${retryAfterMs}ms`);
    this.name = 'RateLimitExceededError';
    this.key = key;
    this.retryAfterMs = retryAfterMs;
  }
}

export class ConcurrencyLimitError extends TurboPulseError {
  public readonly maxConcurrent: number;
  public readonly key: string;

  constructor(key: string, maxConcurrent: number) {
    super(`Concurrency limit of ${maxConcurrent} reached for key "${key}"`);
    this.name = 'ConcurrencyLimitError';
    this.key = key;
    this.maxConcurrent = maxConcurrent;
  }
}

export class JobTimeoutError extends TurboPulseError {
  public readonly jobId: string;

  constructor(jobId: string, timeoutMs: number) {
    super(`Job ${jobId} timed out after ${timeoutMs}ms`);
    this.name = 'JobTimeoutError';
    this.jobId = jobId;
  }
}
