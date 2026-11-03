import { JobDto, JobOptions, JobState, NativeQueueBinding } from './types';

/**
 * High-performance Job representation.
 * Supports zero-copy binary data and fast JSON parsing.
 */
export class Job<TData = any, TResult = any> {
  public readonly id: string;
  public readonly name: string;
  public readonly queueName: string;
  public readonly data: TData;
  public readonly opts: JobOptions;
  public state: JobState;
  public attemptsMade: number;
  public progress: number;
  public timestamp: number;
  public processedOn?: number;
  public finishedOn?: number;
  public failedReason?: string;
  public returnvalue?: TResult;
  private nativeQueue: NativeQueueBinding;

  constructor(dto: JobDto, nativeQueue: NativeQueueBinding, opts: JobOptions = {}) {
    this.id = dto.id;
    this.name = dto.name;
    this.queueName = dto.queueName;
    this.opts = opts;
    this.state = dto.state;
    this.attemptsMade = dto.attemptsMade;
    this.progress = dto.progress;
    this.timestamp = dto.createdAt;
    this.processedOn = dto.processedAt;
    this.finishedOn = dto.finishedAt;
    this.failedReason = dto.failedReason;
    this.nativeQueue = nativeQueue;

    // Zero-copy deserialization: if Buffer, deserialize JSON or keep raw
    this.data = Job.deserializePayload<TData>(dto.data);

    if (dto.returnValue) {
      this.returnvalue = Job.deserializePayload<TResult>(dto.returnValue);
    }
  }

  /**
   * Fast serializer: converts string, Buffer, or JS object to Buffer for zero-copy N-API transfer
   */
  public static serializePayload(data: any): Buffer {
    if (Buffer.isBuffer(data)) {
      return data;
    }
    if (typeof data === 'string') {
      return Buffer.from(data, 'utf-8');
    }
    if (data === undefined || data === null) {
      return Buffer.alloc(0);
    }
    return Buffer.from(JSON.stringify(data), 'utf-8');
  }

  /**
   * Fast deserializer: converts Buffer to typed object or string
   */
  public static deserializePayload<T>(buf: Buffer): T {
    if (!buf || buf.length === 0) {
      return undefined as unknown as T;
    }
    try {
      const text = buf.toString('utf-8');
      return JSON.parse(text) as T;
    } catch {
      // Fallback: return as raw string or Buffer
      return buf as unknown as T;
    }
  }

  /**
   * Updates job progress percentage (0 - 100)
   */
  public async updateProgress(progress: number): Promise<void> {
    const clamped = Math.min(100, Math.max(0, Math.round(progress)));
    this.progress = clamped;
    await this.nativeQueue.updateProgress(this.id, clamped);
  }

  /**
   * Appends execution log
   */
  public log(message: string): void {
    // In production, logs can be attached to queue trace
    const timestamp = new Date().toISOString();
    console.log(`[Job ${this.queueName}:${this.id}] ${timestamp} - ${message}`);
  }

  /**
   * Fetches latest job state from native engine
   */
  public async getState(): Promise<JobState> {
    const latest = await this.nativeQueue.getJob(this.id);
    return latest ? latest.state : JobState.Failed;
  }
}
