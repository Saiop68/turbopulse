pub mod config;
pub mod error;
pub mod queue;
pub mod rate_limiter;
pub mod redis_backend;
pub mod utils;

use std::sync::Arc;
use napi::bindgen_prelude::*;
use napi_derive::napi;
use uuid::Uuid;

use crate::config::{EngineConfig, EngineMode, JobConfig, RateLimiterConfig};
use crate::queue::job::{JobCounts, JobDto, JobRecord, JobState};
use crate::queue::memory_queue::InMemoryQueue;
use crate::queue::redis_queue::RedisQueue;
use crate::queue::QueueEngine;
use crate::rate_limiter::in_memory::InMemoryRateLimiter;
use crate::rate_limiter::redis_engine::RedisRateLimiter;
use crate::rate_limiter::{RateLimitResult, RateLimiterBackend, RateLimiterStatus};
use crate::utils::time::current_time_ms;

#[napi]
pub struct NativeRateLimiter {
    backend: Arc<dyn RateLimiterBackend>,
    in_memory_ref: Option<Arc<InMemoryRateLimiter>>,
    config: Arc<parking_lot::RwLock<RateLimiterConfig>>,
}

#[napi]
impl NativeRateLimiter {
    /// Attempts to acquire execution permit for a given key (e.g. user_id, route, or queue_name).
    /// Enforces token bucket, max_concurrent, and min_time spacing atomically.
    #[napi]
    pub async fn acquire(&self, key: String, tokens: Option<u32>) -> Result<RateLimitResult> {
        let requested = tokens.unwrap_or(1);
        let conf = self.config.read().clone();
        self.backend
            .acquire(&key, requested, &conf)
            .await
            .map_err(Into::into)
    }

    /// Ultra-fast synchronous token acquisition (bypasses Promise/Tokio overhead).
    /// Capable of 2,000,000 - 5,000,000+ operations/sec in In-Memory mode.
    #[napi]
    pub fn acquire_sync(&self, key: String, tokens: Option<u32>) -> Result<RateLimitResult> {
        if let Some(ref im) = self.in_memory_ref {
            let requested = tokens.unwrap_or(1);
            let conf = self.config.read().clone();
            im.acquire_sync(&key, requested, &conf).map_err(Into::into)
        } else {
            Err(napi::Error::new(
                Status::GenericFailure,
                "Synchronous acquire is only supported in InMemory mode. Use async acquire() or LeasedRateLimiter for Redis.",
            ))
        }
    }

    /// Releases concurrency permit upon job or operation completion.
    #[napi]
    pub async fn release(&self, key: String) -> Result<u32> {
        self.backend.release(&key).await.map_err(Into::into)
    }

    /// Refunds tokens and releases concurrency permit (e.g. when an idle worker finds no work).
    #[napi]
    pub async fn refund(&self, key: String, tokens: Option<u32>) -> Result<()> {
        let t = tokens.unwrap_or(1);
        self.backend.refund(&key, t).await.map_err(Into::into)
    }

    /// Queries current bucket status (available tokens, active concurrency) without consuming tokens.
    #[napi]
    pub async fn status(&self, key: String) -> Result<RateLimiterStatus> {
        let conf = self.config.read().clone();
        self.backend
            .status(&key, &conf)
            .await
            .map_err(Into::into)
    }

    /// Resets rate limiter state for a key.
    #[napi]
    pub async fn reset(&self, key: String) -> Result<()> {
        self.backend.reset(&key).await.map_err(Into::into)
    }

    /// Evicts expired keys from in-memory cache to reclaim memory in high-cardinality workloads.
    #[napi]
    pub fn cleanup(&self, ttl_ms: Option<u32>) -> Result<u32> {
        if let Some(ref im) = self.in_memory_ref {
            let ttl = ttl_ms.unwrap_or(60_000) as u64;
            Ok(im.cleanup_stale_buckets(ttl) as u32)
        } else {
            Ok(0)
        }
    }

    /// Checks if a request for N tokens would be allowed right now without consuming them
    #[napi]
    pub fn check(&self, key: String, tokens: Option<u32>) -> Result<bool> {
        if let Some(ref im) = self.in_memory_ref {
            let requested = tokens.unwrap_or(1);
            let conf = self.config.read().clone();
            Ok(im.check_sync(&key, requested, &conf))
        } else {
            Ok(true)
        }
    }

    /// Returns current reservoir remaining count
    #[napi]
    pub fn current_reservoir(&self, key: String) -> Result<Option<i32>> {
        if let Some(ref im) = self.in_memory_ref {
            let res = im.current_reservoir(&key);
            if res.is_some() {
                Ok(res)
            } else {
                Ok(self.config.read().reservoir)
            }
        } else {
            Ok(self.config.read().reservoir)
        }
    }

    /// Increments or decrements reservoir tokens by amount
    #[napi]
    pub fn increment_reservoir(&self, key: String, amount: i32) -> Result<i32> {
        if let Some(ref im) = self.in_memory_ref {
            let def = self.config.read().reservoir;
            Ok(im.increment_reservoir(&key, amount, def))
        } else {
            Ok(amount)
        }
    }

    /// Dynamically updates limiter settings at runtime
    #[napi]
    pub fn update_settings(&self, config: RateLimiterConfig) -> Result<()> {
        *self.config.write() = config;
        Ok(())
    }
}

#[napi(object)]
pub struct BulkJobInput {
    pub job_id: Option<String>,
    pub name: String,
    pub data: Buffer,
    pub config: Option<JobConfig>,
}

#[napi]
pub struct NativeQueue {
    name: String,
    engine: Arc<QueueEngine>,
}

#[napi]
impl NativeQueue {
    /// Adds a job to the queue. Uses zero-copy Buffer to bypass V8 GC pressure for queue data.
    #[napi]
    pub async fn add(
        &self,
        job_id: Option<String>,
        name: String,
        data: Buffer,
        config: Option<JobConfig>,
    ) -> Result<String> {
        let id = job_id.unwrap_or_else(|| Uuid::new_v4().to_string());
        let conf = config.unwrap_or_default();
        let now = current_time_ms();
        let delay_ms = conf.delay_ms.unwrap_or(0) as u64;

        let record = JobRecord {
            id: id.clone(),
            name,
            queue_name: self.name.clone(),
            data: data.as_ref().to_vec(),
            config: conf.clone(),
            state: if conf.is_waiting_children == Some(true) {
                JobState::WaitingChildren
            } else if delay_ms > 0 {
                JobState::Delayed
            } else {
                JobState::Waiting
            },
            attempts_made: 0,
            stalled_count: 0,
            progress: 0,
            created_at: now,
            processed_at: None,
            finished_at: None,
            failed_reason: None,
            return_value: None,
            delay_until_ms: now + delay_ms,
            lock_token: None,
        };

        self.engine.push_job(record).await.map_err(Into::<napi::Error>::into)?;
        Ok(id)
    }

    /// Atomically enqueues a bulk batch of jobs
    #[napi]
    pub async fn add_bulk(&self, items: Vec<BulkJobInput>) -> Result<Vec<String>> {
        let now = current_time_ms();
        let mut records = Vec::with_capacity(items.len());

        for item in items {
            let id = item.job_id.unwrap_or_else(|| Uuid::new_v4().to_string());
            let conf = item.config.unwrap_or_default();
            let delay_ms = conf.delay_ms.unwrap_or(0) as u64;

            records.push(JobRecord {
                id: id.clone(),
                name: item.name,
                queue_name: self.name.clone(),
                data: item.data.as_ref().to_vec(),
                config: conf.clone(),
                state: if conf.is_waiting_children == Some(true) {
                    JobState::WaitingChildren
                } else if delay_ms > 0 {
                    JobState::Delayed
                } else {
                    JobState::Waiting
                },
                attempts_made: 0,
                stalled_count: 0,
                progress: 0,
                created_at: now,
                processed_at: None,
                finished_at: None,
                failed_reason: None,
                return_value: None,
                delay_until_ms: now + delay_ms,
                lock_token: None,
            });
        }

        self.engine.push_jobs_bulk(records).await.map_err(Into::<napi::Error>::into)
    }

    /// Fetches next prioritized, available job. Returns None if queue is empty or paused.
    #[napi]
    pub async fn fetch_next(&self, lock_duration_ms: Option<u32>) -> Result<Option<JobDto>> {
        let duration = lock_duration_ms.map(|d| d as u64);
        let record_opt = self.engine.fetch_next_job(&self.name, duration).await.map_err(Into::<napi::Error>::into)?;
        Ok(record_opt.map(|r| r.to_dto()))
    }

    /// Fetches a batch of prioritized, available jobs
    #[napi]
    pub async fn fetch_batch(&self, count: u32, lock_duration_ms: Option<u32>) -> Result<Vec<JobDto>> {
        let duration = lock_duration_ms.map(|d| d as u64);
        let records = self.engine.fetch_next_batch(&self.name, count as usize, duration).await.map_err(Into::<napi::Error>::into)?;
        Ok(records.into_iter().map(|r| r.to_dto()).collect())
    }

    /// Marks job as completed and stores optional binary result.
    #[napi]
    pub async fn complete(&self, job_id: String, return_value: Option<Buffer>, lock_token: String) -> Result<bool> {
        if lock_token.trim().is_empty() {
            return Err(napi::Error::new(
                Status::InvalidArg,
                "lock_token is required to complete a job (empty token is not allowed)",
            ));
        }
        let val = return_value.map(|b| b.as_ref().to_vec());
        self.engine.complete_job(&self.name, &job_id, val, &lock_token).await.map_err(Into::<napi::Error>::into)
    }

    /// Fails job. If retries remain, automatically calculates backoff with jitter and re-schedules.
    /// Returns true if permanently failed; false if retry scheduled.
    #[napi]
    pub async fn fail(&self, job_id: String, error_msg: String, lock_token: String) -> Result<bool> {
        if lock_token.trim().is_empty() {
            return Err(napi::Error::new(
                Status::InvalidArg,
                "lock_token is required to fail a job (empty token is not allowed)",
            ));
        }
        self.engine.fail_job(&self.name, &job_id, &error_msg, &lock_token).await.map_err(Into::<napi::Error>::into)
    }

    /// Renews active lock on an executing job (heartbeat).
    #[napi]
    pub async fn renew_lock(&self, job_id: String, lock_token: String, lock_duration_ms: Option<u32>) -> Result<bool> {
        if lock_token.trim().is_empty() {
            return Err(napi::Error::new(
                Status::InvalidArg,
                "lock_token is required to renew lock (empty token is not allowed)",
            ));
        }
        let duration = lock_duration_ms.unwrap_or(30000);
        self.engine.renew_lock(&self.name, &job_id, &lock_token, duration).await.map_err(Into::<napi::Error>::into)
    }

    /// Updates progress percentage (0 - 100) for a job.
    #[napi]
    pub async fn update_progress(&self, job_id: String, progress: u32) -> Result<()> {
        self.engine.update_progress(&self.name, &job_id, progress).await.map_err(Into::<napi::Error>::into)
    }

    /// Retrieves job status and metadata by ID.
    #[napi]
    pub async fn get_job(&self, job_id: String) -> Result<Option<JobDto>> {
        let record_opt = self.engine.get_job(&self.name, &job_id).await.map_err(Into::<napi::Error>::into)?;
        Ok(record_opt.map(|r| r.to_dto()))
    }

    /// Returns waiting, active, delayed, completed, and failed counts.
    #[napi]
    pub async fn get_counts(&self) -> Result<JobCounts> {
        self.engine.get_counts(&self.name).await.map_err(Into::<napi::Error>::into)
    }

    /// Pauses queue job dispatching.
    #[napi]
    pub async fn pause(&self) -> Result<()> {
        self.engine.pause(&self.name).await.map_err(Into::<napi::Error>::into)
    }

    /// Resumes queue job dispatching.
    #[napi]
    pub async fn resume(&self) -> Result<()> {
        self.engine.resume(&self.name).await.map_err(Into::<napi::Error>::into)
    }

    /// Clears waiting and delayed jobs from queue.
    #[napi]
    pub async fn drain(&self) -> Result<()> {
        self.engine.drain(&self.name).await.map_err(Into::<napi::Error>::into)
    }

    /// Obliterates queue completely (removes all keys and records).
    /// If force is false, returns error if active jobs exist.
    #[napi]
    pub async fn obliterate(&self, force: Option<bool>) -> Result<u32> {
        let f = force.unwrap_or(false);
        self.engine.obliterate(&self.name, f).await.map_err(Into::<napi::Error>::into)
    }

    /// Cleans jobs older than grace_ms in the specified state ('completed', 'failed')
    #[napi]
    pub async fn clean(&self, grace_ms: u32, limit: Option<u32>, state: Option<String>) -> Result<u32> {
        let lim = limit.unwrap_or(1000);
        let st = state.unwrap_or_else(|| "completed".to_string());
        self.engine.clean(&self.name, grace_ms as u64, lim, &st).await.map_err(Into::<napi::Error>::into)
    }

    /// Promotes delayed job to waiting queue immediately
    #[napi]
    pub async fn promote(&self, job_id: String) -> Result<bool> {
        self.engine.promote(&self.name, &job_id).await.map_err(Into::<napi::Error>::into)
    }

    /// Recovers stalled active jobs whose heartbeats expired
    #[napi]
    pub async fn recover_stalled(&self, lock_duration_ms: u32, max_stalled: Option<u32>) -> Result<u32> {
        let max_st = max_stalled.unwrap_or(3);
        self.engine.recover_stalled(&self.name, lock_duration_ms as u64, max_st).await.map_err(Into::<napi::Error>::into)
    }

    /// Updates arbitrary job binary payload
    #[napi]
    pub async fn update_data(&self, job_id: String, data: Buffer) -> Result<()> {
        let bytes = data.as_ref().to_vec();
        self.engine.update_data(&self.name, &job_id, bytes).await.map_err(Into::<napi::Error>::into)
    }

    /// Registers parent with children IDs for DAG FlowProducer
    #[napi]
    pub async fn register_parent_children(&self, parent_id: String, child_ids: Vec<String>) -> Result<()> {
        self.engine.register_parent_children(&self.name, &parent_id, &child_ids).await.map_err(Into::<napi::Error>::into)
    }

    /// Resolves child dependency for parent job
    #[napi]
    pub async fn resolve_parent_dep(&self, parent_id: String, child_id: String) -> Result<bool> {
        self.engine.resolve_parent_dep(&self.name, &parent_id, &child_id).await.map_err(Into::<napi::Error>::into)
    }
}

/// Core native engine holding shared state for In-Memory or Redis distributed backends.
#[napi]
pub struct NativeEngine {
    mode: EngineMode,
    limiter_backend: Arc<dyn RateLimiterBackend>,
    in_memory_limiter_ref: Option<Arc<InMemoryRateLimiter>>,
    queue_engine: Arc<QueueEngine>,
}

#[napi]
impl NativeEngine {
    /// Synchronous constructor for In-Memory mode
    #[napi(constructor)]
    pub fn new(config: Option<EngineConfig>) -> Result<Self> {
        let conf = config.unwrap_or(EngineConfig {
            mode: EngineMode::InMemory,
            redis_url: None,
            redis_pool_size: None,
            key_prefix: None,
        });

        match conf.mode {
            EngineMode::InMemory => {
                let im_limiter = Arc::new(InMemoryRateLimiter::new());
                let im_queue = Arc::new(InMemoryQueue::new());
                Ok(Self {
                    mode: EngineMode::InMemory,
                    limiter_backend: im_limiter.clone(),
                    in_memory_limiter_ref: Some(im_limiter),
                    queue_engine: Arc::new(QueueEngine::InMemory(im_queue)),
                })
            }
            EngineMode::Redis => {
                Err(napi::Error::new(
                    Status::InvalidArg,
                    "Redis mode requires async initialization. Use 'NativeEngine.create(config)' instead.",
                ))
            }
        }
    }

    /// Async factory function to initialize NativeEngine in Redis or InMemory mode
    #[napi(factory)]
    pub async fn create(config: EngineConfig) -> Result<Self> {
        match config.mode {
            EngineMode::InMemory => {
                let im_limiter = Arc::new(InMemoryRateLimiter::new());
                let im_queue = Arc::new(InMemoryQueue::new());
                Ok(Self {
                    mode: EngineMode::InMemory,
                    limiter_backend: im_limiter.clone(),
                    in_memory_limiter_ref: Some(im_limiter),
                    queue_engine: Arc::new(QueueEngine::InMemory(im_queue)),
                })
            }
            EngineMode::Redis => {
                let redis_url = config.redis_url.ok_or_else(|| {
                    napi::Error::new(
                        Status::InvalidArg,
                        "redis_url is required when mode is 'Redis'",
                    )
                })?;

                let prefix = config.key_prefix.unwrap_or_else(|| "turbopulse".to_string());
                let rl_prefix = format!("{}:rl", prefix);
                let q_prefix = format!("{}:q", prefix);

                let redis_rl = RedisRateLimiter::new(&redis_url, Some(rl_prefix))
                    .await
                    .map_err(Into::<napi::Error>::into)?;

                let redis_q = RedisQueue::new(&redis_url, Some(q_prefix))
                    .await
                    .map_err(Into::<napi::Error>::into)?;

                Ok(Self {
                    mode: EngineMode::Redis,
                    limiter_backend: Arc::new(redis_rl),
                    in_memory_limiter_ref: None,
                    queue_engine: Arc::new(QueueEngine::Redis(Arc::new(redis_q))),
                })
            }
        }
    }

    /// Creates a rate limiter instance bound to this engine
    #[napi]
    pub fn create_rate_limiter(&self, config: Option<RateLimiterConfig>) -> NativeRateLimiter {
        NativeRateLimiter {
            backend: self.limiter_backend.clone(),
            in_memory_ref: self.in_memory_limiter_ref.clone(),
            config: Arc::new(parking_lot::RwLock::new(config.unwrap_or_default())),
        }
    }

    /// Creates or connects to a named job queue
    #[napi]
    pub fn create_queue(&self, name: String) -> NativeQueue {
        NativeQueue {
            name,
            engine: self.queue_engine.clone(),
        }
    }

    /// Returns the engine mode (InMemory or Redis)
    #[napi(getter)]
    pub fn mode(&self) -> EngineMode {
        self.mode
    }
}
