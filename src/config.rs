use napi_derive::napi;
use serde::{Deserialize, Serialize};

#[napi(string_enum)]
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum EngineMode {
    InMemory,
    Redis,
}

#[napi(string_enum)]
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum BackoffType {
    Fixed,
    Exponential,
    Linear,
}

#[napi(string_enum)]
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RateLimiterStrategy {
    Leak,
    Overflow,
    Block,
    LeakPriority,
}

#[napi(object)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimiterConfig {
    /// Maximum burst token capacity in the bucket
    pub capacity: u32,
    /// Number of tokens refilled per interval
    pub refill_rate: u32,
    /// Interval in milliseconds for token refill (e.g. 1000 for 1s)
    pub refill_interval_ms: u32,
    /// Maximum concurrent operations executing at any given time
    pub max_concurrent: Option<u32>,
    /// Minimum time in milliseconds between consecutive operations
    pub min_time_ms: Option<u32>,
    /// Additional penalty/cool-down sleep when rate limit is exceeded
    pub cool_down_ms: Option<u32>,
    /// Maximum queue depth before strategy triggers
    pub high_water: Option<u32>,
    /// Strategy when highWater is reached: Leak, Overflow, Block, LeakPriority
    pub strategy: Option<RateLimiterStrategy>,
    /// Initial reservoir tokens
    pub reservoir: Option<i32>,
    /// Milliseconds between reservoir resets
    pub reservoir_refresh_interval_ms: Option<u32>,
    /// What the reservoir resets to at each refresh interval
    pub reservoir_refresh_amount: Option<u32>,
    /// Milliseconds between reservoir increments
    pub reservoir_increase_interval_ms: Option<u32>,
    /// Amount to increment reservoir by
    pub reservoir_increase_amount: Option<u32>,
    /// Maximum ceiling for reservoir increments
    pub reservoir_increase_maximum: Option<u32>,
}

impl Default for RateLimiterConfig {
    fn default() -> Self {
        Self {
            capacity: 10,
            refill_rate: 10,
            refill_interval_ms: 1000,
            max_concurrent: None,
            min_time_ms: None,
            cool_down_ms: None,
            high_water: None,
            strategy: Some(RateLimiterStrategy::Leak),
            reservoir: None,
            reservoir_refresh_interval_ms: None,
            reservoir_refresh_amount: None,
            reservoir_increase_interval_ms: None,
            reservoir_increase_amount: None,
            reservoir_increase_maximum: None,
        }
    }
}

#[napi(object)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobConfig {
    /// Priority level (higher value executes earlier; 0 is normal)
    pub priority: Option<i32>,
    /// Delay execution by N milliseconds
    pub delay_ms: Option<u32>,
    /// Maximum number of retry attempts on failure
    pub max_retries: Option<u32>,
    /// Retry backoff algorithm: Fixed, Exponential, or Linear
    pub backoff_type: Option<BackoffType>,
    /// Base delay in ms for backoff calculations (e.g. 1000ms)
    pub backoff_base_ms: Option<u32>,
    /// Maximum delay cap for exponential backoff (e.g. 60000ms)
    pub backoff_max_ms: Option<u32>,
    /// Whether to add randomized jitter (+/- 20%) to avoid thundering herd
    pub jitter: Option<bool>,
    /// Timeout in ms after which active job is considered stalled/timed out
    pub timeout_ms: Option<u32>,
    /// Custom key for group-based rate limiting (per-tenant/customer)
    pub group_key: Option<String>,
    /// LIFO execution order (add to front of waiting queue)
    pub lifo: Option<bool>,
    /// Automatically prune from history when completed
    pub remove_on_complete: Option<bool>,
    /// Automatically prune from history when failed
    pub remove_on_fail: Option<bool>,
    /// Repeat interval in milliseconds
    pub repeat_every_ms: Option<u32>,
    /// Maximum number of repeat executions
    pub repeat_limit: Option<u32>,
    /// Parent job ID for flow-producer job hierarchies
    pub parent_id: Option<String>,
    /// Deduplication key to prevent enqueueing duplicate active jobs
    pub dedup_key: Option<String>,
    /// Job expiration timestamp in ms (auto-aborts if not started before)
    pub expiration_ms: Option<u32>,
    /// Job token weight consumed from rate limiter reservoir
    pub weight: Option<u32>,
}

impl Default for JobConfig {
    fn default() -> Self {
        Self {
            priority: Some(0),
            delay_ms: Some(0),
            max_retries: Some(3),
            backoff_type: Some(BackoffType::Exponential),
            backoff_base_ms: Some(1000),
            backoff_max_ms: Some(60000),
            jitter: Some(true),
            timeout_ms: Some(30000),
            group_key: None,
            lifo: Some(false),
            remove_on_complete: Some(false),
            remove_on_fail: Some(false),
            repeat_every_ms: None,
            repeat_limit: None,
            parent_id: None,
            dedup_key: None,
            expiration_ms: None,
            weight: Some(1),
        }
    }
}

#[napi(object)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineConfig {
    pub mode: EngineMode,
    /// Redis connection URL (e.g. "redis://127.0.0.1:6379/0"), required if mode is Redis
    pub redis_url: Option<String>,
    /// Connection pool size for Redis operations
    pub redis_pool_size: Option<u32>,
    /// Key prefix for Redis keys (defaults to "turbopulse")
    pub key_prefix: Option<String>,
}
