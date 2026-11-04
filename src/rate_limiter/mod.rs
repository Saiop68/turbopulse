pub mod in_memory;
pub mod redis_engine;
pub mod lua_scripts;

use async_trait::async_trait;
use napi_derive::napi;
use serde::{Deserialize, Serialize};
use crate::config::RateLimiterConfig;
use crate::error::TurboPulseError;

#[napi(object)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimitResult {
    /// True if the request is permitted immediately; false if throttled
    pub allowed: bool,
    /// Number of tokens remaining in the bucket
    pub remaining: u32,
    /// Milliseconds to wait before retrying (0 if allowed)
    pub retry_after_ms: u32,
    /// Current number of concurrent jobs actively executing for this key
    pub current_concurrency: u32,
    /// Reason string ("OK", "RATE_LIMIT", "CONCURRENCY_LIMIT", "MIN_TIME_LIMIT")
    pub reason: String,
}

#[napi(object)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimiterStatus {
    pub remaining_tokens: u32,
    pub active_concurrency: u32,
    pub capacity: u32,
}

/// Abstract rate limiter backend: can be In-Memory (DashMap) or Distributed (Redis)
#[async_trait]
pub trait RateLimiterBackend: Send + Sync {
    async fn acquire(
        &self,
        key: &str,
        tokens: u32,
        config: &RateLimiterConfig,
    ) -> Result<RateLimitResult, TurboPulseError>;

    async fn release(
        &self,
        key: &str,
    ) -> Result<u32, TurboPulseError>;

    async fn refund(
        &self,
        key: &str,
        tokens: u32,
    ) -> Result<(), TurboPulseError>;

    async fn status(
        &self,
        key: &str,
        config: &RateLimiterConfig,
    ) -> Result<RateLimiterStatus, TurboPulseError>;

    async fn reset(
        &self,
        key: &str,
    ) -> Result<(), TurboPulseError>;
}
