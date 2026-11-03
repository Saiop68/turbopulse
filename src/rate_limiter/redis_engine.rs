use async_trait::async_trait;
use redis::aio::ConnectionManager;
use redis::{AsyncCommands, Client, Script};

use crate::config::RateLimiterConfig;
use crate::error::TurboPulseError;
use crate::rate_limiter::lua_scripts::{ACQUIRE_TOKEN_LUA, RELEASE_CONCURRENCY_LUA, STATUS_LUA};
use crate::rate_limiter::{RateLimitResult, RateLimiterBackend, RateLimiterStatus};
use crate::utils::time::current_time_ms;

pub struct RedisRateLimiter {
    manager: ConnectionManager,
    key_prefix: String,
    acquire_script: Script,
    release_script: Script,
    status_script: Script,
}

impl RedisRateLimiter {
    pub async fn new(redis_url: &str, key_prefix: Option<String>) -> Result<Self, TurboPulseError> {
        let client = Client::open(redis_url).map_err(TurboPulseError::Redis)?;
        let manager = ConnectionManager::new(client).await.map_err(TurboPulseError::Redis)?;

        Ok(Self {
            manager,
            key_prefix: key_prefix.unwrap_or_else(|| "turbopulse:rl".to_string()),
            acquire_script: Script::new(ACQUIRE_TOKEN_LUA),
            release_script: Script::new(RELEASE_CONCURRENCY_LUA),
            status_script: Script::new(STATUS_LUA),
        })
    }

    #[inline]
    fn format_key(&self, key: &str) -> String {
        format!("{}:{}", self.key_prefix, key)
    }
}

#[async_trait]
impl RateLimiterBackend for RedisRateLimiter {
    async fn acquire(
        &self,
        key: &str,
        tokens_requested: u32,
        config: &RateLimiterConfig,
    ) -> Result<RateLimitResult, TurboPulseError> {
        let redis_key = self.format_key(key);
        let now = current_time_ms();
        let max_concurrent = config.max_concurrent.unwrap_or(0);
        let min_time = config.min_time_ms.unwrap_or(0);
        let cool_down = config.cool_down_ms.unwrap_or(0);

        let mut conn = self.manager.clone();

        // Script returns: { allowed (0 or 1), tokens, retry_after_ms, active_count, reason }
        let (allowed, remaining, retry_after_ms, active_count, reason): (i64, i64, i64, i64, String) = self
            .acquire_script
            .key(&redis_key)
            .arg(tokens_requested)
            .arg(config.capacity)
            .arg(config.refill_rate)
            .arg(config.refill_interval_ms)
            .arg(max_concurrent)
            .arg(min_time)
            .arg(cool_down)
            .arg(now)
            .invoke_async(&mut conn)
            .await
            .map_err(TurboPulseError::Redis)?;

        Ok(RateLimitResult {
            allowed: allowed == 1,
            remaining: remaining.max(0) as u32,
            retry_after_ms: retry_after_ms.max(0) as u32,
            current_concurrency: active_count.max(0) as u32,
            reason,
        })
    }

    async fn release(&self, key: &str) -> Result<u32, TurboPulseError> {
        let redis_key = self.format_key(key);
        let mut conn = self.manager.clone();

        let active: i64 = self
            .release_script
            .key(&redis_key)
            .invoke_async(&mut conn)
            .await
            .map_err(TurboPulseError::Redis)?;

        Ok(active.max(0) as u32)
    }

    async fn status(
        &self,
        key: &str,
        config: &RateLimiterConfig,
    ) -> Result<RateLimiterStatus, TurboPulseError> {
        let redis_key = self.format_key(key);
        let now = current_time_ms();
        let mut conn = self.manager.clone();

        let (tokens, active, _): (i64, i64, i64) = self
            .status_script
            .key(&redis_key)
            .arg(config.capacity)
            .arg(config.refill_rate)
            .arg(config.refill_interval_ms)
            .arg(now)
            .invoke_async(&mut conn)
            .await
            .map_err(TurboPulseError::Redis)?;

        Ok(RateLimiterStatus {
            remaining_tokens: tokens.max(0) as u32,
            active_concurrency: active.max(0) as u32,
            capacity: config.capacity,
        })
    }

    async fn reset(&self, key: &str) -> Result<(), TurboPulseError> {
        let redis_key = self.format_key(key);
        let mut conn = self.manager.clone();
        conn.del::<_, ()>(&redis_key).await.map_err(TurboPulseError::Redis)?;
        Ok(())
    }
}
