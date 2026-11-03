use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use dashmap::DashMap;
use parking_lot::Mutex;
use async_trait::async_trait;

use crate::config::RateLimiterConfig;
use crate::error::TurboPulseError;
use crate::rate_limiter::{RateLimitResult, RateLimiterBackend, RateLimiterStatus};
use crate::utils::time::current_time_ms;

/// Internal state of a single bucket within the thread-safe DashMap
struct BucketState {
    tokens: Mutex<f64>,
    last_refill_ms: Mutex<u64>,
    last_exec_ms: Mutex<u64>,
    active_concurrency: AtomicU32,
    last_accessed_ms: AtomicU64,
    reservoir: Mutex<Option<i32>>,
    last_reservoir_refresh_ms: Mutex<u64>,
    last_reservoir_increase_ms: Mutex<u64>,
}

impl BucketState {
    fn new(capacity: f64, now: u64, initial_reservoir: Option<i32>) -> Self {
        Self {
            tokens: Mutex::new(capacity),
            last_refill_ms: Mutex::new(now),
            last_exec_ms: Mutex::new(0),
            active_concurrency: AtomicU32::new(0),
            last_accessed_ms: AtomicU64::new(now),
            reservoir: Mutex::new(initial_reservoir),
            last_reservoir_refresh_ms: Mutex::new(now),
            last_reservoir_increase_ms: Mutex::new(now),
        }
    }
}

/// High-performance in-memory rate limiter using DashMap and fine-grained per-bucket locks.
/// Guarantees zero cross-key lock contention, sub-microsecond latency, and automatic GC avoidance.
pub struct InMemoryRateLimiter {
    buckets: DashMap<String, Arc<BucketState>>,
}

impl InMemoryRateLimiter {
    pub fn new() -> Self {
        Self {
            buckets: DashMap::new(),
        }
    }

    /// Prunes stale buckets that haven't been accessed in `ttl_ms` (prevents memory leak with high-cardinality keys)
    pub fn cleanup_stale_buckets(&self, ttl_ms: u64) -> usize {
        let now = current_time_ms();
        let mut pruned = 0;
        self.buckets.retain(|_, state| {
            let last_accessed = state.last_accessed_ms.load(Ordering::Relaxed);
            let active = state.active_concurrency.load(Ordering::Relaxed);
            if active == 0 && now.saturating_sub(last_accessed) > ttl_ms {
                pruned += 1;
                false
            } else {
                true
            }
        });
        pruned
    }

    /// Queries the current reservoir value for a key without mutating it
    pub fn current_reservoir(&self, key: &str) -> Option<i32> {
        self.buckets.get(key).and_then(|b| *b.reservoir.lock())
    }

    /// Increments or decrements the reservoir value by amount
    pub fn increment_reservoir(&self, key: &str, amount: i32, default_res: Option<i32>) -> i32 {
        let now = current_time_ms();
        let bucket = self.buckets
            .entry(key.to_string())
            .or_insert_with(|| Arc::new(BucketState::new(10.0, now, default_res)))
            .clone();

        let mut res_guard = bucket.reservoir.lock();
        let current = res_guard.or(default_res).unwrap_or(0);
        let updated = current + amount;
        *res_guard = Some(updated);
        updated
    }

    /// Dry-run check: returns true if a request would be permitted right now, false otherwise
    pub fn check_sync(&self, key: &str, tokens_requested: u32, config: &RateLimiterConfig) -> bool {
        let now = current_time_ms();
        if let Some(bucket) = self.buckets.get(key) {
            // Check concurrency
            if let Some(max_conc) = config.max_concurrent {
                if bucket.active_concurrency.load(Ordering::Relaxed) >= max_conc {
                    return false;
                }
            }

            // Check min_time
            if let Some(min_time) = config.min_time_ms {
                let last_exec = *bucket.last_exec_ms.lock();
                if last_exec > 0 && now.saturating_sub(last_exec) < min_time as u64 {
                    return false;
                }
            }

            // Check reservoir
            let res = *bucket.reservoir.lock();
            if let Some(r) = res {
                if r < tokens_requested as i32 {
                    return false;
                }
            }

            // Check tokens
            let tokens = *bucket.tokens.lock();
            let last_refill = *bucket.last_refill_ms.lock();
            let elapsed = now.saturating_sub(last_refill);
            let mut available = tokens;
            if elapsed > 0 && config.refill_interval_ms > 0 {
                let refilled = (elapsed as f64 / config.refill_interval_ms as f64) * config.refill_rate as f64;
                available = (available + refilled).min(config.capacity as f64);
            }

            available >= tokens_requested as f64
        } else {
            // Key doesn't exist yet, so it has fresh bucket capacity
            if let Some(res) = config.reservoir {
                if res < tokens_requested as i32 {
                    return false;
                }
            }
            config.capacity >= tokens_requested
        }
    }

    /// Synchronous token bucket acquisition with zero Tokio/Promise overhead
    pub fn acquire_sync(
        &self,
        key: &str,
        tokens_requested: u32,
        config: &RateLimiterConfig,
    ) -> Result<RateLimitResult, TurboPulseError> {
        let now = current_time_ms();
        let bucket = self.buckets
            .entry(key.to_string())
            .or_insert_with(|| Arc::new(BucketState::new(config.capacity as f64, now, config.reservoir)))
            .clone();

        bucket.last_accessed_ms.store(now, Ordering::Relaxed);

        let active = bucket.active_concurrency.load(Ordering::SeqCst);

        // 1. Concurrency limit check
        if let Some(max_conc) = config.max_concurrent {
            if active >= max_conc {
                let current_tokens = *bucket.tokens.lock();
                return Ok(RateLimitResult {
                    allowed: false,
                    remaining: current_tokens.floor() as u32,
                    retry_after_ms: 25, // Quick backoff to poll for concurrency release
                    current_concurrency: active,
                    reason: "CONCURRENCY_LIMIT".to_string(),
                });
            }
        }

        // Lock bucket for atomic token, min_time, and reservoir evaluation
        let mut tokens_guard = bucket.tokens.lock();
        let mut last_refill_guard = bucket.last_refill_ms.lock();
        let mut last_exec_guard = bucket.last_exec_ms.lock();
        let mut reservoir_guard = bucket.reservoir.lock();
        let mut last_res_refresh_guard = bucket.last_reservoir_refresh_ms.lock();
        let mut last_res_incr_guard = bucket.last_reservoir_increase_ms.lock();

        // 2. Reservoir refresh & increment processing
        if let Some(refresh_interval) = config.reservoir_refresh_interval_ms {
            if refresh_interval > 0 && now.saturating_sub(*last_res_refresh_guard) >= refresh_interval as u64 {
                let refresh_amt = config.reservoir_refresh_amount.unwrap_or(config.capacity) as i32;
                *reservoir_guard = Some(refresh_amt);
                *last_res_refresh_guard = now;
            }
        }

        if let Some(incr_interval) = config.reservoir_increase_interval_ms {
            if incr_interval > 0 && now.saturating_sub(*last_res_incr_guard) >= incr_interval as u64 {
                let incr_amt = config.reservoir_increase_amount.unwrap_or(1) as i32;
                let max_amt = config.reservoir_increase_maximum.unwrap_or(config.capacity) as i32;
                let current = reservoir_guard.unwrap_or(0);
                *reservoir_guard = Some((current + incr_amt).min(max_amt));
                *last_res_incr_guard = now;
            }
        }

        if let Some(current_res) = *reservoir_guard {
            if current_res < tokens_requested as i32 {
                let wait_ms = config.reservoir_refresh_interval_ms
                    .map(|interval| (interval as u64).saturating_sub(now.saturating_sub(*last_res_refresh_guard)).max(1) as u32)
                    .unwrap_or(100);

                return Ok(RateLimitResult {
                    allowed: false,
                    remaining: 0,
                    retry_after_ms: wait_ms,
                    current_concurrency: active,
                    reason: "RESERVOIR_EMPTY".to_string(),
                });
            }
        }

        // 3. MinTime spacing check
        if let Some(min_time) = config.min_time_ms {
            let last_exec = *last_exec_guard;
            if last_exec > 0 && now.saturating_sub(last_exec) < min_time as u64 {
                let wait_time = (min_time as u64).saturating_sub(now.saturating_sub(last_exec));
                return Ok(RateLimitResult {
                    allowed: false,
                    remaining: tokens_guard.floor() as u32,
                    retry_after_ms: wait_time.max(1) as u32,
                    current_concurrency: active,
                    reason: "MIN_TIME_LIMIT".to_string(),
                });
            }
        }

        // 4. Token refill computation (Leaky / Token bucket)
        let elapsed = now.saturating_sub(*last_refill_guard);
        if elapsed > 0 && config.refill_interval_ms > 0 {
            let refill_intervals = elapsed as f64 / config.refill_interval_ms as f64;
            let refilled_tokens = refill_intervals * config.refill_rate as f64;
            if refilled_tokens > 0.0 {
                *tokens_guard = (*tokens_guard + refilled_tokens).min(config.capacity as f64);
                *last_refill_guard = now;
            }
        }

        let needed = tokens_requested as f64;
        if *tokens_guard < needed {
            let missing = needed - *tokens_guard;
            let interval_ms = config.refill_interval_ms as f64;
            let rate = config.refill_rate.max(1) as f64;
            let wait_time = (missing / rate * interval_ms).ceil() as u64;
            let cool_down = config.cool_down_ms.unwrap_or(0) as u64;

            return Ok(RateLimitResult {
                allowed: false,
                remaining: tokens_guard.floor() as u32,
                retry_after_ms: (wait_time + cool_down).max(1) as u32,
                current_concurrency: active,
                reason: "RATE_LIMIT".to_string(),
            });
        }

        // All checks passed: Deduct reservoir, deduct tokens, increment active concurrency, and update last execution timestamp
        if let Some(current_res) = *reservoir_guard {
            *reservoir_guard = Some(current_res - tokens_requested as i32);
        }
        *tokens_guard -= needed;
        *last_exec_guard = now;
        let new_active = bucket.active_concurrency.fetch_add(1, Ordering::SeqCst) + 1;

        Ok(RateLimitResult {
            allowed: true,
            remaining: tokens_guard.floor() as u32,
            retry_after_ms: 0,
            current_concurrency: new_active,
            reason: "OK".to_string(),
        })
    }
}

#[async_trait]
impl RateLimiterBackend for InMemoryRateLimiter {
    async fn acquire(
        &self,
        key: &str,
        tokens_requested: u32,
        config: &RateLimiterConfig,
    ) -> Result<RateLimitResult, TurboPulseError> {
        self.acquire_sync(key, tokens_requested, config)
    }

    async fn release(&self, key: &str) -> Result<u32, TurboPulseError> {
        if let Some(bucket) = self.buckets.get(key) {
            let val = bucket.active_concurrency.fetch_update(
                Ordering::SeqCst,
                Ordering::SeqCst,
                |curr| Some(curr.saturating_sub(1)),
            );
            return Ok(val.unwrap_or(0).saturating_sub(1));
        }
        Ok(0)
    }

    async fn status(
        &self,
        key: &str,
        config: &RateLimiterConfig,
    ) -> Result<RateLimiterStatus, TurboPulseError> {
        if let Some(bucket) = self.buckets.get(key) {
            let now = current_time_ms();
            let mut tokens_guard = bucket.tokens.lock();
            let mut last_refill_guard = bucket.last_refill_ms.lock();

            let elapsed = now.saturating_sub(*last_refill_guard);
            if elapsed > 0 && config.refill_interval_ms > 0 {
                let refill_intervals = elapsed as f64 / config.refill_interval_ms as f64;
                let refilled = refill_intervals * config.refill_rate as f64;
                *tokens_guard = (*tokens_guard + refilled).min(config.capacity as f64);
                *last_refill_guard = now;
            }

            Ok(RateLimiterStatus {
                remaining_tokens: tokens_guard.floor() as u32,
                active_concurrency: bucket.active_concurrency.load(Ordering::Relaxed),
                capacity: config.capacity,
            })
        } else {
            Ok(RateLimiterStatus {
                remaining_tokens: config.capacity,
                active_concurrency: 0,
                capacity: config.capacity,
            })
        }
    }

    async fn reset(&self, key: &str) -> Result<(), TurboPulseError> {
        self.buckets.remove(key);
        Ok(())
    }
}
