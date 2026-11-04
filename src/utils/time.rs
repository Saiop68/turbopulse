use std::time::{SystemTime, UNIX_EPOCH};
use rand::Rng;
use crate::config::BackoffType;

/// Returns current Unix epoch time in milliseconds.
#[inline]
pub fn current_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Computes backoff delay in milliseconds based on attempt count, algorithm, and jitter.
pub fn calculate_backoff(
    attempt: u32,
    backoff_type: BackoffType,
    base_ms: u64,
    max_ms: u64,
    jitter: bool,
) -> u64 {
    let raw_delay = match backoff_type {
        BackoffType::Fixed => base_ms,
        BackoffType::Linear => base_ms.saturating_mul(attempt.max(1) as u64),
        BackoffType::Exponential => {
            let exp = attempt.min(30);
            let multiplier = 2u64.saturating_pow(exp.saturating_sub(1));
            base_ms.saturating_mul(multiplier)
        }
    };

    let capped_delay = raw_delay.min(max_ms);

    if jitter {
        let mut rng = rand::thread_rng();
        // Add full jitter: random value between 50% and 100% of capped delay
        let min_val = capped_delay / 2;
        if capped_delay > min_val {
            rng.gen_range(min_val..=capped_delay)
        } else {
            capped_delay
        }
    } else {
        capped_delay
    }
}
