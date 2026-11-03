use thiserror::Error;
use napi::bindgen_prelude::*;

#[derive(Error, Debug)]
pub enum TurboPulseError {
    #[error("Rate limit exceeded for key '{key}'. Retry after {retry_after_ms}ms")]
    RateLimitExceeded {
        key: String,
        retry_after_ms: u64,
    },

    #[error("Maximum concurrency limit of {max_concurrent} reached for key '{key}'")]
    ConcurrencyLimitReached {
        key: String,
        max_concurrent: u32,
    },

    #[error("Job '{0}' not found")]
    JobNotFound(String),

    #[error("Queue '{0}' is paused or full")]
    QueueFullOrPaused(String),

    #[error("Redis operation failed: {0}")]
    Redis(#[from] redis::RedisError),

    #[error("Serialization / Deserialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("Internal runtime error: {0}")]
    Internal(String),

    #[error("Worker error: {0}")]
    Worker(String),
}

impl From<TurboPulseError> for napi::Error {
    fn from(err: TurboPulseError) -> Self {
        match &err {
            TurboPulseError::RateLimitExceeded { retry_after_ms: _, .. } => {
                napi::Error::new(
                    Status::GenericFailure,
                    format!("RATE_LIMIT_EXCEEDED: {}", err),
                )
            }
            TurboPulseError::ConcurrencyLimitReached { .. } => {
                napi::Error::new(Status::GenericFailure, format!("CONCURRENCY_LIMIT: {}", err))
            }
            TurboPulseError::JobNotFound(_) => {
                napi::Error::new(Status::GenericFailure, format!("JOB_NOT_FOUND: {}", err))
            }
            TurboPulseError::Redis(e) => {
                napi::Error::new(Status::GenericFailure, format!("REDIS_ERROR: {}", e))
            }
            TurboPulseError::Serialization(e) => {
                napi::Error::new(Status::InvalidArg, format!("SERIALIZATION_ERROR: {}", e))
            }
            TurboPulseError::QueueFullOrPaused(msg) => {
                napi::Error::new(Status::GenericFailure, format!("QUEUE_UNAVAILABLE: {}", msg))
            }
            TurboPulseError::Internal(msg) | TurboPulseError::Worker(msg) => {
                napi::Error::new(Status::GenericFailure, format!("TURBOPULSE_ERROR: {}", msg))
            }
        }
    }
}
