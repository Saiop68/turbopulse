use napi::bindgen_prelude::Buffer;
use napi_derive::napi;
use serde::{Deserialize, Serialize};
use crate::config::JobConfig;

#[napi(string_enum)]
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum JobState {
    Waiting,
    Delayed,
    Active,
    Completed,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobRecord {
    pub id: String,
    pub name: String,
    pub queue_name: String,
    pub data: Vec<u8>,
    pub config: JobConfig,
    pub state: JobState,
    pub attempts_made: u32,
    pub progress: u32,
    pub created_at: u64,
    pub processed_at: Option<u64>,
    pub finished_at: Option<u64>,
    pub failed_reason: Option<String>,
    pub return_value: Option<Vec<u8>>,
    pub delay_until_ms: u64,
}

#[napi(object)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobCounts {
    pub waiting: u32,
    pub active: u32,
    pub delayed: u32,
    pub completed: u32,
    pub failed: u32,
}

/// NAPI-exposed Job representation for TypeScript wrapper
#[napi(object)]
#[derive(Clone)]
pub struct JobDto {
    pub id: String,
    pub name: String,
    pub queue_name: String,
    pub data: Buffer,
    pub state: JobState,
    pub attempts_made: u32,
    pub progress: u32,
    pub created_at: i64,
    pub processed_at: Option<i64>,
    pub finished_at: Option<i64>,
    pub failed_reason: Option<String>,
    pub return_value: Option<Buffer>,
}

impl JobRecord {
    pub fn to_dto(&self) -> JobDto {
        JobDto {
            id: self.id.clone(),
            name: self.name.clone(),
            queue_name: self.queue_name.clone(),
            data: Buffer::from(self.data.clone()),
            state: self.state,
            attempts_made: self.attempts_made,
            progress: self.progress,
            created_at: self.created_at as i64,
            processed_at: self.processed_at.map(|v| v as i64),
            finished_at: self.finished_at.map(|v| v as i64),
            failed_reason: self.failed_reason.clone(),
            return_value: self.return_value.as_ref().map(|v| Buffer::from(v.clone())),
        }
    }
}
