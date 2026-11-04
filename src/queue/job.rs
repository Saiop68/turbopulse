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
    WaitingChildren,
}

fn deserialize_flexible_bytes<'de, D>(deserializer: D) -> Result<Option<Vec<u8>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Deserialize;
    let opt = Option::<serde_json::Value>::deserialize(deserializer)?;
    match opt {
        None => Ok(None),
        Some(serde_json::Value::Null) => Ok(None),
        Some(serde_json::Value::Array(arr)) => {
            let mut bytes = Vec::with_capacity(arr.len());
            for v in &arr {
                if let Some(b) = v.as_u64() {
                    bytes.push(b as u8);
                } else {
                    let s = serde_json::to_string(&serde_json::Value::Array(arr)).map_err(serde::de::Error::custom)?;
                    return Ok(Some(s.into_bytes()));
                }
            }
            Ok(Some(bytes))
        }
        Some(serde_json::Value::String(s)) => Ok(Some(s.into_bytes())),
        Some(other) => {
            let s = serde_json::to_string(&other).map_err(serde::de::Error::custom)?;
            Ok(Some(s.into_bytes()))
        }
    }
}

fn deserialize_flexible_bytes_required<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    deserialize_flexible_bytes(deserializer).map(|opt| opt.unwrap_or_default())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobRecord {
    pub id: String,
    pub name: String,
    pub queue_name: String,
    #[serde(default, deserialize_with = "deserialize_flexible_bytes_required")]
    pub data: Vec<u8>,
    pub config: JobConfig,
    pub state: JobState,
    pub attempts_made: u32,
    #[serde(default)]
    pub stalled_count: u32,
    pub progress: u32,
    pub created_at: u64,
    pub processed_at: Option<u64>,
    pub finished_at: Option<u64>,
    pub failed_reason: Option<String>,
    #[serde(default, deserialize_with = "deserialize_flexible_bytes")]
    pub return_value: Option<Vec<u8>>,
    pub delay_until_ms: u64,
    #[serde(default)]
    pub lock_token: Option<String>,
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
    pub stalled_count: u32,
    pub progress: u32,
    pub created_at: i64,
    pub processed_at: Option<i64>,
    pub finished_at: Option<i64>,
    pub failed_reason: Option<String>,
    pub return_value: Option<Buffer>,
    pub lock_token: Option<String>,
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
            stalled_count: self.stalled_count,
            progress: self.progress,
            created_at: self.created_at as i64,
            processed_at: self.processed_at.map(|v| v as i64),
            finished_at: self.finished_at.map(|v| v as i64),
            failed_reason: self.failed_reason.clone(),
            return_value: self.return_value.as_ref().map(|v| Buffer::from(v.clone())),
            lock_token: self.lock_token.clone(),
        }
    }
}
