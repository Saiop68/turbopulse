pub mod job;
pub mod memory_queue;
pub mod redis_queue;

use std::sync::Arc;
use crate::error::TurboPulseError;
use crate::queue::job::{JobCounts, JobRecord};
use crate::queue::memory_queue::InMemoryQueue;
use crate::queue::redis_queue::RedisQueue;

pub enum QueueEngine {
    InMemory(Arc<InMemoryQueue>),
    Redis(Arc<RedisQueue>),
}

impl QueueEngine {
    pub async fn push_job(&self, job: JobRecord) -> Result<(), TurboPulseError> {
        match self {
            QueueEngine::InMemory(q) => q.push_job(job),
            QueueEngine::Redis(q) => q.push_job(job).await,
        }
    }

    pub async fn push_jobs_bulk(&self, jobs: Vec<JobRecord>) -> Result<Vec<String>, TurboPulseError> {
        match self {
            QueueEngine::InMemory(q) => q.push_jobs_bulk(jobs),
            QueueEngine::Redis(q) => q.push_jobs_bulk(jobs).await,
        }
    }

    pub async fn fetch_next_job(
        &self,
        queue_name: &str,
        lock_duration_ms: Option<u64>,
    ) -> Result<Option<JobRecord>, TurboPulseError> {
        match self {
            QueueEngine::InMemory(q) => q.fetch_next_job(queue_name, lock_duration_ms),
            QueueEngine::Redis(q) => q.fetch_next_job(queue_name, lock_duration_ms).await,
        }
    }

    pub async fn fetch_next_batch(
        &self,
        queue_name: &str,
        count: usize,
        lock_duration_ms: Option<u64>,
    ) -> Result<Vec<JobRecord>, TurboPulseError> {
        match self {
            QueueEngine::InMemory(q) => q.fetch_next_batch(queue_name, count, lock_duration_ms),
            QueueEngine::Redis(q) => q.fetch_next_batch(queue_name, count, lock_duration_ms).await,
        }
    }

    pub async fn complete_job(
        &self,
        queue_name: &str,
        job_id: &str,
        return_value: Option<Vec<u8>>,
        lock_token: &str,
    ) -> Result<bool, TurboPulseError> {
        // Read parent dependency info before completing if available
        let parent_info = if let Ok(Some(j)) = self.get_job(queue_name, job_id).await {
            if let Some(pid) = j.config.parent_id {
                let pq = j.config.parent_queue.unwrap_or_else(|| queue_name.to_string());
                Some((pq, pid))
            } else {
                None
            }
        } else {
            None
        };

        let res = match self {
            QueueEngine::InMemory(q) => q.complete_job(queue_name, job_id, return_value, lock_token),
            QueueEngine::Redis(q) => q.complete_job(queue_name, job_id, return_value, lock_token).await,
        }?;

        if res {
            if let Some((pq, pid)) = parent_info {
                let _ = self.resolve_parent_dep(&pq, &pid, job_id).await;
            }
        }

        Ok(res)
    }

    pub async fn fail_job(
        &self,
        queue_name: &str,
        job_id: &str,
        error_msg: &str,
        lock_token: &str,
    ) -> Result<bool, TurboPulseError> {
        match self {
            QueueEngine::InMemory(q) => q.fail_job(queue_name, job_id, error_msg, lock_token),
            QueueEngine::Redis(q) => q.fail_job(queue_name, job_id, error_msg, lock_token).await,
        }
    }

    pub async fn renew_lock(
        &self,
        queue_name: &str,
        job_id: &str,
        lock_token: &str,
        lock_duration_ms: u32,
    ) -> Result<bool, TurboPulseError> {
        match self {
            QueueEngine::InMemory(q) => q.renew_lock(queue_name, job_id, lock_token, lock_duration_ms),
            QueueEngine::Redis(q) => q.renew_lock(queue_name, job_id, lock_token, lock_duration_ms).await,
        }
    }

    pub async fn update_progress(
        &self,
        queue_name: &str,
        job_id: &str,
        progress: u32,
    ) -> Result<(), TurboPulseError> {
        match self {
            QueueEngine::InMemory(q) => q.update_progress(queue_name, job_id, progress),
            QueueEngine::Redis(q) => q.update_progress(queue_name, job_id, progress).await,
        }
    }

    pub async fn get_job(&self, queue_name: &str, job_id: &str) -> Result<Option<JobRecord>, TurboPulseError> {
        match self {
            QueueEngine::InMemory(q) => Ok(q.get_job(job_id)),
            QueueEngine::Redis(q) => q.get_job(queue_name, job_id).await,
        }
    }

    pub async fn get_counts(&self, queue_name: &str) -> Result<JobCounts, TurboPulseError> {
        match self {
            QueueEngine::InMemory(q) => Ok(q.get_counts(queue_name)),
            QueueEngine::Redis(q) => q.get_counts(queue_name).await,
        }
    }

    pub async fn pause(&self, queue_name: &str) -> Result<(), TurboPulseError> {
        match self {
            QueueEngine::InMemory(q) => {
                q.pause(queue_name);
                Ok(())
            }
            QueueEngine::Redis(q) => q.pause(queue_name).await,
        }
    }

    pub async fn resume(&self, queue_name: &str) -> Result<(), TurboPulseError> {
        match self {
            QueueEngine::InMemory(q) => {
                q.resume(queue_name);
                Ok(())
            }
            QueueEngine::Redis(q) => q.resume(queue_name).await,
        }
    }

    pub async fn drain(&self, queue_name: &str) -> Result<(), TurboPulseError> {
        match self {
            QueueEngine::InMemory(q) => {
                q.drain(queue_name);
                Ok(())
            }
            QueueEngine::Redis(q) => q.drain(queue_name).await,
        }
    }

    pub async fn obliterate(&self, queue_name: &str, force: bool) -> Result<u32, TurboPulseError> {
        match self {
            QueueEngine::InMemory(q) => q.obliterate(queue_name, force),
            QueueEngine::Redis(q) => q.obliterate(queue_name, force).await,
        }
    }

    pub async fn clean(&self, queue_name: &str, grace_ms: u64, limit: u32, state_type: &str) -> Result<u32, TurboPulseError> {
        match self {
            QueueEngine::InMemory(q) => Ok(q.clean(queue_name, grace_ms, limit, state_type)),
            QueueEngine::Redis(q) => q.clean(queue_name, grace_ms, limit, state_type).await,
        }
    }

    pub async fn promote(&self, queue_name: &str, job_id: &str) -> Result<bool, TurboPulseError> {
        match self {
            QueueEngine::InMemory(q) => Ok(q.promote(queue_name, job_id)),
            QueueEngine::Redis(q) => q.promote(queue_name, job_id).await,
        }
    }

    pub async fn recover_stalled(&self, queue_name: &str, lock_duration_ms: u64, max_stalled: u32) -> Result<u32, TurboPulseError> {
        match self {
            QueueEngine::InMemory(q) => Ok(q.recover_stalled(queue_name, lock_duration_ms, max_stalled)),
            QueueEngine::Redis(q) => q.recover_stalled(queue_name, lock_duration_ms, max_stalled).await,
        }
    }

    pub async fn update_data(&self, queue_name: &str, job_id: &str, data: Vec<u8>) -> Result<(), TurboPulseError> {
        match self {
            QueueEngine::InMemory(q) => q.update_data(queue_name, job_id, data),
            QueueEngine::Redis(q) => q.update_data(queue_name, job_id, data).await,
        }
    }

    pub async fn register_parent_children(
        &self,
        parent_queue: &str,
        parent_id: &str,
        child_ids: &[String],
    ) -> Result<(), TurboPulseError> {
        match self {
            QueueEngine::InMemory(q) => {
                q.register_parent_children(parent_id, child_ids);
                Ok(())
            }
            QueueEngine::Redis(q) => q.register_parent_children(parent_queue, parent_id, child_ids).await,
        }
    }

    pub async fn resolve_parent_dep(
        &self,
        parent_queue: &str,
        parent_id: &str,
        child_id: &str,
    ) -> Result<bool, TurboPulseError> {
        match self {
            QueueEngine::InMemory(q) => q.resolve_parent_dep(parent_queue, parent_id, child_id),
            QueueEngine::Redis(q) => q.resolve_parent_dep(parent_queue, parent_id, child_id).await,
        }
    }
}
