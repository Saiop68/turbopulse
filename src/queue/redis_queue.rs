use redis::aio::ConnectionManager;
use redis::{AsyncCommands, Client, Script};
use serde_json;

use crate::config::BackoffType;
use crate::error::TurboPulseError;
use crate::queue::job::{JobCounts, JobRecord, JobState};
use crate::utils::time::{calculate_backoff, current_time_ms};

const PUSH_JOB_LUA: &str = r#"
local wait_key = KEYS[1]
local delayed_key = KEYS[2]
local job_key = KEYS[3]

local job_id = ARGV[1]
local priority = tonumber(ARGV[2]) or 0
local delay_until = tonumber(ARGV[3]) or 0
local now = tonumber(ARGV[4])
local job_data = ARGV[5]

redis.call('SET', job_key, job_data)

if delay_until > now then
    redis.call('ZADD', delayed_key, delay_until, job_id)
else
    -- Using ZADD with priority: higher priority gets popped first via ZPOPMAX
    redis.call('ZADD', wait_key, priority, job_id)
end

return 1
"#;

const BULK_PUSH_JOB_LUA: &str = r#"
local wait_key = KEYS[1]
local delayed_key = KEYS[2]
local prefix = KEYS[3]

local now = tonumber(ARGV[1])
local count = tonumber(ARGV[2])

for i = 1, count do
    local offset = 2 + (i - 1) * 5
    local job_id = ARGV[offset + 1]
    local priority = tonumber(ARGV[offset + 2]) or 0
    local delay_until = tonumber(ARGV[offset + 3]) or 0
    local job_data = ARGV[offset + 5]

    local job_key = prefix .. ':jobs:' .. job_id
    redis.call('SET', job_key, job_data)

    if delay_until > now then
        redis.call('ZADD', delayed_key, delay_until, job_id)
    else
        redis.call('ZADD', wait_key, priority, job_id)
    end
end

return count
"#;

const FETCH_BATCH_LUA: &str = r#"
local delayed_key = KEYS[1]
local wait_key = KEYS[2]
local active_key = KEYS[3]
local meta_key = KEYS[4]

local now = tonumber(ARGV[1])
local prefix = ARGV[2]
local batch_size = tonumber(ARGV[3]) or 1

-- Check if paused
local is_paused = redis.call('HGET', meta_key, 'paused')
if is_paused == '1' then
    return {}
end

-- 1. Promote matured delayed jobs
local matured = redis.call('ZRANGEBYSCORE', delayed_key, 0, now, 'LIMIT', 0, 50)
for _, id in ipairs(matured) do
    redis.call('ZREM', delayed_key, id)
    redis.call('ZADD', wait_key, 0, id)
end

-- 2. Pop up to batch_size waiting jobs
local popped = redis.call('ZPOPMAX', wait_key, batch_size)
if #popped == 0 then
    return {}
end

local jobs = {}
for i = 1, #popped, 2 do
    local job_id = popped[i]
    redis.call('ZADD', active_key, now, job_id)
    local job_key = prefix .. ':jobs:' .. job_id
    local raw_job = redis.call('GET', job_key)
    if raw_job then
        table.insert(jobs, raw_job)
    end
end

return jobs
"#;

const COMPLETE_JOB_LUA: &str = r#"
local active_key = KEYS[1]
local completed_key = KEYS[2]
local job_key = KEYS[3]

local job_id = ARGV[1]
local return_val = ARGV[2]
local now = tonumber(ARGV[3])

redis.call('ZREM', active_key, job_id)
redis.call('ZADD', completed_key, now, job_id)

local raw = redis.call('GET', job_key)
if raw then
    local updated = string.gsub(raw, '"state":"%a+"', '"state":"completed"')
    updated = string.gsub(updated, '"finished_at":null', '"finished_at":' .. now)
    redis.call('SET', job_key, updated)
    return 1
end
return 0
"#;

const FAIL_JOB_LUA: &str = r#"
local active_key = KEYS[1]
local delayed_key = KEYS[2]
local failed_key = KEYS[3]
local job_key = KEYS[4]

local job_id = ARGV[1]
local retry_delay = tonumber(ARGV[2])
local is_final = tonumber(ARGV[3])
local now = tonumber(ARGV[4])

redis.call('ZREM', active_key, job_id)

if is_final == 1 then
    redis.call('ZADD', failed_key, now, job_id)
else
    local delay_until = now + retry_delay
    redis.call('ZADD', delayed_key, delay_until, job_id)
end

return 1
"#;

pub struct RedisQueue {
    manager: ConnectionManager,
    key_prefix: String,
    push_script: Script,
    bulk_push_script: Script,
    fetch_batch_script: Script,
    complete_script: Script,
    fail_script: Script,
}

impl RedisQueue {
    pub async fn new(redis_url: &str, key_prefix: Option<String>) -> Result<Self, TurboPulseError> {
        let client = Client::open(redis_url).map_err(TurboPulseError::Redis)?;
        let manager = ConnectionManager::new(client).await.map_err(TurboPulseError::Redis)?;

        Ok(Self {
            manager,
            key_prefix: key_prefix.unwrap_or_else(|| "turbopulse:queue".to_string()),
            push_script: Script::new(PUSH_JOB_LUA),
            bulk_push_script: Script::new(BULK_PUSH_JOB_LUA),
            fetch_batch_script: Script::new(FETCH_BATCH_LUA),
            complete_script: Script::new(COMPLETE_JOB_LUA),
            fail_script: Script::new(FAIL_JOB_LUA),
        })
    }

    #[inline]
    fn wait_key(&self, q: &str) -> String {
        format!("{}:{}:wait", self.key_prefix, q)
    }

    #[inline]
    fn delayed_key(&self, q: &str) -> String {
        format!("{}:{}:delayed", self.key_prefix, q)
    }

    #[inline]
    fn active_key(&self, q: &str) -> String {
        format!("{}:{}:active", self.key_prefix, q)
    }

    #[inline]
    fn completed_key(&self, q: &str) -> String {
        format!("{}:{}:completed", self.key_prefix, q)
    }

    #[inline]
    fn failed_key(&self, q: &str) -> String {
        format!("{}:{}:failed", self.key_prefix, q)
    }

    #[inline]
    fn job_key(&self, q: &str, id: &str) -> String {
        format!("{}:{}:jobs:{}", self.key_prefix, q, id)
    }

    #[inline]
    fn meta_key(&self, q: &str) -> String {
        format!("{}:{}:meta", self.key_prefix, q)
    }

    pub async fn push_job(&self, job: JobRecord) -> Result<(), TurboPulseError> {
        let q = &job.queue_name;
        let job_id = &job.id;
        let priority = job.config.priority.unwrap_or(0);
        let now = current_time_ms();
        let payload = serde_json::to_string(&job)?;

        let mut conn = self.manager.clone();
        let _: i64 = self
            .push_script
            .key(self.wait_key(q))
            .key(self.delayed_key(q))
            .key(self.job_key(q, job_id))
            .arg(job_id)
            .arg(priority)
            .arg(job.delay_until_ms)
            .arg(now)
            .arg(payload)
            .invoke_async(&mut conn)
            .await
            .map_err(TurboPulseError::Redis)?;

        Ok(())
    }

    pub async fn push_jobs_bulk(&self, jobs: Vec<JobRecord>) -> Result<Vec<String>, TurboPulseError> {
        if jobs.is_empty() {
            return Ok(vec![]);
        }

        let now = current_time_ms();
        let q = &jobs[0].queue_name;
        let prefix = format!("{}:{}", self.key_prefix, q);
        let mut ids = Vec::with_capacity(jobs.len());

        for chunk in jobs.chunks(500) {
            let mut script_inv = self.bulk_push_script.key(self.wait_key(q));
            script_inv.key(self.delayed_key(q));
            script_inv.key(&prefix);
            script_inv.arg(now);
            script_inv.arg(chunk.len() as i64);

            for job in chunk {
                let priority = job.config.priority.unwrap_or(0);
                let payload = serde_json::to_string(&job)?;
                script_inv.arg(&job.id);
                script_inv.arg(priority);
                script_inv.arg(job.delay_until_ms);
                script_inv.arg(now);
                script_inv.arg(payload);

                ids.push(job.id.clone());
            }

            let mut conn = self.manager.clone();
            let _: i64 = script_inv.invoke_async(&mut conn).await.map_err(TurboPulseError::Redis)?;
        }

        Ok(ids)
    }

    pub async fn fetch_next_job(&self, queue_name: &str) -> Result<Option<JobRecord>, TurboPulseError> {
        let batch = self.fetch_next_batch(queue_name, 1).await?;
        Ok(batch.into_iter().next())
    }

    pub async fn fetch_next_batch(&self, queue_name: &str, count: usize) -> Result<Vec<JobRecord>, TurboPulseError> {
        let now = current_time_ms();
        let queue_prefix = format!("{}:{}", self.key_prefix, queue_name);

        let mut conn = self.manager.clone();
        let raw_list: Vec<String> = self
            .fetch_batch_script
            .key(self.delayed_key(queue_name))
            .key(self.wait_key(queue_name))
            .key(self.active_key(queue_name))
            .key(self.meta_key(queue_name))
            .arg(now)
            .arg(&queue_prefix)
            .arg(count as i64)
            .invoke_async(&mut conn)
            .await
            .map_err(TurboPulseError::Redis)?;

        let mut jobs = Vec::with_capacity(raw_list.len());
        for raw in raw_list {
            if let Ok(mut job) = serde_json::from_str::<JobRecord>(&raw) {
                job.state = JobState::Active;
                job.attempts_made += 1;
                job.processed_at = Some(now);
                jobs.push(job);
            }
        }

        Ok(jobs)
    }

    pub async fn complete_job(
        &self,
        queue_name: &str,
        job_id: &str,
        return_value: Option<Vec<u8>>,
    ) -> Result<(), TurboPulseError> {
        let now = current_time_ms();
        let mut conn = self.manager.clone();
        let key = self.job_key(queue_name, job_id);

        let ret_str = return_value
            .map(|v| String::from_utf8_lossy(&v).to_string())
            .unwrap_or_default();

        let _: i64 = self
            .complete_script
            .key(self.active_key(queue_name))
            .key(self.completed_key(queue_name))
            .key(&key)
            .arg(job_id)
            .arg(ret_str)
            .arg(now)
            .invoke_async(&mut conn)
            .await
            .map_err(TurboPulseError::Redis)?;

        Ok(())
    }

    pub async fn fail_job(
        &self,
        queue_name: &str,
        job_id: &str,
        error_msg: &str,
    ) -> Result<bool, TurboPulseError> {
        let now = current_time_ms();
        let mut conn = self.manager.clone();
        let key = self.job_key(queue_name, job_id);

        let raw: Option<String> = conn.get(&key).await.map_err(TurboPulseError::Redis)?;
        if let Some(s) = raw {
            if let Ok(mut job) = serde_json::from_str::<JobRecord>(&s) {
                let max_retries = job.config.max_retries.unwrap_or(3);
                let will_retry = job.attempts_made < max_retries;

                let (retry_delay, is_final) = if will_retry {
                    let backoff_type = job.config.backoff_type.unwrap_or(BackoffType::Exponential);
                    let base_ms = job.config.backoff_base_ms.unwrap_or(1000) as u64;
                    let max_ms = job.config.backoff_max_ms.unwrap_or(60000) as u64;
                    let jitter = job.config.jitter.unwrap_or(true);
                    let delay = calculate_backoff(job.attempts_made, backoff_type, base_ms, max_ms, jitter);

                    job.state = JobState::Delayed;
                    job.delay_until_ms = now + delay;
                    job.failed_reason = Some(format!("Retry {}/{}: {}", job.attempts_made, max_retries, error_msg));
                    (delay, 0)
                } else {
                    job.state = JobState::Failed;
                    job.finished_at = Some(now);
                    job.failed_reason = Some(error_msg.to_string());
                    (0, 1)
                };

                if let Ok(updated) = serde_json::to_string(&job) {
                    let _: () = conn.set(&key, updated).await.unwrap_or(());
                }

                let _: i64 = self
                    .fail_script
                    .key(self.active_key(queue_name))
                    .key(self.delayed_key(queue_name))
                    .key(self.failed_key(queue_name))
                    .key(&key)
                    .arg(job_id)
                    .arg(retry_delay)
                    .arg(is_final)
                    .arg(now)
                    .invoke_async(&mut conn)
                    .await
                    .map_err(TurboPulseError::Redis)?;

                return Ok(is_final == 1);
            }
        }

        Ok(true)
    }

    pub async fn update_progress(
        &self,
        queue_name: &str,
        job_id: &str,
        progress: u32,
    ) -> Result<(), TurboPulseError> {
        let mut conn = self.manager.clone();
        let key = self.job_key(queue_name, job_id);
        let raw: Option<String> = conn.get(&key).await.map_err(TurboPulseError::Redis)?;
        if let Some(s) = raw {
            if let Ok(mut job) = serde_json::from_str::<JobRecord>(&s) {
                job.progress = progress.min(100);
                let updated = serde_json::to_string(&job)?;
                conn.set::<_, _, ()>(&key, updated).await.map_err(TurboPulseError::Redis)?;
                return Ok(());
            }
        }
        Err(TurboPulseError::JobNotFound(job_id.to_string()))
    }

    pub async fn get_job(&self, queue_name: &str, job_id: &str) -> Result<Option<JobRecord>, TurboPulseError> {
        let mut conn = self.manager.clone();
        let key = self.job_key(queue_name, job_id);
        let raw: Option<String> = conn.get(&key).await.map_err(TurboPulseError::Redis)?;
        if let Some(s) = raw {
            let job: JobRecord = serde_json::from_str(&s)?;
            Ok(Some(job))
        } else {
            Ok(None)
        }
    }

    pub async fn get_counts(&self, queue_name: &str) -> Result<JobCounts, TurboPulseError> {
        let mut conn = self.manager.clone();
        let wait: u32 = conn.zcard(self.wait_key(queue_name)).await.unwrap_or(0);
        let active: u32 = conn.zcard(self.active_key(queue_name)).await.unwrap_or(0);
        let delayed: u32 = conn.zcard(self.delayed_key(queue_name)).await.unwrap_or(0);
        let completed: u32 = conn.zcard(self.completed_key(queue_name)).await.unwrap_or(0);
        let failed: u32 = conn.zcard(self.failed_key(queue_name)).await.unwrap_or(0);

        Ok(JobCounts {
            waiting: wait,
            active,
            delayed,
            completed,
            failed,
        })
    }

    pub async fn pause(&self, queue_name: &str) -> Result<(), TurboPulseError> {
        let mut conn = self.manager.clone();
        conn.hset::<_, _, _, ()>(self.meta_key(queue_name), "paused", "1")
            .await
            .map_err(TurboPulseError::Redis)
    }

    pub async fn resume(&self, queue_name: &str) -> Result<(), TurboPulseError> {
        let mut conn = self.manager.clone();
        conn.hset::<_, _, _, ()>(self.meta_key(queue_name), "paused", "0")
            .await
            .map_err(TurboPulseError::Redis)
    }

    pub async fn drain(&self, queue_name: &str) -> Result<(), TurboPulseError> {
        let mut conn = self.manager.clone();
        let _: () = conn.del(self.wait_key(queue_name)).await.unwrap_or(());
        let _: () = conn.del(self.delayed_key(queue_name)).await.unwrap_or(());
        Ok(())
    }

    pub async fn clean(&self, queue_name: &str, grace_ms: u64, limit: u32, state_type: &str) -> Result<u32, TurboPulseError> {
        let now = current_time_ms();
        let max_score = now.saturating_sub(grace_ms);
        let target_key = match state_type {
            "completed" => self.completed_key(queue_name),
            "failed" => self.failed_key(queue_name),
            _ => return Ok(0),
        };

        let mut conn = self.manager.clone();
        let ids: Vec<String> = conn.zrangebyscore_limit(&target_key, 0, max_score, 0, limit as isize)
            .await
            .unwrap_or_default();

        let count = ids.len() as u32;
        for id in ids {
            let _: () = conn.zrem(&target_key, &id).await.unwrap_or(());
            let _: () = conn.del(self.job_key(queue_name, &id)).await.unwrap_or(());
        }

        Ok(count)
    }

    pub async fn promote(&self, queue_name: &str, job_id: &str) -> Result<bool, TurboPulseError> {
        let mut conn = self.manager.clone();
        let removed: i64 = conn.zrem(self.delayed_key(queue_name), job_id).await.map_err(TurboPulseError::Redis)?;
        if removed > 0 {
            conn.zadd::<_, _, _, ()>(self.wait_key(queue_name), job_id, 0).await.map_err(TurboPulseError::Redis)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub async fn recover_stalled(&self, queue_name: &str, lock_duration_ms: u64, max_stalled: u32) -> Result<u32, TurboPulseError> {
        let now = current_time_ms();
        let max_score = now.saturating_sub(lock_duration_ms);
        let mut conn = self.manager.clone();

        let stalled_ids: Vec<String> = conn.zrangebyscore_limit(self.active_key(queue_name), 0, max_score, 0, 100)
            .await
            .unwrap_or_default();

        let mut recovered = 0;
        for id in stalled_ids {
            let _: () = conn.zrem(self.active_key(queue_name), &id).await.unwrap_or(());
            let job_k = self.job_key(queue_name, &id);
            if let Ok(Some(raw)) = conn.get::<_, Option<String>>(&job_k).await {
                if let Ok(mut job) = serde_json::from_str::<JobRecord>(&raw) {
                    if job.attempts_made < max_stalled {
                        job.state = JobState::Waiting;
                        if let Ok(updated) = serde_json::to_string(&job) {
                            let _: () = conn.set(&job_k, updated).await.unwrap_or(());
                        }
                        let _: () = conn.zadd(self.wait_key(queue_name), &id, 0).await.unwrap_or(());
                        recovered += 1;
                    } else {
                        job.state = JobState::Failed;
                        job.finished_at = Some(now);
                        job.failed_reason = Some(format!("Stalled more than {} times", max_stalled));
                        if let Ok(updated) = serde_json::to_string(&job) {
                            let _: () = conn.set(&job_k, updated).await.unwrap_or(());
                        }
                        let _: () = conn.zadd(self.failed_key(queue_name), &id, now).await.unwrap_or(());
                    }
                }
            }
        }

        Ok(recovered)
    }

    pub async fn update_data(&self, queue_name: &str, job_id: &str, data: Vec<u8>) -> Result<(), TurboPulseError> {
        let mut conn = self.manager.clone();
        let key = self.job_key(queue_name, job_id);
        let raw: Option<String> = conn.get(&key).await.map_err(TurboPulseError::Redis)?;
        if let Some(s) = raw {
            if let Ok(mut job) = serde_json::from_str::<JobRecord>(&s) {
                job.data = data;
                let updated = serde_json::to_string(&job)?;
                conn.set::<_, _, ()>(&key, updated).await.map_err(TurboPulseError::Redis)?;
                return Ok(());
            }
        }
        Err(TurboPulseError::JobNotFound(job_id.to_string()))
    }
}
