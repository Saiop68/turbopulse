use redis::Script;
use serde_json;

use crate::config::BackoffType;
use crate::error::TurboPulseError;
use crate::queue::job::{JobCounts, JobRecord};
use crate::redis_backend::RedisBackend;
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
    -- Composite score guarantees:
    -- 1. Higher priority jobs pop first (via ZPOPMAX)
    -- 2. Equal priority jobs strictly preserve FIFO arrival order
    local fifo_tiebreaker = 1000000000000 - (now % 1000000000000)
    local score = (priority * 1000000000000) + fifo_tiebreaker
    redis.call('ZADD', wait_key, score, job_id)
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
        local fifo_tiebreaker = 1000000000000 - (now % 1000000000000) + (count - i)
        local score = (priority * 1000000000000) + fifo_tiebreaker
        redis.call('ZADD', wait_key, score, job_id)
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
local lock_duration = tonumber(ARGV[4]) or 30000

-- Check if paused
local is_paused = redis.call('HGET', meta_key, 'paused')
if is_paused == '1' then
    return {}
end

-- 1. Move ready delayed jobs to wait queue preserving priority
local ready_delayed = redis.call('ZRANGEBYSCORE', delayed_key, 0, now, 'LIMIT', 0, 100)
for _, id in ipairs(ready_delayed) do
    redis.call('ZREM', delayed_key, id)
    local job_key = prefix .. ':jobs:' .. id
    local raw_job = redis.call('GET', job_key)
    local priority = 0
    if raw_job then
        local ok, data = pcall(cjson.decode, raw_job)
        if ok and type(data) == 'table' and data['config'] and data['config']['priority'] then
            priority = tonumber(data['config']['priority']) or 0
        end
    end
    local fifo_tiebreaker = 1000000000000 - (now % 1000000000000)
    local composite_score = (priority * 1000000000000) + fifo_tiebreaker
    redis.call('ZADD', wait_key, composite_score, id)
end

-- 2. Pop up to batch_size waiting jobs
local popped = redis.call('ZPOPMAX', wait_key, batch_size)
if #popped == 0 then
    return {}
end

local jobs = {}
for i = 1, #popped, 2 do
    local job_id = popped[i]
    local lock_token = tostring(now) .. '-' .. tostring(redis.call('INCR', prefix .. ':lock_seq'))
    redis.call('ZADD', active_key, now + lock_duration, job_id)
    local job_key = prefix .. ':jobs:' .. job_id
    local raw_job = redis.call('GET', job_key)
    if raw_job then
        local ok, data = pcall(cjson.decode, raw_job)
        if ok and type(data) == 'table' then
            data['state'] = 'Active'
            data['attempts_made'] = (tonumber(data['attempts_made']) or 0) + 1
            data['processed_at'] = now
            data['lock_token'] = lock_token
            local updated_raw = cjson.encode(data)
            redis.call('SET', job_key, updated_raw)
            table.insert(jobs, updated_raw)
        else
            table.insert(jobs, raw_job)
        end
    end
end

return jobs
"#;

const COMPLETE_JOB_LUA: &str = r#"
local active_key = KEYS[1]
local completed_key = KEYS[2]
local job_key = KEYS[3]
local meta_prefix = KEYS[4]

local job_id = ARGV[1]
local return_val = ARGV[2]
local now = tonumber(ARGV[3])
local lock_token = ARGV[4] or ''

local raw = redis.call('GET', job_key)
if not raw then
    return 0
end

local ok, data = pcall(cjson.decode, raw)
if not ok or type(data) ~= 'table' then
    return 0
end

-- Verify ownership if lock_token supplied
if lock_token ~= '' and data['lock_token'] and data['lock_token'] ~= lock_token then
    return -1 -- LOCK_LOST
end

redis.call('ZREM', active_key, job_id)
redis.call('ZADD', completed_key, now, job_id)

data['state'] = 'Completed'
data['finished_at'] = now
data['progress'] = 100
if return_val and #return_val > 0 then
    local ok_ret, parsed_ret = pcall(cjson.decode, return_val)
    if ok_ret then
        data['return_value'] = parsed_ret
    else
        data['return_value'] = return_val
    end
end
redis.call('SET', job_key, cjson.encode(data))

-- Check parent DAG resolution
local parent_id = nil
if data['config'] and data['config']['parent_id'] then
    parent_id = data['config']['parent_id']
end

if parent_id and meta_prefix and #meta_prefix > 0 then
    local dep_key = meta_prefix .. ':deps:' .. parent_id
    local pending = redis.call('DECR', dep_key)
    if pending <= 0 then
        redis.call('DEL', dep_key)
        local parent_job_key = meta_prefix .. ':jobs:' .. parent_id
        local parent_raw = redis.call('GET', parent_job_key)
        if parent_raw then
            local ok_p, p_data = pcall(cjson.decode, parent_raw)
            if ok_p and type(p_data) == 'table' then
                local p_queue = p_data['queue_name'] or 'default'
                local p_wait = '{' .. meta_prefix .. ':' .. p_queue .. '}:wait'
                local p_delayed = '{' .. meta_prefix .. ':' .. p_queue .. '}:delayed'
                redis.call('ZREM', p_delayed, parent_id)
                local prio = 0
                if p_data['config'] and p_data['config']['priority'] then
                    prio = tonumber(p_data['config']['priority']) or 0
                end
                local fifo_tiebreaker = 1000000000000 - (now % 1000000000000)
                local p_score = (prio * 1000000000000) + fifo_tiebreaker
                p_data['state'] = 'Waiting'
                redis.call('SET', parent_job_key, cjson.encode(p_data))
                redis.call('ZADD', p_wait, p_score, parent_id)
            end
        end
    end
end

return 1
"#;

const FAIL_JOB_LUA: &str = r#"
local active_key = KEYS[1]
local delayed_key = KEYS[2]
local failed_key = KEYS[3]
local job_key = KEYS[4]

local job_id = ARGV[1]
local error_msg = ARGV[2]
local retry_delay = tonumber(ARGV[3]) or 0
local is_final = tonumber(ARGV[4]) or 0
local now = tonumber(ARGV[5])
local lock_token = ARGV[6] or ''

local raw = redis.call('GET', job_key)
if not raw then
    return 0
end

local ok, data = pcall(cjson.decode, raw)
if not ok or type(data) ~= 'table' then
    return 0
end

-- Verify ownership if lock_token supplied
if lock_token ~= '' and data['lock_token'] and data['lock_token'] ~= lock_token then
    return -1 -- LOCK_LOST
end

redis.call('ZREM', active_key, job_id)

data['failed_reason'] = error_msg
if is_final == 1 then
    data['state'] = 'Failed'
    data['finished_at'] = now
    redis.call('ZADD', failed_key, now, job_id)
else
    data['state'] = 'Delayed'
    local delay_until = now + retry_delay
    data['delay_until_ms'] = delay_until
    redis.call('ZADD', delayed_key, delay_until, job_id)
end
redis.call('SET', job_key, cjson.encode(data))

return 1
"#;

const RENEW_LOCK_LUA: &str = r#"
local active_key = KEYS[1]
local job_key = KEYS[2]

local job_id = ARGV[1]
local lock_token = ARGV[2] or ''
local lock_duration = tonumber(ARGV[3]) or 30000
local now = tonumber(ARGV[4])

local raw = redis.call('GET', job_key)
if not raw then
    return 0
end

local ok, data = pcall(cjson.decode, raw)
if not ok or type(data) ~= 'table' then
    return 0
end

if data['state'] ~= 'Active' then
    return 0
end

if lock_token ~= '' and data['lock_token'] and data['lock_token'] ~= lock_token then
    return -1 -- LOCK_LOST
end

redis.call('ZADD', active_key, now + lock_duration, job_id)
return 1
"#;

const RECOVER_STALLED_LUA: &str = r#"
local active_key = KEYS[1]
local wait_key = KEYS[2]
local failed_key = KEYS[3]

local prefix = ARGV[1]
local stale_threshold = tonumber(ARGV[2])
local max_stalled = tonumber(ARGV[3]) or 3
local now = tonumber(ARGV[4])

local stalled_ids = redis.call('ZRANGEBYSCORE', active_key, 0, stale_threshold, 'LIMIT', 0, 100)
local recovered = 0

for _, id in ipairs(stalled_ids) do
    redis.call('ZREM', active_key, id)
    local job_key = prefix .. ':jobs:' .. id
    local raw = redis.call('GET', job_key)
    if raw then
        local ok, data = pcall(cjson.decode, raw)
        if ok and type(data) == 'table' then
            local stalled_count = (tonumber(data['stalled_count']) or 0) + 1
            data['stalled_count'] = stalled_count
            if stalled_count > max_stalled then
                data['state'] = 'Failed'
                data['finished_at'] = now
                data['failed_reason'] = 'Job stalled more than maxStalledCount times'
                redis.call('ZADD', failed_key, now, id)
            else
                data['state'] = 'Waiting'
                local prio = 0
                if data['config'] and data['config']['priority'] then
                    prio = tonumber(data['config']['priority']) or 0
                end
                local fifo_tiebreaker = 1000000000000 - (now % 1000000000000)
                local composite_score = (prio * 1000000000000) + fifo_tiebreaker
                redis.call('ZADD', wait_key, composite_score, id)
            end
            redis.call('SET', job_key, cjson.encode(data))
            recovered = recovered + 1
        end
    end
end

return recovered
"#;

const REGISTER_PARENT_CHILDREN_LUA: &str = r#"
local dep_key = KEYS[1]
local count = tonumber(ARGV[1]) or 0
redis.call('SET', dep_key, count)
return 1
"#;

pub struct RedisQueue {
    backend: RedisBackend,
    key_prefix: String,
    push_script: Script,
    bulk_push_script: Script,
    fetch_batch_script: Script,
    complete_script: Script,
    fail_script: Script,
    renew_lock_script: Script,
    recover_stalled_script: Script,
    register_parent_children_script: Script,
}

impl RedisQueue {
    pub async fn new(redis_url: &str, key_prefix: Option<String>) -> Result<Self, TurboPulseError> {
        let backend = RedisBackend::new(redis_url).await?;

        Ok(Self {
            backend,
            key_prefix: key_prefix.unwrap_or_else(|| "turbopulse:queue".to_string()),
            push_script: Script::new(PUSH_JOB_LUA),
            bulk_push_script: Script::new(BULK_PUSH_JOB_LUA),
            fetch_batch_script: Script::new(FETCH_BATCH_LUA),
            complete_script: Script::new(COMPLETE_JOB_LUA),
            fail_script: Script::new(FAIL_JOB_LUA),
            renew_lock_script: Script::new(RENEW_LOCK_LUA),
            recover_stalled_script: Script::new(RECOVER_STALLED_LUA),
            register_parent_children_script: Script::new(REGISTER_PARENT_CHILDREN_LUA),
        })
    }

    #[inline]
    fn prefix(&self, q: &str) -> String {
        format!("{{{}:{}}}", self.key_prefix, q)
    }

    #[inline]
    fn wait_key(&self, q: &str) -> String {
        format!("{}:wait", self.prefix(q))
    }

    #[inline]
    fn delayed_key(&self, q: &str) -> String {
        format!("{}:delayed", self.prefix(q))
    }

    #[inline]
    fn active_key(&self, q: &str) -> String {
        format!("{}:active", self.prefix(q))
    }

    #[inline]
    fn completed_key(&self, q: &str) -> String {
        format!("{}:completed", self.prefix(q))
    }

    #[inline]
    fn failed_key(&self, q: &str) -> String {
        format!("{}:failed", self.prefix(q))
    }

    #[inline]
    fn job_key(&self, q: &str, id: &str) -> String {
        format!("{}:jobs:{}", self.prefix(q), id)
    }

    #[inline]
    fn meta_key(&self, q: &str) -> String {
        format!("{}:meta", self.prefix(q))
    }

    pub async fn push_job(&self, job: JobRecord) -> Result<(), TurboPulseError> {
        let q = &job.queue_name;
        let job_id = &job.id;
        let priority = job.config.priority.unwrap_or(0);
        let now = current_time_ms();
        let payload = serde_json::to_string(&job)?;

        let mut inv = self.push_script.key(self.wait_key(q));
        inv.key(self.delayed_key(q))
            .key(self.job_key(q, job_id))
            .arg(job_id)
            .arg(priority)
            .arg(job.delay_until_ms)
            .arg(now)
            .arg(payload);

        let _: i64 = self.backend.invoke_script(&inv).await?;

        Ok(())
    }

    pub async fn push_jobs_bulk(&self, jobs: Vec<JobRecord>) -> Result<Vec<String>, TurboPulseError> {
        if jobs.is_empty() {
            return Ok(vec![]);
        }

        let now = current_time_ms();
        let q = &jobs[0].queue_name;
        let prefix = self.prefix(q);
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

            let _: i64 = self.backend.invoke_script(&script_inv).await?;
        }

        Ok(ids)
    }

    pub async fn fetch_next_job(&self, queue_name: &str) -> Result<Option<JobRecord>, TurboPulseError> {
        let batch = self.fetch_next_batch(queue_name, 1).await?;
        Ok(batch.into_iter().next())
    }

    pub async fn fetch_next_batch(&self, queue_name: &str, count: usize) -> Result<Vec<JobRecord>, TurboPulseError> {
        let now = current_time_ms();
        let queue_prefix = self.prefix(queue_name);

        let mut inv = self.fetch_batch_script.key(self.delayed_key(queue_name));
        inv.key(self.wait_key(queue_name))
            .key(self.active_key(queue_name))
            .key(self.meta_key(queue_name))
            .arg(now)
            .arg(&queue_prefix)
            .arg(count as i64)
            .arg(30000i64);

        let raw_list: Vec<String> = self.backend.invoke_script(&inv).await?;

        let mut jobs = Vec::with_capacity(raw_list.len());
        for raw in raw_list {
            if let Ok(job) = serde_json::from_str::<JobRecord>(&raw) {
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
        lock_token: Option<&str>,
    ) -> Result<bool, TurboPulseError> {
        let now = current_time_ms();
        let key = self.job_key(queue_name, job_id);

        let ret_str = return_value
            .map(|v| String::from_utf8_lossy(&v).to_string())
            .unwrap_or_default();

        let mut inv = self.complete_script.key(self.active_key(queue_name));
        inv.key(self.completed_key(queue_name))
            .key(&key)
            .key(self.key_prefix.clone())
            .arg(job_id)
            .arg(ret_str)
            .arg(now)
            .arg(lock_token.unwrap_or(""));

        let res: i64 = self.backend.invoke_script(&inv).await?;
        if res == -1 {
            return Err(TurboPulseError::InvalidJobState("LOCK_LOST: Job lock was lost or expired".to_string()));
        }

        Ok(res > 0)
    }

    pub async fn fail_job(
        &self,
        queue_name: &str,
        job_id: &str,
        error_msg: &str,
        lock_token: Option<&str>,
    ) -> Result<bool, TurboPulseError> {
        let now = current_time_ms();
        let key = self.job_key(queue_name, job_id);

        let raw = self.backend.get(&key).await?;
        if let Some(s) = raw {
            if let Ok(job) = serde_json::from_str::<JobRecord>(&s) {
                let max_retries = job.config.max_retries.unwrap_or(3);
                // Note: job.attempts_made is persisted directly in Redis by FETCH_BATCH_LUA
                let will_retry = job.attempts_made < max_retries;

                let (retry_delay, is_final, reason_str) = if will_retry {
                    let backoff_type = job.config.backoff_type.unwrap_or(BackoffType::Exponential);
                    let base_ms = job.config.backoff_base_ms.unwrap_or(1000) as u64;
                    let max_ms = job.config.backoff_max_ms.unwrap_or(60000) as u64;
                    let jitter = job.config.jitter.unwrap_or(true);
                    let delay = calculate_backoff(job.attempts_made, backoff_type, base_ms, max_ms, jitter);

                    (delay, 0, format!("Retry {}/{}: {}", job.attempts_made, max_retries, error_msg))
                } else {
                    (0, 1, error_msg.to_string())
                };

                let mut inv = self.fail_script.key(self.active_key(queue_name));
                inv.key(self.delayed_key(queue_name))
                    .key(self.failed_key(queue_name))
                    .key(&key)
                    .arg(job_id)
                    .arg(reason_str)
                    .arg(retry_delay)
                    .arg(is_final)
                    .arg(now)
                    .arg(lock_token.unwrap_or(""));

                let res: i64 = self.backend.invoke_script(&inv).await?;
                if res == -1 {
                    return Err(TurboPulseError::InvalidJobState("LOCK_LOST: Job lock was lost or expired".to_string()));
                }

                return Ok(is_final == 1);
            }
        }

        Ok(true)
    }

    pub async fn renew_lock(
        &self,
        queue_name: &str,
        job_id: &str,
        lock_token: &str,
        lock_duration_ms: u32,
    ) -> Result<bool, TurboPulseError> {
        let now = current_time_ms();
        let key = self.job_key(queue_name, job_id);

        let mut inv = self.renew_lock_script.key(self.active_key(queue_name));
        inv.key(&key)
            .arg(job_id)
            .arg(lock_token)
            .arg(lock_duration_ms)
            .arg(now);

        let res: i64 = self.backend.invoke_script(&inv).await?;
        Ok(res == 1)
    }

    pub async fn update_progress(
        &self,
        queue_name: &str,
        job_id: &str,
        progress: u32,
    ) -> Result<(), TurboPulseError> {
        let key = self.job_key(queue_name, job_id);
        let raw = self.backend.get(&key).await?;
        if let Some(s) = raw {
            if let Ok(mut job) = serde_json::from_str::<JobRecord>(&s) {
                job.progress = progress.min(100);
                let updated = serde_json::to_string(&job)?;
                self.backend.set(&key, &updated).await?;
                return Ok(());
            }
        }
        Err(TurboPulseError::JobNotFound(job_id.to_string()))
    }

    pub async fn get_job(&self, queue_name: &str, job_id: &str) -> Result<Option<JobRecord>, TurboPulseError> {
        let key = self.job_key(queue_name, job_id);
        let raw = self.backend.get(&key).await?;
        if let Some(s) = raw {
            let job: JobRecord = serde_json::from_str(&s)?;
            Ok(Some(job))
        } else {
            Ok(None)
        }
    }

    pub async fn get_counts(&self, queue_name: &str) -> Result<JobCounts, TurboPulseError> {
        let wait = self.backend.zcard(&self.wait_key(queue_name)).await?;
        let active = self.backend.zcard(&self.active_key(queue_name)).await?;
        let delayed = self.backend.zcard(&self.delayed_key(queue_name)).await?;
        let completed = self.backend.zcard(&self.completed_key(queue_name)).await?;
        let failed = self.backend.zcard(&self.failed_key(queue_name)).await?;

        Ok(JobCounts {
            waiting: wait,
            active,
            delayed,
            completed,
            failed,
        })
    }

    pub async fn pause(&self, queue_name: &str) -> Result<(), TurboPulseError> {
        self.backend
            .hset(&self.meta_key(queue_name), "paused", "1")
            .await
    }

    pub async fn resume(&self, queue_name: &str) -> Result<(), TurboPulseError> {
        self.backend
            .hset(&self.meta_key(queue_name), "paused", "0")
            .await
    }

    pub async fn drain(&self, queue_name: &str) -> Result<(), TurboPulseError> {
        let _ = self.backend.del(&self.wait_key(queue_name)).await;
        let _ = self.backend.del(&self.delayed_key(queue_name)).await;
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

        let ids = self
            .backend
            .zrangebyscore_limit(&target_key, 0, max_score, 0, limit as isize)
            .await
            .unwrap_or_default();

        let count = ids.len() as u32;
        for id in ids {
            let _ = self.backend.zrem(&target_key, &id).await;
            let _ = self.backend.del(&self.job_key(queue_name, &id)).await;
        }

        Ok(count)
    }

    pub async fn promote(&self, queue_name: &str, job_id: &str) -> Result<bool, TurboPulseError> {
        let removed = self.backend.zrem(&self.delayed_key(queue_name), job_id).await?;
        if removed > 0 {
            self.backend.zadd(&self.wait_key(queue_name), job_id, 0).await?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub async fn recover_stalled(&self, queue_name: &str, lock_duration_ms: u64, max_stalled: u32) -> Result<u32, TurboPulseError> {
        let now = current_time_ms();
        let queue_prefix = self.prefix(queue_name);

        let mut inv = self.recover_stalled_script.key(self.active_key(queue_name));
        inv.key(self.wait_key(queue_name))
            .key(self.failed_key(queue_name))
            .arg(&queue_prefix)
            .arg(now.saturating_sub(lock_duration_ms))
            .arg(max_stalled)
            .arg(now);

        let recovered: i64 = self.backend.invoke_script(&inv).await?;
        Ok(recovered.max(0) as u32)
    }

    pub async fn register_parent_children(&self, parent_id: &str, count: u32) -> Result<(), TurboPulseError> {
        let dep_key = format!("{}:deps:{}", self.key_prefix, parent_id);
        let mut inv = self.register_parent_children_script.key(&dep_key);
        inv.arg(count);
        let _: i64 = self.backend.invoke_script(&inv).await?;
        Ok(())
    }

    pub async fn update_data(&self, queue_name: &str, job_id: &str, data: Vec<u8>) -> Result<(), TurboPulseError> {
        let key = self.job_key(queue_name, job_id);
        let raw = self.backend.get(&key).await?;
        if let Some(s) = raw {
            if let Ok(mut job) = serde_json::from_str::<JobRecord>(&s) {
                job.data = data;
                let updated = serde_json::to_string(&job)?;
                self.backend.set(&key, &updated).await?;
                return Ok(());
            }
        }
        Err(TurboPulseError::JobNotFound(job_id.to_string()))
    }
}
