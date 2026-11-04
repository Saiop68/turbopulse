use redis::Script;
use serde_json;

use crate::error::TurboPulseError;
use crate::queue::job::{JobCounts, JobRecord};
use crate::redis_backend::RedisBackend;

const PUSH_JOB_LUA: &str = r#"
local wait_key = KEYS[1]
local delayed_key = KEYS[2]
local job_key = KEYS[3]
local seq_key = KEYS[4]

local job_id = ARGV[1]
local priority = tonumber(ARGV[2]) or 0
local delay_until = tonumber(ARGV[3]) or 0
local now = tonumber(ARGV[4])
local job_data = ARGV[5]
local is_waiting_children = tonumber(ARGV[6]) or 0
local lifo = tonumber(ARGV[7]) or 0

-- Reject duplicate job ID
if redis.call('EXISTS', job_key) == 1 then
    return 0
end

redis.call('SET', job_key, job_data)

if is_waiting_children == 1 then
    return 1
elseif delay_until > now then
    redis.call('ZADD', delayed_key, delay_until, job_id)
    return 1
else
    local seq = redis.call('INCR', seq_key)
    local prio = math.max(-1000000, math.min(1000000, priority))
    local seq_offset = seq % 10000000
    local score
    if lifo == 1 then
        score = (prio * 10000000) + seq_offset
    else
        score = (prio * 10000000) + (10000000 - seq_offset)
    end
    redis.call('ZADD', wait_key, score, job_id)
    return 1
end
"#;

const BULK_PUSH_JOB_LUA: &str = r#"
local wait_key = KEYS[1]
local delayed_key = KEYS[2]
local prefix = KEYS[3]
local seq_key = KEYS[4]

local now = tonumber(ARGV[1])
local count = tonumber(ARGV[2])

-- Pre-validation: reject if any duplicate exists in Redis or within batch
local seen = {}
for i = 1, count do
    local offset = 2 + (i - 1) * 6
    local job_id = ARGV[offset + 1]
    local job_key = prefix .. ':jobs:' .. job_id
    if seen[job_id] or redis.call('EXISTS', job_key) == 1 then
        return -1
    end
    seen[job_id] = true
end

local seq = redis.call('INCRBY', seq_key, count)
for i = 1, count do
    local offset = 2 + (i - 1) * 6
    local job_id = ARGV[offset + 1]
    local priority = tonumber(ARGV[offset + 2]) or 0
    local delay_until = tonumber(ARGV[offset + 3]) or 0
    local job_data = ARGV[offset + 4]
    local is_waiting_children = tonumber(ARGV[offset + 5]) or 0
    local lifo = tonumber(ARGV[offset + 6]) or 0

    local job_key = prefix .. ':jobs:' .. job_id
    redis.call('SET', job_key, job_data)

    if is_waiting_children == 1 then
        -- WaitingChildren: blocked until children complete
    elseif delay_until > now then
        redis.call('ZADD', delayed_key, delay_until, job_id)
    else
        local job_seq = seq - count + i
        local prio = math.max(-1000000, math.min(1000000, priority))
        local seq_offset = job_seq % 10000000
        local score
        if lifo == 1 then
            score = (prio * 10000000) + seq_offset
        else
            score = (prio * 10000000) + (10000000 - seq_offset)
        end
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
local seq_key = KEYS[5]

local prefix = ARGV[1]
local batch_size = tonumber(ARGV[2]) or 1
local lock_duration = tonumber(ARGV[3]) or 30000

local is_paused = redis.call('HGET', meta_key, 'paused')
if is_paused == '1' then
    return {}
end

local time_res = redis.call('TIME')
local now = tonumber(time_res[1]) * 1000 + math.floor(tonumber(time_res[2]) / 1000)

-- Move matured delayed jobs to wait queue
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
    local seq = redis.call('INCR', seq_key)
    local prio = math.max(-1000000, math.min(1000000, priority))
    local score = (prio * 10000000) + (10000000 - (seq % 10000000))
    redis.call('ZADD', wait_key, score, id)
end

-- Pop up to batch_size waiting jobs
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

local job_id = ARGV[1]
local return_val = ARGV[2]
local lock_token = ARGV[3]

if not lock_token or lock_token == '' then
    return -1 -- LOCK_LOST
end

local raw = redis.call('GET', job_key)
if not raw then
    return -2 -- NOT_FOUND
end

local ok, data = pcall(cjson.decode, raw)
if not ok or type(data) ~= 'table' then
    return -2 -- NOT_FOUND
end

if data['state'] ~= 'Active' then
    return -3 -- INVALID_STATE
end

if not data['lock_token'] or data['lock_token'] ~= lock_token then
    return -1 -- LOCK_LOST
end

local time_res = redis.call('TIME')
local now = tonumber(time_res[1]) * 1000 + math.floor(tonumber(time_res[2]) / 1000)

local active_expire = redis.call('ZSCORE', active_key, job_id)
if not active_expire or tonumber(active_expire) < now then
    return -1 -- LOCK_LOST (lease expired)
end

redis.call('ZREM', active_key, job_id)
redis.call('ZADD', completed_key, now, job_id)

data['state'] = 'Completed'
data['finished_at'] = now
data['progress'] = 100
data['lock_token'] = cjson.null

if return_val and #return_val > 0 then
    local ok_ret, parsed_ret = pcall(cjson.decode, return_val)
    if ok_ret then
        data['return_value'] = parsed_ret
    else
        data['return_value'] = return_val
    end
end

redis.call('SET', job_key, cjson.encode(data))
return 1
"#;

const FAIL_JOB_LUA: &str = r#"
local active_key = KEYS[1]
local delayed_key = KEYS[2]
local failed_key = KEYS[3]
local job_key = KEYS[4]

local job_id = ARGV[1]
local error_msg = ARGV[2]
local lock_token = ARGV[3]

if not lock_token or lock_token == '' then
    return -1 -- LOCK_LOST
end

local raw = redis.call('GET', job_key)
if not raw then
    return -2 -- NOT_FOUND
end

local ok, data = pcall(cjson.decode, raw)
if not ok or type(data) ~= 'table' then
    return -2 -- NOT_FOUND
end

if data['state'] ~= 'Active' then
    return -3 -- INVALID_STATE
end

if not data['lock_token'] or data['lock_token'] ~= lock_token then
    return -1 -- LOCK_LOST
end

local time_res = redis.call('TIME')
local now = tonumber(time_res[1]) * 1000 + math.floor(tonumber(time_res[2]) / 1000)

local active_expire = redis.call('ZSCORE', active_key, job_id)
if not active_expire or tonumber(active_expire) < now then
    return -1 -- LOCK_LOST
end

data['lock_token'] = cjson.null
redis.call('ZREM', active_key, job_id)

local max_retries = 3
local backoff_type = 'Exponential'
local backoff_base = 1000
local backoff_max = 60000
local jitter = true

if data['config'] and type(data['config']) == 'table' then
    local cfg = data['config']
    if cfg['max_retries'] ~= nil then max_retries = tonumber(cfg['max_retries']) or 3 end
    if cfg['backoff_type'] ~= nil then backoff_type = tostring(cfg['backoff_type']) end
    if cfg['backoff_base_ms'] ~= nil then backoff_base = tonumber(cfg['backoff_base_ms']) or 1000 end
    if cfg['backoff_max_ms'] ~= nil then backoff_max = tonumber(cfg['backoff_max_ms']) or 60000 end
    if cfg['jitter'] ~= nil then jitter = (cfg['jitter'] == true or cfg['jitter'] == 1 or cfg['jitter'] == 'true') end
end

local attempts = tonumber(data['attempts_made']) or 1
if attempts < max_retries then
    local delay = backoff_base
    if backoff_type == 'Fixed' then
        delay = backoff_base
    elseif backoff_type == 'Linear' then
        delay = backoff_base * attempts
    else
        delay = backoff_base * (2 ^ (attempts - 1))
    end
    if delay > backoff_max then delay = backoff_max end

    if jitter then
        local rnd = (tonumber(time_res[2]) % 1000) / 1000.0
        local jitter_amount = math.floor(delay * 0.2 * rnd)
        delay = delay + jitter_amount
    end

    data['state'] = 'Delayed'
    local delay_until = now + math.floor(delay)
    data['delay_until_ms'] = delay_until
    data['failed_reason'] = 'Retry ' .. tostring(attempts) .. '/' .. tostring(max_retries) .. ': ' .. error_msg
    redis.call('ZADD', delayed_key, delay_until, job_id)
    redis.call('SET', job_key, cjson.encode(data))
    return 1 -- SCHEDULED_RETRY
else
    data['state'] = 'Failed'
    data['finished_at'] = now
    data['failed_reason'] = error_msg
    redis.call('ZADD', failed_key, now, job_id)
    redis.call('SET', job_key, cjson.encode(data))
    return 2 -- PERMANENTLY_FAILED
end
"#;

const RENEW_LOCK_LUA: &str = r#"
local active_key = KEYS[1]
local job_key = KEYS[2]

local job_id = ARGV[1]
local lock_token = ARGV[2]
local lock_duration = tonumber(ARGV[3]) or 30000

if not lock_token or lock_token == '' then
    return -1
end

local raw = redis.call('GET', job_key)
if not raw then
    return 0
end

local ok, data = pcall(cjson.decode, raw)
if not ok or type(data) ~= 'table' then
    return 0
end

if data['state'] ~= 'Active' then
    return -1
end

if not data['lock_token'] or data['lock_token'] ~= lock_token then
    return -1
end

local time_res = redis.call('TIME')
local now = tonumber(time_res[1]) * 1000 + math.floor(tonumber(time_res[2]) / 1000)

local active_expire = redis.call('ZSCORE', active_key, job_id)
if not active_expire or tonumber(active_expire) < now then
    return -1 -- Lease expired
end

redis.call('ZADD', active_key, now + lock_duration, job_id)
return 1
"#;

const RECOVER_STALLED_LUA: &str = r#"
local active_key = KEYS[1]
local wait_key = KEYS[2]
local failed_key = KEYS[3]
local seq_key = KEYS[4]

local prefix = ARGV[1]
local max_stalled = tonumber(ARGV[2]) or 3
local lock_duration = tonumber(ARGV[3]) or 0

local time_res = redis.call('TIME')
local now = tonumber(time_res[1]) * 1000 + math.floor(tonumber(time_res[2]) / 1000)

local stalled_ids = redis.call('ZRANGEBYSCORE', active_key, '-inf', now, 'LIMIT', 0, 100)
if #stalled_ids == 0 and lock_duration > 0 and lock_duration < 30000 then
    local cutoff = now + 30000 - lock_duration
    stalled_ids = redis.call('ZRANGEBYSCORE', active_key, '-inf', cutoff, 'LIMIT', 0, 100)
end
local recovered = 0

for _, id in ipairs(stalled_ids) do
    redis.call('ZREM', active_key, id)
    local job_key = prefix .. ':jobs:' .. id
    local raw = redis.call('GET', job_key)
    if raw then
        local ok, data = pcall(cjson.decode, raw)
        if ok and type(data) == 'table' then
            if data['state'] == 'Active' then
                local stalled_count = (tonumber(data['stalled_count']) or 0) + 1
                data['stalled_count'] = stalled_count
                data['lock_token'] = cjson.null
                if stalled_count > max_stalled then
                    data['state'] = 'Failed'
                    data['finished_at'] = now
                    data['failed_reason'] = 'Job stalled more than maxStalledCount times'
                    redis.call('ZADD', failed_key, now, id)
                else
                    data['state'] = 'Waiting'
                    local priority = 0
                    if data['config'] and data['config']['priority'] then
                        priority = tonumber(data['config']['priority']) or 0
                    end
                    local seq = redis.call('INCR', seq_key)
                    local prio = math.max(-1000000, math.min(1000000, priority))
                    local score = (prio * 10000000) + (10000000 - (seq % 10000000))
                    redis.call('ZADD', wait_key, score, id)
                end
                redis.call('SET', job_key, cjson.encode(data))
                recovered = recovered + 1
            end
        end
    end
end

return recovered
"#;

const REGISTER_PARENT_CHILDREN_LUA: &str = r#"
local dep_key = KEYS[1]
for i = 1, #ARGV do
    redis.call('SADD', dep_key, ARGV[i])
end
redis.call('EXPIRE', dep_key, 2592000) -- 30-day safety TTL
return 1
"#;

const RESOLVE_PARENT_DEP_LUA: &str = r#"
local deps_key = KEYS[1]
local wait_key = KEYS[2]
local job_key = KEYS[3]
local seq_key = KEYS[4]

local parent_id = ARGV[1]
local child_id = ARGV[2]

local removed = redis.call('SREM', deps_key, child_id)
local remaining = redis.call('SCARD', deps_key)

if remaining == 0 then
    redis.call('DEL', deps_key)
    local raw = redis.call('GET', job_key)
    if raw then
        local ok, data = pcall(cjson.decode, raw)
        if ok and type(data) == 'table' then
            if data['state'] == 'WaitingChildren' then
                data['state'] = 'Waiting'
                local priority = 0
                if data['config'] and data['config']['priority'] then
                    priority = tonumber(data['config']['priority']) or 0
                end
                local seq = redis.call('INCR', seq_key)
                local prio = math.max(-1000000, math.min(1000000, priority))
                local score = (prio * 10000000) + (10000000 - (seq % 10000000))
                redis.call('SET', job_key, cjson.encode(data))
                redis.call('ZADD', wait_key, score, parent_id)
                return 1
            end
        end
    end
end
return 0
"#;

const DRAIN_QUEUE_LUA: &str = r#"
local wait_key = KEYS[1]
local delayed_key = KEYS[2]
local prefix = ARGV[1]

local wait_ids = redis.call('ZRANGE', wait_key, 0, -1)
for _, id in ipairs(wait_ids) do
    redis.call('DEL', prefix .. ':jobs:' .. id)
end
redis.call('DEL', wait_key)

local delayed_ids = redis.call('ZRANGE', delayed_key, 0, -1)
for _, id in ipairs(delayed_ids) do
    redis.call('DEL', prefix .. ':jobs:' .. id)
end
redis.call('DEL', delayed_key)

return #wait_ids + #delayed_ids
"#;

const OBLITERATE_QUEUE_LUA: &str = r#"
local wait_key = KEYS[1]
local delayed_key = KEYS[2]
local active_key = KEYS[3]
local completed_key = KEYS[4]
local failed_key = KEYS[5]
local meta_key = KEYS[6]
local seq_key = KEYS[7]

local prefix = ARGV[1]
local force = tonumber(ARGV[2]) or 0

local active_count = redis.call('ZCARD', active_key)
if active_count > 0 and force ~= 1 then
    return -1
end

local sets = {wait_key, delayed_key, active_key, completed_key, failed_key}
local count = 0
for _, set_key in ipairs(sets) do
    local ids = redis.call('ZRANGE', set_key, 0, -1)
    for _, id in ipairs(ids) do
        redis.call('DEL', prefix .. ':jobs:' .. id)
        count = count + 1
    end
    redis.call('DEL', set_key)
end

redis.call('DEL', meta_key)
redis.call('DEL', seq_key)

return count
"#;

const CLEAN_QUEUE_LUA: &str = r#"
local target_key = KEYS[1]
local prefix = ARGV[1]
local grace_ms = tonumber(ARGV[2]) or 0
local limit = tonumber(ARGV[3]) or 1000

local time_res = redis.call('TIME')
local now = tonumber(time_res[1]) * 1000 + math.floor(tonumber(time_res[2]) / 1000)
local max_score = now - grace_ms

local ids = redis.call('ZRANGEBYSCORE', target_key, 0, max_score, 'LIMIT', 0, limit)
for _, id in ipairs(ids) do
    redis.call('ZREM', target_key, id)
    redis.call('DEL', prefix .. ':jobs:' .. id)
end

return #ids
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
    resolve_parent_dep_script: Script,
    drain_script: Script,
    obliterate_script: Script,
    clean_script: Script,
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
            resolve_parent_dep_script: Script::new(RESOLVE_PARENT_DEP_LUA),
            drain_script: Script::new(DRAIN_QUEUE_LUA),
            obliterate_script: Script::new(OBLITERATE_QUEUE_LUA),
            clean_script: Script::new(CLEAN_QUEUE_LUA),
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

    #[inline]
    fn seq_key(&self, q: &str) -> String {
        format!("{}:seq", self.prefix(q))
    }

    #[inline]
    fn deps_key(&self, q: &str, parent_id: &str) -> String {
        format!("{}:deps:{}", self.prefix(q), parent_id)
    }

    pub async fn push_job(&self, job: JobRecord) -> Result<(), TurboPulseError> {
        let q = &job.queue_name;
        let job_id = &job.id;
        let priority = job.config.priority.unwrap_or(0);
        let now = crate::utils::time::current_time_ms();
        let payload = serde_json::to_string(&job)?;
        let is_waiting_children = if job.config.is_waiting_children.unwrap_or(false) { 1 } else { 0 };
        let lifo = if job.config.lifo.unwrap_or(false) { 1 } else { 0 };

        let mut inv = self.push_script.key(self.wait_key(q));
        inv.key(self.delayed_key(q))
            .key(self.job_key(q, job_id))
            .key(self.seq_key(q))
            .arg(job_id)
            .arg(priority)
            .arg(job.delay_until_ms)
            .arg(now)
            .arg(payload)
            .arg(is_waiting_children)
            .arg(lifo);

        let res: i64 = self.backend.invoke_script(&inv).await?;
        if res == 0 {
            return Err(TurboPulseError::InvalidJobState(format!("Job with ID {} already exists", job_id)));
        }

        Ok(())
    }

    pub async fn push_jobs_bulk(&self, jobs: Vec<JobRecord>) -> Result<Vec<String>, TurboPulseError> {
        if jobs.is_empty() {
            return Ok(vec![]);
        }

        let now = crate::utils::time::current_time_ms();
        let q = &jobs[0].queue_name;
        let queue_prefix = self.prefix(q);
        let count = jobs.len();

        let mut inv = self.bulk_push_script.key(self.wait_key(q));
        inv.key(self.delayed_key(q))
            .key(&queue_prefix)
            .key(self.seq_key(q))
            .arg(now)
            .arg(count as i64);

        let mut ids = Vec::with_capacity(count);
        for j in &jobs {
            ids.push(j.id.clone());
            let priority = j.config.priority.unwrap_or(0);
            let payload = serde_json::to_string(j)?;
            let is_waiting_children = if j.config.is_waiting_children.unwrap_or(false) { 1 } else { 0 };
            let lifo = if j.config.lifo.unwrap_or(false) { 1 } else { 0 };

            inv.arg(&j.id)
                .arg(priority)
                .arg(j.delay_until_ms)
                .arg(payload)
                .arg(is_waiting_children)
                .arg(lifo);
        }

        let res: i64 = self.backend.invoke_script(&inv).await?;
        if res == -1 {
            return Err(TurboPulseError::InvalidJobState("Bulk insert failed: duplicate job ID detected".to_string()));
        }

        Ok(ids)
    }

    pub async fn fetch_next_job(&self, queue_name: &str, lock_duration_ms: Option<u64>) -> Result<Option<JobRecord>, TurboPulseError> {
        let batch = self.fetch_next_batch(queue_name, 1, lock_duration_ms).await?;
        Ok(batch.into_iter().next())
    }

    pub async fn fetch_next_batch(&self, queue_name: &str, count: usize, lock_duration_ms: Option<u64>) -> Result<Vec<JobRecord>, TurboPulseError> {
        let queue_prefix = self.prefix(queue_name);
        let duration = lock_duration_ms.unwrap_or(30000) as i64;

        let mut inv = self.fetch_batch_script.key(self.delayed_key(queue_name));
        inv.key(self.wait_key(queue_name))
            .key(self.active_key(queue_name))
            .key(self.meta_key(queue_name))
            .key(self.seq_key(queue_name))
            .arg(&queue_prefix)
            .arg(count as i64)
            .arg(duration);

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
        lock_token: &str,
    ) -> Result<bool, TurboPulseError> {
        if lock_token.is_empty() {
            return Err(TurboPulseError::InvalidJobState("LOCK_LOST: Missing lock token".to_string()));
        }
        let key = self.job_key(queue_name, job_id);

        let ret_str = return_value
            .map(|v| String::from_utf8_lossy(&v).to_string())
            .unwrap_or_default();

        let mut inv = self.complete_script.key(self.active_key(queue_name));
        inv.key(self.completed_key(queue_name))
            .key(&key)
            .arg(job_id)
            .arg(ret_str)
            .arg(lock_token);

        let res: i64 = self.backend.invoke_script(&inv).await?;
        if res == -1 {
            return Err(TurboPulseError::InvalidJobState("LOCK_LOST: Job lock was lost, expired, or token mismatch".to_string()));
        }
        if res == -2 {
            return Err(TurboPulseError::InvalidJobState("Job not found".to_string()));
        }
        if res == -3 {
            return Err(TurboPulseError::InvalidJobState("LOCK_LOST: Job is not in Active state".to_string()));
        }

        Ok(res > 0)
    }

    pub async fn fail_job(
        &self,
        queue_name: &str,
        job_id: &str,
        error_msg: &str,
        lock_token: &str,
    ) -> Result<bool, TurboPulseError> {
        if lock_token.is_empty() {
            return Err(TurboPulseError::InvalidJobState("LOCK_LOST: Missing lock token".to_string()));
        }
        let key = self.job_key(queue_name, job_id);

        let mut inv = self.fail_script.key(self.active_key(queue_name));
        inv.key(self.delayed_key(queue_name))
            .key(self.failed_key(queue_name))
            .key(&key)
            .arg(job_id)
            .arg(error_msg)
            .arg(lock_token);

        let res: i64 = self.backend.invoke_script(&inv).await?;
        if res == -1 {
            return Err(TurboPulseError::InvalidJobState("LOCK_LOST: Job lock was lost, expired, or token mismatch".to_string()));
        }
        if res == -2 {
            return Err(TurboPulseError::InvalidJobState("Job not found".to_string()));
        }
        if res == -3 {
            return Err(TurboPulseError::InvalidJobState("LOCK_LOST: Job is not in Active state".to_string()));
        }

        Ok(res == 2)
    }

    pub async fn renew_lock(
        &self,
        queue_name: &str,
        job_id: &str,
        lock_token: &str,
        lock_duration_ms: u32,
    ) -> Result<bool, TurboPulseError> {
        if lock_token.is_empty() {
            return Ok(false);
        }
        let key = self.job_key(queue_name, job_id);
        let mut inv = self.renew_lock_script.key(self.active_key(queue_name));
        inv.key(&key)
            .arg(job_id)
            .arg(lock_token)
            .arg(lock_duration_ms as i64);

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
                job.progress = progress;
                let updated = serde_json::to_string(&job)?;
                self.backend.set(&key, &updated).await?;
            }
        }
        Ok(())
    }

    pub async fn get_job(&self, queue_name: &str, job_id: &str) -> Result<Option<JobRecord>, TurboPulseError> {
        let key = self.job_key(queue_name, job_id);
        let raw = self.backend.get(&key).await?;
        if let Some(s) = raw {
            let job = serde_json::from_str::<JobRecord>(&s)?;
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
        let prefix = self.prefix(queue_name);
        let mut inv = self.drain_script.key(self.wait_key(queue_name));
        inv.key(self.delayed_key(queue_name))
            .arg(&prefix);
        let _: i64 = self.backend.invoke_script(&inv).await?;
        Ok(())
    }

    pub async fn obliterate(&self, queue_name: &str, force: bool) -> Result<u32, TurboPulseError> {
        let prefix = self.prefix(queue_name);
        let mut inv = self.obliterate_script.key(self.wait_key(queue_name));
        inv.key(self.delayed_key(queue_name))
            .key(self.active_key(queue_name))
            .key(self.completed_key(queue_name))
            .key(self.failed_key(queue_name))
            .key(self.meta_key(queue_name))
            .key(self.seq_key(queue_name))
            .arg(&prefix)
            .arg(if force { 1 } else { 0 });

        let res: i64 = self.backend.invoke_script(&inv).await?;
        if res == -1 {
            return Err(TurboPulseError::InvalidJobState("Cannot obliterate queue with active jobs without force=true".to_string()));
        }
        Ok(res.max(0) as u32)
    }

    pub async fn clean(&self, queue_name: &str, grace_ms: u64, limit: u32, state_type: &str) -> Result<u32, TurboPulseError> {
        let target_key = match state_type {
            "completed" => self.completed_key(queue_name),
            "failed" => self.failed_key(queue_name),
            _ => return Ok(0),
        };
        let prefix = self.prefix(queue_name);
        let mut inv = self.clean_script.key(&target_key);
        inv.arg(&prefix)
            .arg(grace_ms as i64)
            .arg(limit as i64);

        let res: i64 = self.backend.invoke_script(&inv).await?;
        Ok(res.max(0) as u32)
    }

    pub async fn promote(&self, queue_name: &str, job_id: &str) -> Result<bool, TurboPulseError> {
        let removed = self.backend.zrem(&self.delayed_key(queue_name), job_id).await?;
        if removed > 0 {
            let key = self.job_key(queue_name, job_id);
            let raw = self.backend.get(&key).await?;
            let mut priority = 0;
            if let Some(s) = raw {
                if let Ok(mut job) = serde_json::from_str::<JobRecord>(&s) {
                    priority = job.config.priority.unwrap_or(0);
                    job.state = crate::queue::job::JobState::Waiting;
                    let _ = self.backend.set(&key, &serde_json::to_string(&job)?).await;
                }
            }
            let seq = self.backend.incr(&self.seq_key(queue_name)).await?;
            let prio = priority.clamp(-1000000, 1000000);
            let score = (prio as i64 * 10000000) + (10000000 - (seq % 10000000));
            self.backend.zadd(&self.wait_key(queue_name), job_id, score as i64).await?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub async fn recover_stalled(&self, queue_name: &str, lock_duration_ms: u64, max_stalled: u32) -> Result<u32, TurboPulseError> {
        let queue_prefix = self.prefix(queue_name);

        let mut inv = self.recover_stalled_script.key(self.active_key(queue_name));
        inv.key(self.wait_key(queue_name))
            .key(self.failed_key(queue_name))
            .key(self.seq_key(queue_name))
            .arg(&queue_prefix)
            .arg(max_stalled as i64)
            .arg(lock_duration_ms as i64);

        let recovered: i64 = self.backend.invoke_script(&inv).await?;
        Ok(recovered.max(0) as u32)
    }

    pub async fn register_parent_children(&self, parent_queue: &str, parent_id: &str, child_ids: &[String]) -> Result<(), TurboPulseError> {
        if child_ids.is_empty() {
            return Ok(());
        }
        let dep_key = self.deps_key(parent_queue, parent_id);
        let mut inv = self.register_parent_children_script.key(&dep_key);
        for id in child_ids {
            inv.arg(id);
        }
        let _: i64 = self.backend.invoke_script(&inv).await?;
        Ok(())
    }

    pub async fn resolve_parent_dep(&self, parent_queue: &str, parent_id: &str, child_id: &str) -> Result<bool, TurboPulseError> {
        let dep_key = self.deps_key(parent_queue, parent_id);
        let mut inv = self.resolve_parent_dep_script.key(&dep_key);
        inv.key(self.wait_key(parent_queue))
            .key(self.job_key(parent_queue, parent_id))
            .key(self.seq_key(parent_queue))
            .arg(parent_id)
            .arg(child_id);

        let res: i64 = self.backend.invoke_script(&inv).await?;
        Ok(res == 1)
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
        Err(TurboPulseError::InvalidJobState("Job not found".to_string()))
    }
}
