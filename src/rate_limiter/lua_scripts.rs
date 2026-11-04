/// Redis Lua Script for atomic dual-constraint rate limiting:
/// Checks Token Bucket (capacity, refill rate, interval) + Concurrency Limit + MinTime spacing.
pub const ACQUIRE_TOKEN_LUA: &str = r#"
local key = KEYS[1]

local requested = tonumber(ARGV[1])
local capacity = tonumber(ARGV[2])
local refill_rate = tonumber(ARGV[3])
local refill_interval = tonumber(ARGV[4])
local max_concurrent = tonumber(ARGV[5])
local min_time = tonumber(ARGV[6])
local cool_down = tonumber(ARGV[7])
local now = tonumber(ARGV[8])

local data = redis.call('HMGET', key, 'tokens', 'last_refill', 'last_exec', 'active')
local tokens = tonumber(data[1])
local last_refill = tonumber(data[2])
local last_exec = tonumber(data[3]) or 0
local active = tonumber(data[4]) or 0

if tokens == nil then
    tokens = capacity
    last_refill = now
    last_exec = 0
    active = 0
else
    local elapsed = now - last_refill
    if elapsed > 0 and refill_interval > 0 then
        local refilled = (elapsed / refill_interval) * refill_rate
        if refilled > 0 then
            tokens = math.min(capacity, tokens + refilled)
            last_refill = now
        end
    end
end

-- 1. Concurrency limit check
if max_concurrent > 0 and active >= max_concurrent then
    -- Concurrent limit reached, suggest small backoff (e.g. 25ms)
    return {0, math.floor(tokens), 25, active, "CONCURRENCY_LIMIT"}
end

-- 2. MinTime spacing check
if min_time > 0 and (now - last_exec) < min_time then
    local wait_time = min_time - (now - last_exec)
    return {0, math.floor(tokens), wait_time, active, "MIN_TIME_LIMIT"}
end

-- 3. Token check
if tokens < requested then
    local missing = requested - tokens
    local wait_time = math.ceil((missing / refill_rate) * refill_interval) + cool_down
    return {0, math.floor(tokens), wait_time, active, "RATE_LIMIT"}
end

-- All checks passed: deduct tokens and increment active counter
tokens = tokens - requested
active = active + 1
last_exec = now

redis.call('HMSET', key,
    'tokens', tokens,
    'last_refill', last_refill,
    'last_exec', last_exec,
    'active', active
)

-- Set TTL to 2 * refill_interval or at least 60 seconds to prevent key leakage
local ttl_seconds = math.max(60, math.ceil((refill_interval * 2) / 1000))
redis.call('EXPIRE', key, ttl_seconds)

return {1, math.floor(tokens), 0, active, "OK"}
"#;

/// Redis Lua Script to atomically decrement the concurrency active counter
pub const RELEASE_CONCURRENCY_LUA: &str = r#"
local key = KEYS[1]
local active = tonumber(redis.call('HGET', key, 'active') or 0)
if active > 0 then
    active = active - 1
    redis.call('HSET', key, 'active', active)
end
local ttl_seconds = tonumber(ARGV[1]) or 60
redis.call('EXPIRE', key, ttl_seconds)
return active
"#;

/// Redis Lua Script to atomically refund tokens and decrement concurrency
pub const REFUND_PERMIT_LUA: &str = r#"
local key = KEYS[1]
local tokens_to_refund = tonumber(ARGV[1]) or 1
local ttl_seconds = tonumber(ARGV[2]) or 60

local active = tonumber(redis.call('HGET', key, 'active') or 0)
if active > 0 then
    active = active - 1
    redis.call('HSET', key, 'active', active)
end

local tokens = tonumber(redis.call('HGET', key, 'tokens') or 0)
redis.call('HSET', key, 'tokens', tokens + tokens_to_refund)
redis.call('EXPIRE', key, ttl_seconds)
return active
"#;

/// Redis Lua Script to inspect status without consuming tokens
pub const STATUS_LUA: &str = r#"
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local refill_rate = tonumber(ARGV[2])
local refill_interval = tonumber(ARGV[3])
local now = tonumber(ARGV[4])

local data = redis.call('HMGET', key, 'tokens', 'last_refill', 'active')
local tokens = tonumber(data[1])
local last_refill = tonumber(data[2])
local active = tonumber(data[3]) or 0

if tokens == nil then
    return {capacity, 0, 0}
end

local elapsed = now - last_refill
if elapsed > 0 then
    local intervals = math.floor(elapsed / refill_interval)
    if intervals > 0 then
        local refilled = intervals * refill_rate
        tokens = math.min(capacity, tokens + refilled)
    end
end

return {math.floor(tokens), active, 0}
"#;
