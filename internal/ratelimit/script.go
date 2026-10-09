package ratelimit

import "serverflow/internal/redis"

// The scripts are fixed text with all inputs passed as KEYS and ARGV, so there is no string-built Lua and
// nothing a client controls can become code. They use Redis TIME unless ARGV[1] (acquire, renew) is a positive
// millisecond timestamp, which only a test clock can supply (Config.Clock is not settable from the
// configuration file).
//
// All arithmetic is on integers held in doubles and stays below 2^53 given the bounds in limiter.go
// (quota <= MaxQuota, burst <= MaxBurstSeconds, cost <= MaxCost). Numbers are written to Redis with
// '%.0f' because Lua's default number-to-string conversion keeps only 14 significant digits.
// model.go is the same algorithm in Go; the tests require them to agree.

// acquireScript checks every active quota and, only if all admit the request, charges all of them and
// takes a concurrency lease.
//
//	KEYS: 1 request bucket, 2 token bucket, 3 concurrency set, 4 model bucket (a key is touched only when its quota is > 0)
//	ARGV: 1 now override ms (0 = TIME), 2 requests/min, 3 tokens/min, 4 max concurrent, 5 model requests/min,
//	      6 cost in tokens, 7 burst seconds, 8 lease id, 9 lease ttl ms
//	Reply: {allowed (1/0), limit (1 requests, 2 tokens, 3 concurrency, 4 model; 0 when allowed), retry ms}
var acquireScript = redis.NewScript(`
local override = tonumber(ARGV[1])
local now
if override > 0 then
  now = override
else
  local t = redis.call('TIME')
  now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
end
local rq, tq, mc, mq = tonumber(ARGV[2]), tonumber(ARGV[3]), tonumber(ARGV[4]), tonumber(ARGV[5])
local cost = tonumber(ARGV[6])
local window = tonumber(ARGV[7]) * 1000
local lease_id = ARGV[8]
local lease_ttl = tonumber(ARGV[9])
local SCALE = 60000

local function ceil_div(a, b)
  local x = a + b - 1
  return (x - (x % b)) / b
end

-- Reads a bucket (refilled to now, clamped to the current capacity) and returns its level, what the
-- request needs, and how many ms until it could be admitted (0 = now).
local function check(key, quota, tokens)
  local cap = quota * window
  local level = cap
  local h = redis.call('HMGET', key, 'l', 't')
  local l, ts = tonumber(h[1]), tonumber(h[2])
  if l and ts then
    local elapsed = now - ts
    if elapsed < 0 then elapsed = 0 end
    if elapsed > window then elapsed = window end
    level = l + elapsed * quota
    if level > cap then level = cap end
  end
  local need = tokens * SCALE
  if need > cap then need = cap end
  local wait = 0
  if level < need then wait = ceil_div(need - level, quota) end
  return level, need, wait
end

local best_code, best_wait = 0, 0
local function consider(code, wait)
  if wait > best_wait then best_code, best_wait = code, wait end
end

local req_level, req_need, tok_level, tok_need, mod_level, mod_need, w
if rq > 0 then
  req_level, req_need, w = check(KEYS[1], rq, 1)
  if w > 0 then consider(1, w) end
end
if tq > 0 then
  tok_level, tok_need, w = check(KEYS[2], tq, cost)
  if w > 0 then consider(2, w) end
end
if mc > 0 then
  redis.call('ZREMRANGEBYSCORE', KEYS[3], '-inf', now)
  if redis.call('ZCARD', KEYS[3]) >= mc then consider(3, 1000) end
end
if mq > 0 then
  mod_level, mod_need, w = check(KEYS[4], mq, 1)
  if w > 0 then consider(4, w) end
end
if best_code > 0 then
  return {0, best_code, best_wait}
end

local function store(key, level, need)
  redis.call('HSET', key, 'l', string.format('%.0f', level - need), 't', string.format('%.0f', now))
  redis.call('PEXPIRE', key, window * 2)
end
if rq > 0 then store(KEYS[1], req_level, req_need) end
if tq > 0 then store(KEYS[2], tok_level, tok_need) end
if mq > 0 then store(KEYS[4], mod_level, mod_need) end
if mc > 0 then
  redis.call('ZADD', KEYS[3], string.format('%.0f', now + lease_ttl), lease_id)
  redis.call('PEXPIRE', KEYS[3], lease_ttl * 2)
end
return {1, 0, 0}
`, 3)

// renewScript extends leases that are still held. A lease that expired or was released is not revived (so a
// renewal that races a release cannot resurrect the slot).
//
//	KEYS: 1 concurrency set
//	ARGV: 1 now override ms (0 = TIME), 2 lease ttl ms, 3.. lease ids
//	Reply: {leases renewed}
var renewScript = redis.NewScript(`
local override = tonumber(ARGV[1])
local now
if override > 0 then
  now = override
else
  local t = redis.call('TIME')
  now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
end
local ttl = tonumber(ARGV[2])
local renewed = 0
for i = 3, #ARGV do
  local score = redis.call('ZSCORE', KEYS[1], ARGV[i])
  if score and tonumber(score) > now then
    redis.call('ZADD', KEYS[1], 'XX', string.format('%.0f', now + ttl), ARGV[i])
    renewed = renewed + 1
  end
end
if renewed > 0 then redis.call('PEXPIRE', KEYS[1], ttl * 2) end
return {renewed}
`, 1)

// releaseScript removes one lease. Reply: {1 if it was held, else 0}.
var releaseScript = redis.NewScript(`return {redis.call('ZREM', KEYS[1], ARGV[1])}`, 1)
