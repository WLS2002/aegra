"""Replay limits are independent of durable jobs and checkpoints."""

import json
from typing import Any

from fastapi import HTTPException

from aegra_api.core.serializers import GeneralSerializer

_serializer = GeneralSerializer()


def replay_size(event_id: str, payload: Any) -> int:
    return len(json.dumps({"event_id": event_id, "payload": payload}, default=_serializer.serialize).encode()) + 128


def replay_unavailable() -> HTTPException:
    return HTTPException(409, "replay_unavailable: reload the run and thread state")


# Metadata and payload changes share one Redis transaction; only replay keys are evicted.
CACHE_EVENT_LUA = r"""
local cache, sizes, expires, totalkey = KEYS[1], KEYS[2], KEYS[3], KEYS[4]
local payload, ttl, runmax, totalmax, countmax = ARGV[1], tonumber(ARGV[2]), tonumber(ARGV[3]), tonumber(ARGV[4]), tonumber(ARGV[5])
local now = tonumber(redis.call('TIME')[1])
local total = tonumber(redis.call('GET', totalkey) or '0')
local function drop(key)
    local size = tonumber(redis.call('HGET', sizes, key) or '0')
    total = math.max(0, total - size)
    redis.call('DEL', key)
    redis.call('HDEL', sizes, key)
    redis.call('ZREM', expires, key)
end
for _, key in ipairs(redis.call('ZRANGEBYSCORE', expires, '-inf', now, 'LIMIT', 0, 256)) do drop(key) end
local current = tonumber(redis.call('HGET', sizes, cache) or '0')
if redis.call('EXISTS', cache) == 0 then
    total = math.max(0, total - current)
    current = 0
    redis.call('HDEL', sizes, cache)
    redis.call('ZREM', expires, cache)
end
local cost = string.len(payload) + 128
if cost > math.min(runmax, totalmax) then
    drop(cache)
else
    local previous = redis.call('LINDEX', cache, -1)
    if previous ~= payload then
        redis.call('RPUSH', cache, payload)
        current = current + cost
        total = total + cost
    end
    while current > runmax or redis.call('LLEN', cache) > countmax do
        local removed = redis.call('LPOP', cache)
        if not removed then break end
        local removedsize = string.len(removed) + 128
        current = current - removedsize
        total = total - removedsize
    end
    redis.call('HSET', sizes, cache, current)
    redis.call('ZADD', expires, now + ttl, cache)
    redis.call('EXPIRE', cache, ttl)
    while total > totalmax do
        local oldest = redis.call('ZRANGE', expires, 0, 0)[1]
        if not oldest then break end
        drop(oldest)
    end
end
redis.call('SET', totalkey, total, 'EX', ttl * 2)
redis.call('EXPIRE', sizes, ttl * 2)
redis.call('EXPIRE', expires, ttl * 2)
return total
"""
