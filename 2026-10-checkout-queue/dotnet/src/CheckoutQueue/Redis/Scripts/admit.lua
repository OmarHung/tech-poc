-- ARGV: prefix, now, maxActive, admissionWindowMs, heartbeatTimeoutMs, maxScan, rateWindowSeconds
-- Every instance runs this on its own tick. Because it is atomic, ten instances ticking
-- at once admit exactly as many users as one would.
local now, maxActive, windowMs = tonumber(ARGV[2]), tonumber(ARGV[3]), tonumber(ARGV[4])
local heartbeatMs, maxScan, rateWindow = tonumber(ARGV[5]), tonumber(ARGV[6]), tonumber(ARGV[7])

-- 1. Admissions past their deadline give their slot back.
local lapsed = redis.call('ZRANGEBYSCORE', key('active'), '-inf', now)
for _, id in ipairs(lapsed) do
  redis.call('HSET', key('ticket', id), 'status', 'expired')
end
if #lapsed > 0 then redis.call('ZREMRANGEBYSCORE', key('active'), '-inf', now) end

-- 2. Fill free slots from the head of the line. Never admit more people than there is
--    stock left: someone admitted to a checkout that cannot succeed is just a worse "sold out".
local free = math.min(maxActive, stock()) - redis.call('ZCARD', key('active'))
local inLine = redis.call('ZCARD', key('waiting'))
local admitted, abandoned, scanned = 0, 0, 0
while admitted < free and scanned < maxScan do
  local head = redis.call('ZPOPMIN', key('waiting'))
  if #head == 0 then break end
  scanned = scanned + 1
  local id = head[1]
  local ticketKey = key('ticket', id)
  if now - tonumber(redis.call('HGET', ticketKey, 'lastSeenAt')) > heartbeatMs then
    redis.call('HSET', ticketKey, 'status', 'abandoned')
    abandoned = abandoned + 1
  else
    local deadline = now + windowMs
    redis.call('HSET', ticketKey, 'status', 'admitted', 'admittedAt', now, 'admissionExpiresAt', deadline)
    redis.call('ZADD', key('active'), deadline, id)
    admitted = admitted + 1
  end
end

-- 3. Admissions per second over the last rateWindow seconds, for wait estimates.
--    Counted from when people first lined up, so the opening seconds are not diluted by
--    a window full of zeros. Any instance's tick refreshes the same shared number.
local second = math.floor(now / 1000)
if admitted > 0 then
  redis.call('INCRBY', key('admits', second), admitted)
  redis.call('EXPIRE', key('admits', second), rateWindow * 2)
end
if inLine > 0 then redis.call('SETNX', key('admitRateSince'), second) end
local since = tonumber(redis.call('GET', key('admitRateSince')) or second)
-- At least 1: an instance whose clock is a few ms behind may still be in the second before `since`.
local span = math.max(1, math.min(rateWindow, second - since + 1))
local buckets = {}
for s = second - span + 1, second do buckets[#buckets + 1] = key('admits', s) end
local total = 0
for _, count in ipairs(redis.call('MGET', unpack(buckets))) do total = total + (tonumber(count) or 0) end
redis.call('SET', key('admitRate'), tostring(total / span))

return reply('ok', {'admitted', admitted, 'abandoned', abandoned, 'expired', #lapsed})
