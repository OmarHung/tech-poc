-- ARGV: prefix, now, batchSize
-- Releases stock held by orders whose payment window has closed.
local now, batch = tonumber(ARGV[2]), tonumber(ARGV[3])
local released = 0
for _, id in ipairs(redis.call('ZRANGEBYSCORE', key('pending'), '-inf', now, 'LIMIT', 0, batch)) do
  local o = redis.call('HMGET', key('order', id), 'status', 'quantity')
  if o[1] == 'pending_payment' then
    expire_order(id, tonumber(o[2]))
    released = released + 1
  end
end
return reply('ok', {'released', released})
