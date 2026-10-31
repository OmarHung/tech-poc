-- ARGV: prefix, ticketId, quantity, newOrderId, now, paymentWindowMs
local ticketId, qty, newId = ARGV[2], tonumber(ARGV[3]), ARGV[4]
local now, paymentWindowMs = tonumber(ARGV[5]), tonumber(ARGV[6])
local ticketKey = key('ticket', ticketId)
local t = hash(ticketKey)
if not t.status then return reply('ticket_not_found') end

-- The ticket is the idempotency key: a double click or a client retry replays the
-- order this ticket already placed instead of reserving stock a second time.
if t.orderId then
  local placed = tonumber(redis.call('HGET', key('order', t.orderId), 'quantity'))
  if placed ~= qty then return reply('ticket_used', {'detail', t.orderId}) end
  return reply('existing', order_view(t.orderId, now))
end

local status = ticket_status(t, now)
if status == 'expired' then
  if t.status == 'admitted' then
    redis.call('HSET', ticketKey, 'status', 'expired')
    redis.call('ZREM', key('active'), ticketId)
  end
  return reply('admission_expired')
end
if t.status ~= 'admitted' then return reply('not_admitted', {'detail', status}) end

-- "sold_out" is final, the same as everywhere else. While unpaid orders still hold stock
-- it may come back, so the admitted user hears "insufficient" (possibly 0) and can retry.
local left = stock()
if left < qty then
  if sold_out() then return reply('sold_out') end
  return reply('insufficient_stock', {'detail', left})
end

local deadline = now + paymentWindowMs
redis.call('DECRBY', key('stock'), qty)
redis.call('HSET', key('order', newId), 'ticketId', ticketId, 'userId', t.userId, 'quantity', qty,
           'status', 'pending_payment', 'createdAt', now, 'paymentExpiresAt', deadline)
redis.call('ZADD', key('pending'), deadline, newId)
redis.call('SADD', key('orders'), newId)
redis.call('HSET', ticketKey, 'status', 'ordered', 'orderId', newId)
redis.call('ZREM', key('active'), ticketId)
return reply('created', order_view(newId, now))
