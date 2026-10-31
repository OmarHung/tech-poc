-- ARGV: prefix, userId, newTicketId, now
local userId, newId, now = ARGV[2], ARGV[3], tonumber(ARGV[4])

-- A ticket is over when its admission lapsed, it was skipped for not polling,
-- or its order went unpaid. Then the user may line up again, at the back.
local function ended(id)
  local t = hash(key('ticket', id))
  local status = ticket_status(t, now)
  if status == 'expired' or status == 'abandoned' then return true end
  return status == 'ordered' and order_status(hash(key('order', t.orderId)), now) == 'expired'
end

-- One live ticket per user: a refresh, a second tab or a retry gets the same place in line.
local current = redis.call('GET', key('user', userId))
if current and not ended(current) then
  if redis.call('HGET', key('ticket', current), 'status') == 'waiting' then
    redis.call('HSET', key('ticket', current), 'lastSeenAt', now)
  end
  return reply('existing', ticket_view(current, now))
end

if sold_out() then return reply('sold_out') end

local seq = redis.call('INCR', key('seq'))
redis.call('HSET', key('ticket', newId), 'userId', userId, 'seq', seq, 'status', 'waiting', 'joinedAt', now, 'lastSeenAt', now)
redis.call('ZADD', key('waiting'), seq, newId)
redis.call('ZADD', key('tickets'), seq, newId)
redis.call('SET', key('user', userId), newId)
return reply('created', ticket_view(newId, now))
