-- Shared helpers, prepended to every script.
--
-- ARGV[1] is always the sale's key prefix, e.g. "cq:{demo}:". The {demo} hash tag puts every
-- key of one sale on the same Redis Cluster slot, so building keys inside a script stays legal.
-- Every script runs atomically: nothing else touches the sale between its reads and writes.
-- That is the whole concurrency story; no locks, no retries, no leader election.
--
-- Times are epoch milliseconds passed in by the caller.
--
-- Keys:
--   stock, initialStock, seq            counters
--   user:<userId>        -> ticketId    one live ticket per user
--   ticket:<id>          hash           userId seq status joinedAt lastSeenAt admittedAt admissionExpiresAt orderId
--   waiting              zset           ticketId by seq (the line)
--   tickets              zset           every ticketId ever issued, by seq, for the admin board
--   admitted             pub/sub        space-separated ticketIds admitted by one admit.lua pass
--   active               zset           ticketId by admission deadline (who may check out now)
--   order:<id>           hash           ticketId userId quantity status createdAt paymentExpiresAt paidAt
--   pending              zset           orderId by payment deadline (stock held for unpaid orders)
--   orders               set            every orderId, for the audit
--   payments             list           one entry per charge, for the audit
--   admits:<second>, admitRateSince     admissions per second, for wait estimates
--   admitRate                           admissions per second over the last few seconds

local P = ARGV[1]

local function key(name, id)
  if id then return P .. name .. ':' .. id end
  return P .. name
end

local function hash(k)
  local flat = redis.call('HGETALL', k)
  local h = {}
  for i = 1, #flat, 2 do h[flat[i]] = flat[i + 1] end
  return h
end

-- Appends name/value pairs, skipping missing values (nil, or false from a missing Redis key).
local function append(out, ...)
  for i = 1, select('#', ...), 2 do
    local name, value = select(i, ...)
    if value ~= nil and value ~= false then
      out[#out + 1] = name
      out[#out + 1] = value
    end
  end
  return out
end

local function reply(outcome, fields)
  local out = {outcome}
  for _, v in ipairs(fields or {}) do out[#out + 1] = v end
  return out
end

local function stock()
  return tonumber(redis.call('GET', key('stock')) or '0')
end

-- Nothing left and nothing held by unpaid orders: no stock can ever come back.
local function sold_out()
  return stock() <= 0 and redis.call('ZCARD', key('pending')) == 0
end

-- Statuses as of `now`. Deadlines count even before the maintenance tick gets to them.
local function ticket_status(t, now)
  if t.status == 'admitted' and now >= tonumber(t.admissionExpiresAt) then return 'expired' end
  if (t.status == 'waiting' or t.status == 'admitted') and sold_out() then return 'sold_out' end
  return t.status
end

local function order_status(o, now)
  if o.status == 'pending_payment' and now >= tonumber(o.paymentExpiresAt) then return 'expired' end
  return o.status
end

local function ticket_view(id, now)
  local t = hash(key('ticket', id))
  if not t.status then return nil end
  local status = ticket_status(t, now)
  local out = {'ticketId', id, 'userId', t.userId, 'seq', t.seq, 'status', status}
  if status == 'waiting' then
    append(out, 'position', redis.call('ZRANK', key('waiting'), id) + 1, 'admitRate', redis.call('GET', key('admitRate')))
  end
  return append(out, 'admittedAt', t.admittedAt, 'admissionExpiresAt', t.admissionExpiresAt, 'orderId', t.orderId)
end

local function order_view(id, now)
  local o = hash(key('order', id))
  if not o.status then return nil end
  return append({'orderId', id, 'ticketId', o.ticketId, 'userId', o.userId, 'quantity', o.quantity,
                 'status', order_status(o, now), 'createdAt', o.createdAt, 'paymentExpiresAt', o.paymentExpiresAt},
                'paidAt', o.paidAt)
end

-- Only for orders still pending: puts the held stock back on the shelf.
local function expire_order(id, quantity)
  redis.call('HSET', key('order', id), 'status', 'expired')
  redis.call('INCRBY', key('stock'), quantity)
  redis.call('ZREM', key('pending'), id)
end
