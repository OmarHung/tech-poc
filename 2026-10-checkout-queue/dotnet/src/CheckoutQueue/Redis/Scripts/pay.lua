-- ARGV: prefix, orderId, now
-- Simulated payment. The charge is recorded in the same atomic step that moves the order
-- out of pending_payment, so however many times "pay" is clicked, it charges once.
local id, now = ARGV[2], tonumber(ARGV[3])
local orderKey = key('order', id)
local o = hash(orderKey)
if not o.status then return reply('order_not_found') end
if o.status == 'paid' then return reply('already_paid', order_view(id, now)) end
if o.status == 'expired' then return reply('order_expired', order_view(id, now)) end

if now >= tonumber(o.paymentExpiresAt) then
  expire_order(id, tonumber(o.quantity))
  return reply('order_expired', order_view(id, now))
end

redis.call('RPUSH', key('payments'), id)
redis.call('HSET', orderKey, 'status', 'paid', 'paidAt', now)
redis.call('ZREM', key('pending'), id)
return reply('paid', order_view(id, now))
