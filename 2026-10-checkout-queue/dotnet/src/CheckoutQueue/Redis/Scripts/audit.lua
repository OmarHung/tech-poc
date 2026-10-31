-- ARGV: prefix
-- Recounts everything from the orders themselves. Walks every order, so it is an admin tool.
local totals = {pending_payment = {0, 0}, paid = {0, 0}, expired = {0, 0}}
local liveByUser, multiple = {}, 0
for _, id in ipairs(redis.call('SMEMBERS', key('orders'))) do
  local o = redis.call('HMGET', key('order', id), 'status', 'quantity', 'userId')
  local total = totals[o[1]]
  total[1] = total[1] + 1
  total[2] = total[2] + tonumber(o[2])
  if o[1] ~= 'expired' then
    liveByUser[o[3]] = (liveByUser[o[3]] or 0) + 1
    if liveByUser[o[3]] == 2 then multiple = multiple + 1 end
  end
end

local charges = redis.call('LRANGE', key('payments'), 0, -1)
local charged, duplicates = {}, 0
for _, id in ipairs(charges) do
  if charged[id] then duplicates = duplicates + 1 end
  charged[id] = true
end

return reply('ok', {
  'initialStock', redis.call('GET', key('initialStock')) or 0,
  'stock', stock(),
  'pendingOrders', totals.pending_payment[1], 'pendingQuantity', totals.pending_payment[2],
  'paidOrders', totals.paid[1], 'paidQuantity', totals.paid[2],
  'expiredOrders', totals.expired[1], 'expiredQuantity', totals.expired[2],
  'charges', #charges, 'duplicateCharges', duplicates,
  'usersWithMultipleLiveOrders', multiple,
  'tickets', redis.call('GET', key('seq')) or 0,
  'waiting', redis.call('ZCARD', key('waiting')),
  'active', redis.call('ZCARD', key('active')),
})
