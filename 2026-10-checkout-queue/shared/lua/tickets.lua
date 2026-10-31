-- ARGV: prefix, now, limit
-- Read-only board of the newest `limit` users: each user's current ticket, then its order if any.
-- Unlike ticket.lua this is not a heartbeat, so watching the board cannot keep anyone in line.
local now, limit = tonumber(ARGV[2]), tonumber(ARGV[3])
local rows = {}
for _, id in ipairs(redis.call('ZREVRANGE', key('tickets'), 0, limit - 1)) do
  local userId = redis.call('HGET', key('ticket', id), 'userId')
  -- A user who lined up again has older tickets too; only the current one is on the board.
  if redis.call('GET', key('user', userId)) == id then
    local view = ticket_view(id, now)
    rows[#rows + 1] = reply('ticket', view)
    local orderId = redis.call('HGET', key('ticket', id), 'orderId')
    if orderId then rows[#rows + 1] = reply('order', order_view(orderId, now)) end
  end
end
return rows
