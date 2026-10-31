-- ARGV: prefix, now, ticketId...
-- Batched views for the tickets one instance is streaming over server-sent events.
-- An open stream means the user is still there, so it is their heartbeat too.
local now = tonumber(ARGV[2])
local rows = {}
for i = 3, #ARGV do
  local id = ARGV[i]
  if redis.call('HGET', key('ticket', id), 'status') == 'waiting' then
    redis.call('HSET', key('ticket', id), 'lastSeenAt', now)
  end
  local view = ticket_view(id, now)
  if view then rows[#rows + 1] = reply('ticket', view) end
end
return rows
