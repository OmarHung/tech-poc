-- ARGV: prefix, ticketId, now
local id, now = ARGV[2], tonumber(ARGV[3])

-- Polling doubles as the heartbeat: whoever stops polling is skipped when their turn comes.
if redis.call('HGET', key('ticket', id), 'status') == 'waiting' then
  redis.call('HSET', key('ticket', id), 'lastSeenAt', now)
end

local view = ticket_view(id, now)
if not view then return reply('not_found') end
return reply('ok', view)
