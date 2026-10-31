-- ARGV: prefix, orderId, now
local view = order_view(ARGV[2], tonumber(ARGV[3]))
if not view then return reply('not_found') end
return reply('ok', view)
