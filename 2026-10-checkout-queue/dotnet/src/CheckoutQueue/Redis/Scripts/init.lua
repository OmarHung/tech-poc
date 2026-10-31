-- ARGV: prefix, initialStock
-- First instance to start sets the stock; later ones (and restarts) keep the live count.
if redis.call('SETNX', key('initialStock'), ARGV[2]) == 1 then
  redis.call('SET', key('stock'), ARGV[2])
end
return reply('ok', {'initialStock', redis.call('GET', key('initialStock'))})
