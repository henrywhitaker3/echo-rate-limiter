local exists = redis.call("EXISTS", KEYS[1])
if exists == 0 then
	redis.call("SET", KEYS[1], 1, "EX", ARGV[1])
else
	redis.call("INCR", KEYS[1])
end
return redis.call("GET", KEYS[1])
