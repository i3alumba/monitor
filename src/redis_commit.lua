-- CAS all inputs before the only allocating write. MSET is one Redis command:
-- an OOM/command rejection cannot commit a prefix of the batch. No Lua numbers
-- are used for sequences or metric values; Rust validates and serializes them.
local n = tonumber(ARGV[1])
for i = 1, n do
    local actual = redis.call('GET', KEYS[i])
    local present = ARGV[2 + (i-1)*2]
    local expected = ARGV[3 + (i-1)*2]
    if (present == '0' and actual ~= false) or
       (present == '1' and actual ~= expected) then
        return 0
    end
end
local offset = 2 + n*2
local writes = tonumber(ARGV[offset])
local values = {}
for i = 1, writes do
    table.insert(values, KEYS[n+i])
    table.insert(values, ARGV[offset+1+(i-1)*2])
end
if writes > 0 then
    redis.call('MSET', unpack(values))
    for i = 1, writes do
        local expiry = ARGV[offset+2+(i-1)*2]
        if expiry == '-1' then
            redis.call('DEL', KEYS[n+i])
        elseif expiry ~= '0' then
            redis.call('PEXPIREAT', KEYS[n+i], expiry)
        end
    end
end
return 1
