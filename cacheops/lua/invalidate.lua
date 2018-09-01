local prefix = KEYS[1]
local db_table = ARGV[1]
local obj = cjson.decode(ARGV[2])
local conj_del_fn = 'unlink'
redis.replicate_commands()
local st = redis.call('TIME')[1]*1000000+redis.call('TIME')[2]
local script_timeout = tonumber(ARGV[3])
local max_number = tonumber(ARGV[4])
-- If Redis version < 4.0 we can't use UNLINK
-- TOSTRIP
conj_del_fn = 'del'
-- /TOSTRIP

-- Utility functions
local conj_cache_key = function (db_table, scheme, obj)
    local parts = {}
    for field in string.gmatch(scheme, "[^,]+") do
        table.insert(parts, field .. '=' .. tostring(obj[field]))
    end

    return prefix .. 'conj:' .. db_table .. ':' .. table.concat(parts, '&')
end

local call_in_chunks = function (command, args)
    local step = 1000
    for i = 1, #args, step do
        if redis.call('TIME')[1]*1000000+redis.call('TIME')[2]-st > script_timeout then
                return redis.error_reply('timeout' .. redis.call('TIME')[1]*1000000+redis.call('TIME')[2]-st .. prefix .. db_table)
        end
        redis.call(command, unpack(args, i, math.min(i + step - 1, #args)))
    end
end


-- Calculate conj keys
local conj_keys = {}
local schemes = redis.call('smembers', prefix .. 'schemes:' .. db_table)
for _, scheme in ipairs(schemes) do
    if redis.call('TIME')[1]*1000000+redis.call('TIME')[2]-st > script_timeout then
                return redis.error_reply('timeout' .. redis.call('TIME')[1]*1000000+redis.call('TIME')[2]-st .. prefix .. db_table )
     end
    table.insert(conj_keys, conj_cache_key(db_table, scheme, obj))
end


-- Delete cache keys and refering conj keys
if next(conj_keys) ~= nil then
    local total = 0
    for _, k in ipairs(conj_keys) do
        total = total + redis.call('SCARD',k)
        if total > max_number then
              return redis.error_reply('too many keys' .. unpack(conj_keys) .. prefix .. db_table .. total)
        end
    end

    local cache_keys = redis.call('sunion', unpack(conj_keys))

    -- we delete cache keys since they are invalid
    -- and conj keys as they will refer only deleted keys
    redis.call(conj_del_fn, unpack(conj_keys))
    if next(cache_keys) ~= nil then
        -- NOTE: can't just do redis.call('del', unpack(...)) cause there is limit on number
        --       of return values in lua.
        call_in_chunks(conj_del_fn, cache_keys)
    end
end
