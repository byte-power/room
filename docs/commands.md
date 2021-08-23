# room 实现的 redis 命令列表

bytepower room 服务使用 redis protocol, 实现了 redis standalone (即单机版 redis) 6.0.x 版本的大部分命令，具体实现的命令如下所示。

## keys commands

+ del
+ exists
+ expire
+ expireat
+ persist
+ pexpire
+ pexpireat
+ pttl
+ rename
+ renamenx
+ ttl
+ type

## string commands

+ set
+ get
+ append
+ decr
+ decrby
+ getrange
+ getset
+ incr
+ incrby
+ incrbyfloat
+ mget
+ mset
+ msetnx
+ psetex
+ setex
+ setnx
+ setrange
+ strlen

## list commands

+ lindex
+ linsert
+ llen
+ lpop
+ lpos
+ lpush
+ lpushx
+ lrange
+ lrem
+ lset
+ ltrim
+ rpop
+ rpoplpush
+ lmove
+ rpush
+ rpushx

## set commands

+ sadd
+ scard
+ sdiff
+ sdiffstore
+ sinter
+ sinterstore
+ sismember
+ smismember
+ smembers
+ smove
+ spop
+ srandmember
+ srem
+ sunion
+ sunionstore

## hash commands

+ hdel
+ hexists
+ hget
+ hgetall
+ hincrby
+ hincrbyfloat
+ hkeys
+ hlen
+ hmget
+ hmset
+ hset
+ hsetnx
+ hstrlen
+ hvals

## zset commands

+ zadd
+ zcard
+ zcount
+ zdiff
+ zdiffstore
+ zincrby
+ zlexcount
+ zpopmax
+ zpopmin
+ zrange
+ zrangebylex
+ zrevrangebylex
+ zrangebyscore
+ zrank
+ zrem
+ zremrangebylex
+ zremrangebyrank
+ zremrangebyscore
+ zrevrange
+ zrevrangebyscore
+ zrevrank
+ zscore
+ zmscore

## server commands

+ command
+ echo
+ ping

## transaction commands

+ watch
+ multi
+ exec
+ discard
+ unwatch
