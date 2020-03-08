# memkv

in memory kv store written in rust. please wait, it's under constructing.

## Done

* String
    - [x] get key
    - [x] set key value
    - [x] set key value expire not_exists already_exists
* List
* Set
    - [x] sadd key member [member ...]
    - [x] srandmember key count
    - [x] spop key
    - [x] sismember key member
    - [x] srem key member [member ...]
    - [x] slen key
    - [x] smembers key
* ZSet
* Hash
    - [x] hget key field
    - [x] hset key field value
    - [x] hmset key field value [field value ...]
    - [x] hmget key field [field ...]
    - [x] hkeys key
    - [x] hvalues key
    - [x] hexists key field
    - [x] hlen key
    - [x] hdel key field
* Common
    - [x] del key [key ...]
    - [x] exists key
    - [x] size
    - [ ] ttl
    - [ ] info
