use std::collections::{HashMap, HashSet};

#[derive(Debug, PartialEq, Eq)]
pub enum DBError {
    KeyNotFound,
    WrongValueType,
    OutOfKeysSize,
}

#[derive(Debug, PartialEq, Eq)]
pub enum DBOk {
    Ok,
    Nil,
}

#[derive(Debug)]
enum Value {
    StringValue(String),
    SetValue(HashSet<String>),
    HashValue(HashMap<String, String>),
}

pub type Result<T> = std::result::Result<T, DBError>;

#[derive(Debug)]
pub struct KVDB {
    db: HashMap<String, Value>,
    //时间轮
    ttl: HashMap<String, u64>,

    // 最多的 keys 数量, None时，无限制
    max_keys: Option<usize>,
}

pub const DEFAULT_DB_KEY_SIZE: usize = 256;

impl KVDB {
    /// 默认构建KVDB，无限 key size
    pub fn default() -> Self {
        KVDB::new(None)
    }

    /// 新建 KVDB ， 需要指定 key_size 大小, 默认为无限制
    pub fn new(key_size: Option<usize>) -> Self {
        KVDB {
            db: HashMap::new(),
            ttl: HashMap::new(),
            max_keys: key_size,
        }
    }

    /// internal：判断KVDB 是否可以创建新的key
    ///
    /// 返回:  
    ///     * true： 可以创建新的key
    ///     * false: 不可以创建新的key
    pub fn can_add_key(&self) -> bool {
        if let Some(size) = self.max_keys {
            size > 0 && self.db.len() < size
        } else {
            true
        }
    }

    ///将字符串值 value 关联到 key 。
    /// 如果 key 已经持有其他值， SET 就覆写旧值， 无视类型。
    /// TODO: 当 SET 命令对一个带有生存时间（TTL）的键进行设置之后， 该键原有的 TTL 将被清除。
    /// 时间复杂度： O(1)
    ///
    /// 参数说明：
    ///     * not_exists 只有在key不存在时，才插入
    ///     * already_exists 只有在key已经存在时，才插入
    ///
    /// 返回值：
    ///     * 只在设置操作成功完成时才返回 OK
    pub fn set(
        &mut self,
        key: &String,
        value: String,
        not_exists: bool,
        already_exists: bool,
        expire: Option<u64>,
    ) -> Result<DBOk> {
        let res: Result<DBOk>;
        match self.db.get(key) {
            Some(Value::StringValue(_)) => {
                if not_exists {
                    res = Ok(DBOk::Nil);
                } else {
                    if self.can_add_key() {
                        self.db.insert(key.clone(), Value::StringValue(value));
                        res = Ok(DBOk::Ok);
                    } else {
                        res = Err(DBError::OutOfKeysSize)
                    }
                }
            }
            Some(_) => res = Err(DBError::WrongValueType),
            None => {
                if already_exists {
                    res = Ok(DBOk::Nil);
                } else {
                    if self.can_add_key() {
                        self.db.insert(key.clone(), Value::StringValue(value));
                        res = Ok(DBOk::Ok);
                    } else {
                        res = Err(DBError::OutOfKeysSize)
                    }
                }
            }
        }
        match res {
            Ok(DBOk::Ok) => {
                if let Some(e) = expire {
                    self.ttl.insert(key.clone(), e);
                }
            }
            _ => {}
        };

        res
    }

    /// 简单的插入方法
    /// 详情查看 `set()` 方法
    pub fn sets(&mut self, key: &String, value: String) -> Result<DBOk> {
        self.set(key, value, false, false, None)
    }

    /// 获取与 key 关联的字符串的值
    /// 时间复杂度： O(1)
    ///
    /// 返回值：
    ///     * key存在且value类型正确， 返回value
    ///     * value类型不是字符串， 返回WrongValueType
    ///     * key 不存在，返回None
    pub fn get(&self, key: &String) -> Result<Option<String>> {
        match self.db.get(key) {
            Some(Value::StringValue(v)) => Ok(Some(v.clone())),
            Some(_) => Err(DBError::WrongValueType),
            None => Ok(None),
        }
    }

    //TODO: 待实现
    // pub fn incr(&mut self, key:String, value: String){
    // }

    /// 将一个或多个 member 元素加入到集合 key 当中，已经存在于集合的 member 元素将被忽略。
    /// 假如 key 不存在，则创建一个只包含 member 元素作成员的集合。
    /// 时间复杂度: O(N)， N 是被添加的元素的数量。
    ///
    /// 返回值：
    ///     * 被添加到集合中的**新元素**的数量，不包括被忽略的元素。
    ///     * 当 key 不是集合类型时，返回一个错误。
    pub fn sadd(&mut self, key: &String, members: Vec<String>) -> Result<usize> {
        let mut counter: usize = 0;
        match self.db.get_mut(key) {
            Some(Value::SetValue(v)) => {
                members.into_iter().for_each(|member| {
                    if v.insert(member) {
                        counter += 1;
                    }
                });

                Ok(counter)
            }
            Some(_) => Err(DBError::WrongValueType),
            None => {
                if self.can_add_key() {
                    let mut set = HashSet::new();
                    members.into_iter().for_each(|member| {
                        set.insert(member);
                        counter += 1;
                    });
                    self.db.insert(key.clone(), Value::SetValue(set));
                    Ok(counter)
                } else {
                    Err(DBError::OutOfKeysSize)
                }
            }
        }
    }

    ///
    /// 移除并返回集合中的最多 count 个随机元素, 当集合的元素少于count时，返回集合中的所有元素。
    /// 时间复杂度 O(N), N 为 set 集合元素个数
    /// TODO： 时间复杂度提升
    ///
    /// 返回值：
    ///     * 最多 count 个集合元素
    ///     * key 对应 value 的类型不是 Set， 则返回 WrongValueType
    ///     * key不存在或空集则返回 None
    pub fn srandmember(&mut self, key: &String, count: usize) -> Result<Option<HashSet<String>>> {
        match self.db.get_mut(key) {
            Some(Value::SetValue(v)) => {
                //WARNNING: rust can only clone and then remove;
                let res: HashSet<String> = v.clone().into_iter().take(count).collect();
                res.iter().for_each(|s| {
                    v.remove(s);
                });
                Ok(Some(res))
            }
            Some(_) => Err(DBError::WrongValueType),
            None => Ok(None),
        }
    }

    ///
    /// 移除并返回集合中的一个随机元素。
    /// 时间复杂度: O(1)
    ///
    /// 返回值：
    ///     * 被移除的随机元素。
    ///     * 当 key 不存在或 key 是空集时，返回 None
    ///     * 当key对应的value 不是 Set 时，返回 WrongValueType
    pub fn spop(&mut self, key: &String) -> Result<Option<String>> {
        match self.db.get_mut(key) {
            Some(Value::SetValue(v)) => {
                //WARNNING: rust can only clone and then remove;
                let res: Option<String> = v.clone().into_iter().take(1).nth(0);
                res.iter().for_each(|s| {
                    v.remove(s);
                });
                Ok(res)
            }
            Some(_) => Err(DBError::WrongValueType),
            None => Ok(None),
        }
    }

    pub fn sismember(&self, key: &String, member: &String) -> Result<Option<bool>> {
        match self.db.get(key) {
            Some(Value::SetValue(v)) => {
                if v.contains(member) {
                    Ok(Some(true))
                } else {
                    Ok(Some(false))
                }
            }
            Some(_) => Err(DBError::WrongValueType),
            None => Ok(None),
        }
    }

    /// 移除集合 key 中的一个或多个 member 元素，不存在的 member 元素会被忽略。
    /// 时间复杂度: O(N)， N 为给定 member 元素的数量
    ///
    /// 返回值：
    ///     * 正常情况，返回被成功移除的元素的数量，不包括被忽略的元素。
    ///     * key不存在，返回0
    ///     * value类型不是集合类型， 返回DBError::WrongValueType
    ///
    pub fn srem(&mut self, key: &String, members: Vec<String>) -> Result<usize> {
        match self.db.get_mut(key) {
            Some(Value::SetValue(v)) => {
                let mut counter: usize = 0;
                members.iter().for_each(|member| {
                    if v.remove(member) {
                        counter += 1;
                    }
                });
                Ok(counter)
            }
            Some(_) => Err(DBError::WrongValueType),
            None => Ok(0),
        }
    }

    /// 获取集合中的成员数量
    /// 时间复杂度： O(1)
    ///
    /// 返回值：
    ///     * 集合中成员数量
    ///     * key不存在，返回DBError::KeyNotFound
    ///     * value类型不是集合类型， 返回DBError::WrongValueType
    pub fn slen(&self, key: &String) -> Result<Option<usize>> {
        match self.db.get(key) {
            Some(Value::SetValue(v)) => Ok(Some(v.len())),
            Some(_) => Err(DBError::WrongValueType),
            None => Ok(None),
        }
    }

    /// 获取集合中的所有成员 members
    /// 时间复杂度: O(N)， N 为给定 member 元素的数量
    ///
    /// 返回值：
    ///     * 正常情况，返回集合的所有成员
    ///     * key不存在，返回DBError::KeyNotFound
    ///     * value类型不是集合类型， 返回DBError::WrongValueType
    ///
    pub fn smembers(&self, key: &String) -> Result<Option<HashSet<String>>> {
        match self.db.get(key) {
            Some(Value::SetValue(v)) => Ok(Some(v.clone())),
            Some(_) => Err(DBError::WrongValueType),
            None => Ok(None),
        }
    }

    /// 将哈希表 hash 中域 field 的值设置为 value 。
    /// 时间复杂度： O(1)
    ///
    /// * 如果给定的哈希表并不存在， 那么一个新的哈希表将被创建并执行 HSET 操作。
    /// * 如果域 field 已经存在于哈希表中， 那么它的旧值将被新值 value 覆盖。
    ///
    /// 返回值：
    ///     * 创建新的field，则返回1；
    ///     * 覆盖原field，则返回0；
    ///     * key对应的类型不是HashMap类型，那么返回错误信息
    pub fn hset(&mut self, key: &String, field: String, value: String) -> Result<u32> {
        match self.db.get_mut(key) {
            Some(Value::HashValue(v)) => {
                if let Some(_) = v.insert(field, value) {
                    Ok(0)
                } else {
                    Ok(1)
                }
            }
            Some(_) => Err(DBError::WrongValueType),
            None => {
                if self.can_add_key() {
                    let mut hashmap: HashMap<String, String> = HashMap::new();
                    hashmap.insert(field, value);
                    self.db.insert(key.clone(), Value::HashValue(hashmap));
                    Ok(1)
                } else {
                    Err(DBError::OutOfKeysSize)
                }
            }
        }
    }

    ///
    /// 返回哈希表中给定域的值。
    /// 时间复杂度： O(1)
    ///
    /// 返回值：
    ///     * 返回 给定域 field 的值
    ///     * 给定域不存在于哈希表中， 又或者给定的哈希表并不存在， 返回None
    ///     * key对应的类型不是哈希表， 返回 WrongValueType
    pub fn hget(&self, key: &String, field: &String) -> Result<Option<String>> {
        match self.db.get(key) {
            Some(Value::HashValue(v)) => {
                if let Some(value) = v.get(field) {
                    Ok(Some(value.clone()))
                } else {
                    Ok(None)
                }
            }
            Some(_) => Err(DBError::WrongValueType),
            None => Ok(None),
        }
    }

    /// 同时将多个 field-value (域-值)对设置到哈希表 key 中。
    /// 此命令会覆盖哈希表中已存在的域。
    /// 时间复杂度：O(N)， N 为 field-value 对的数量。
    ///
    /// 返回值：
    ///     * 如果命令执行成功，返回 OK 。
    ///     * 当 key 不是哈希表(hash)类型时，返回一个错误。
    pub fn hmset(&mut self, key: &String, pairs: Vec<(String, String)>) -> Result<DBOk> {
        match self.db.get_mut(key) {
            Some(Value::HashValue(v)) => {
                pairs.into_iter().for_each(|(field, value)| {
                    v.insert(field, value);
                });
                Ok(DBOk::Ok)
            }
            Some(_) => Err(DBError::WrongValueType),
            None => {
                if self.can_add_key() {
                    let mut hashmap: HashMap<String, String> = HashMap::new();
                    pairs.into_iter().for_each(|(field, value)| {
                        hashmap.insert(field, value);
                    });
                    self.db.insert(key.clone(), Value::HashValue(hashmap));
                    Ok(DBOk::Ok)
                } else {
                    Err(DBError::OutOfKeysSize)
                }
            }
        }
    }

    ///返回哈希表 key 中，一个或多个给定域的值。
    /// 时间复杂度： O(N), N 是 fields 的数量
    ///
    /// 返回值：
    ///     * fields 对应的 values ；顺序一一对应
    ///     * 如果 filed 不存在，返回Option::None
    ///     * 如果 key 不存在，那么返回 DBError::KeyNotFound
    pub fn hmget(&self, key: &String, fields: &Vec<String>) -> Result<Vec<Option<String>>> {
        match self.db.get(key) {
            Some(Value::HashValue(v)) => {
                let values: Vec<Option<String>> = fields
                    .iter()
                    .map(|field| v.get(field).and_then(|z| Some(z.clone())))
                    .collect();
                Ok(values)
            }
            Some(_) => Err(DBError::WrongValueType),
            None => Err(DBError::KeyNotFound),
        }
    }

    ///
    /// 返回哈希表 key 中的所有域。
    /// 时间复杂度： O(N)， N为哈希表大小
    ///
    /// 返回值：
    ///     * 返回 一个包含哈希表中所有域的表。
    ///     * 当 key 不存在时，返回 None。
    ///     * key对应的类型不是哈希表， 返回 WrongValueType
    pub fn hkeys(&self, key: &String) -> Result<Option<Vec<String>>> {
        match self.db.get(key) {
            Some(Value::HashValue(v)) => {
                let keys: Vec<String> = v.keys().map(|s| s.clone()).collect();
                Ok(Some(keys))
            }
            Some(_) => Err(DBError::WrongValueType),
            None => Ok(None),
        }
    }

    ///
    /// 返回哈希表 key 中的所有值。
    /// 时间复杂度： O(N)， N为哈希表大小
    ///
    /// 返回值：
    ///     * 返回 一个包含哈希表中所有值的表。
    ///     * 当 key 不存在时，返回 None。
    ///     * key对应的类型不是哈希表， 返回 WrongValueType
    pub fn hvalues(&self, key: &String) -> Result<Option<Vec<String>>> {
        match self.db.get(key) {
            Some(Value::HashValue(v)) => {
                let values: Vec<String> = v.values().map(|s| s.clone()).collect();
                Ok(Some(values))
            }
            Some(_) => Err(DBError::WrongValueType),
            None => Ok(None),
        }
    }

    ///
    /// 返回哈希表 key 中的 field 是否存在。
    /// 时间复杂度： O(1)，
    ///
    /// 返回值：
    ///     * field 存在时，返回 true， field 不存在， 返回 false。
    ///     * 当 key 不存在时，返回 None。
    ///     * key对应的类型不是哈希表， 返回 WrongValueType
    pub fn hexists(&self, key: &String, field: &String) -> Result<Option<bool>> {
        match self.db.get(key) {
            Some(Value::HashValue(v)) => {
                if v.contains_key(field) {
                    Ok(Some(true))
                } else {
                    Ok(Some(false))
                }
            }
            Some(_) => Err(DBError::WrongValueType),
            None => Ok(None),
        }
    }

    ///
    /// 返回哈希表 key 中域的数量。
    /// 时间复杂度 O(N)， N为哈希表大小
    ///
    /// 返回值：
    ///     * 哈希表中域的数量。
    ///     * 当 key 不存在时，返回 0
    ///     * key对应的类型不是哈希表， 返回 WrongValueType
    pub fn hlen(&self, key: &String) -> Result<Option<usize>> {
        match self.db.get(key) {
            Some(Value::HashValue(v)) => Ok(Some(v.len())),
            Some(_) => Err(DBError::WrongValueType),
            None => Ok(None),
        }
    }

    ///
    /// 删除哈希表 key 中的一个或多个指定域，不存在的域将被忽略。
    /// 时间复杂度：O(N)， N 为要删除的域的数量。
    ///
    /// 返回值：
    ///     * 被成功移除的域的数量，不包括被忽略的域。
    ///     * 当 key 不存在时，返回 0
    ///     * key对应的类型不是哈希表， 返回 WrongValueType
    ///
    pub fn hdel(&mut self, key: &String, field: &String) -> Result<Option<usize>> {
        match self.db.get_mut(key) {
            Some(Value::HashValue(v)) => {
                if let Some(_) = v.remove(field) {
                    Ok(Some(1))
                } else {
                    Ok(Some(0))
                }
            }
            Some(_) => Err(DBError::WrongValueType),
            None => Ok(None),
        }
    }

    /// set key ttl in seconds
    /// return:
    ///     -2: key not exists
    ///     -1: key not set a ttl
    ///     u32: live seconds of key
    // pub fn ttl(&mut self, key: &String, seconds: u32)-> i32{
    // }

    /// 删除db中的keys
    /// 时间复杂度 O(N), N为输入的key的数量
    ///
    /// 返回值：成功删除的key的数量
    ///     
    pub fn del(&mut self, keys: Vec<String>) -> u32 {
        let mut counter = 0;
        keys.iter().for_each(|key| {
            if let Some(_) = self.db.remove(key) {
                counter += 1
            }
        });

        counter
    }

    ///
    /// 判断库中 key 是否存在
    /// 时间复杂度 O(1)
    ///
    /// 返回值：key存在返回 true; 否则返回false
    pub fn exists(&self, key: &String) -> bool {
        self.db.contains_key(key)
    }

    ///
    /// 获取数据库中 key的数量
    /// 时间复杂度 O(N), N数据库中的key的数量
    ///
    /// TODO: 可以优化为O(1), 添加一个key的计数器。
    ///
    /// 返回值：数据库中key的数量
    pub fn size(&self) -> usize {
        self.db.len()
    }
}
