use std::collections::{HashMap, HashSet};

#[derive(Debug)]
enum DBError {
    KeyNotFound,
    WrongValueType,
}

enum DBOk{
    Ok,
    Nil
}

#[derive(Debug)]
enum Value {
    StringValue(String),
    SetValue(HashSet<String>),
    HashValue(HashMap<String, String>),
}

struct KVDB {
    db: HashMap<String, Value>,
    //时间轮
    ttl: HashMap<String, u64>,
}

impl KVDB {
    fn new() -> Self {
        KVDB {
            db: HashMap::new(),
            ttl: HashMap::new(),
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
    fn set(&mut self, key: &String, value: String, expire: Option<u64>, not_exists: bool, already_exists: bool) -> Result<DBOk, DBError> {
        let mut res: Result<DBOk, DBError>;
        match self.db.get(key) {
            Some(Value::StringValue(_)) => {
                if not_exists {
                    res = Ok(DBOk::Nil);
                }else {
                    self.db.insert(key.clone(), Value::StringValue(value));
                    res =Ok(DBOk::Ok) ;
                }
            }
            Some(_) => res = Err(DBError::WrongValueType),
            None => {
                if already_exists {
                    res =Ok(DBOk::Nil);
                }else {
                    self.db.insert(key.clone(), Value::StringValue(value));
                    res = Ok(DBOk::Ok);
                }
            }
        }
        match res {
            Ok(DBOk::Ok) => {
                if let Some(e) = expire{
                    self.ttl.insert(key.clone(), e);
                }
            },
            _=> {}
        };

        res
    }

    /// 获取与 key 关联的字符串的值
    /// 时间复杂度： O(1)
    /// 
    /// 返回值：
    ///     * key存在且value类型正确， 返回value
    ///     * value类型不是字符串， 返回WrongValueType
    ///     * key 不存在，返回None
    fn get(&self, key: &String)-> Result<Option<String>, DBError> {
        match self.db.get(key){
            Some(Value::StringValue(v)) =>  Ok(Some(v.clone())),
            Some(_) => Err(DBError::WrongValueType),
            None => Ok(None)
        }
    }

    //TODO: 待实现
    // fn incr(&mut self, key:String, value: String){
    // }

    /// 将一个或多个 member 元素加入到集合 key 当中，已经存在于集合的 member 元素将被忽略。
    /// 假如 key 不存在，则创建一个只包含 member 元素作成员的集合。
    /// 时间复杂度: O(N)， N 是被添加的元素的数量。
    ///
    /// 返回值：
    ///     * 被添加到集合中的**新元素**的数量，不包括被忽略的元素。
    ///     * 当 key 不是集合类型时，返回一个错误。
    fn sadd(&mut self, key: &String, members: Vec<String>) -> Result<u32, DBError> {
        match self.db.get_mut(key) {
            Some(Value::SetValue(v)) => {
                let mut counter: u32 = 0;
                members.into_iter().for_each(|member| {
                    if v.insert(member) {
                        counter += 1;
                    }
                });

                Ok(counter)
            }
            Some(_) => Err(DBError::WrongValueType),
            None => {
                let mut set = HashSet::new();
                let mut counter: u32 = 0;
                members.into_iter().for_each(|member| {
                    set.insert(member);
                    counter += 1;
                });
                self.db.insert(key.clone(), Value::SetValue(set));
                Ok(counter)
            }
        }
    }

    ///
    /// 移除并返回集合中的最多 count 个随机元素, 当集合的元素少于count时，返回集合中的所有元素。
    /// 时间复杂度 O(count)
    /// 
    /// 返回值：
    ///     * 最多 count 个集合元素
    ///     * key 对应 value 的类型不是 Set， 则返回 WrongValueType
    ///     * key不存在或空集则返回 None
    fn srandmember(&self, key: &String, count: usize)->Result< Option<HashSet<String>>, DBError> {
        match self.db.get(key) {
            Some(Value::SetValue(v)) => {
                //WARNNING: rust can only clone and then remove;
                let res: HashSet<String> = v.clone().into_iter().take(count).collect();
                Ok(Some(res))
            },
            Some(_) => Err(DBError::WrongValueType),
            None => Ok(None)
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
    fn spop(&mut self, key: &String)-> Result<Option<String>, DBError>{
        match self.db.get_mut(key) {
            Some(Value::SetValue(v)) => {
                //WARNNING: rust can only clone and then remove;
                let res: Option<String> = v.clone().into_iter().take(1).nth(0);
                res.iter().for_each(|s| {v.remove(s);});
                Ok(res)
            },
            Some(_) => Err(DBError::WrongValueType),
            None => Ok(None)
        }
    }

    fn sismember(&self, key: &String, member: &String) -> Result<Option<bool>, DBError>{
        match self.db.get(key){
            Some(Value::SetValue(v)) => {
                if v.contains(member){
                    Ok(Some(true))
                }else{
                    Ok(Some(false))
                }
            },
            Some(_) => Err(DBError::WrongValueType),
            None => Ok(None)
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
    fn srem(&mut self, key: &String, members: Vec<String>) -> Result<u32, DBError> {
        match self.db.get_mut(key) {
            Some(Value::SetValue(v)) => {
                let mut counter: u32 = 0;
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

    /// 获取集合中的所有成员 members
    /// 时间复杂度: O(N)， N 为给定 member 元素的数量
    ///
    /// 返回值：
    ///     * 正常情况，返回集合的所有成员
    ///     * key不存在，返回DBError::KeyNotFound
    ///     * value类型不是集合类型， 返回DBError::WrongValueType
    ///
    fn members(&self, key: &String)-> Result<Option<HashSet<String>>, DBError>{
        match self.db.get(key) {
            Some(Value::SetValue(v)) => {
                Ok(Some(v.clone()))
            }
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
    fn hset(&mut self, key: &String, field: String, value: String) -> Result<u32, DBError> {
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
                let mut hashmap: HashMap<String, String> = HashMap::new();
                hashmap.insert(field, value);
                self.db.insert(key.clone(), Value::HashValue(hashmap));
                Ok(1)
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
    fn hget(&self, key: &String, field: &String)-> Result<Option<String>, DBError>{
        unimplemented!()
    }

    ///
    /// 返回哈希表 key 中的所有域。
    /// 时间复杂度： O(N)， N为哈希表大小
    /// 
    /// 返回值：
    ///     * 返回 一个包含哈希表中所有域的表。
    ///     * 当 key 不存在时，返回 None。
    ///     * key对应的类型不是哈希表， 返回 WrongValueType
    fn hkeys(&self, key: &String)-> Result<Option<Vec<String>>, DBError>{
        unimplemented!()}

    ///
    /// 返回哈希表 key 中的所有值。
    /// 时间复杂度： O(N)， N为哈希表大小
    /// 
    /// 返回值：
    ///     * 返回 一个包含哈希表中所有值的表。
    ///     * 当 key 不存在时，返回 None。
    ///     * key对应的类型不是哈希表， 返回 WrongValueType
    fn hvalues(&self, key: &String)-> Result<Option<Vec<String>>, DBError>{
        unimplemented!()}

    ///
    /// 返回哈希表 key 中的所有域。
    /// 时间复杂度： O(N)， N为哈希表大小
    /// 
    /// 返回值：
    ///     * 返回 一个包含哈希表中所有域的表。
    ///     * 当 key 不存在时，返回 None。
    ///     * key对应的类型不是哈希表， 返回 WrongValueType
    fn hexists(&self, key: &String, field: &String) -> Result<Option<bool>, DBError>{
        unimplemented!()
    }

    ///
    /// 返回哈希表 key 中域的数量。
    /// 时间复杂度 O(N)， N为哈希表大小
    /// 
    /// 返回值：
    ///     * 哈希表中域的数量。
    ///     * 当 key 不存在时，返回 0 
    ///     * key对应的类型不是哈希表， 返回 WrongValueType
    fn hlen(&self, key: &String)->Result<Option<u32>, DBError>{
        unimplemented!()
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
    fn hdel(&mut self, key: &String, field: &String)->Result<Option<u32>, DBError> {
        unimplemented!()
    }

    /// set key ttl in seconds
    /// return:
    ///     -2: key not exists
    ///     -1: key not set a ttl
    ///     u32: live seconds of key
    // fn ttl(&mut self, key: &String, seconds: u32)-> i32{
    // }

    /// delete specific keys from db
    /// return : deleted keys num
    fn del(&mut self, keys: Vec<String>) -> u32 {
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
    fn exists(&self, key: &String)-> Result<Option<bool>, DBError>{
        unimplemented!()
    }
}

fn main() {
    println!("Hello, world!");
}
