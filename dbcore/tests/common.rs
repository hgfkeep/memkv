use dbcore::KVDB;
use dbcore::DBOk;

#[cfg(test)]
pub fn setup(key_size: Option<usize>) -> KVDB {
    KVDB::new(key_size)
}

pub fn setup_common_one_key_set(key: &String, members: &Vec<String> ) -> KVDB {
    let mut db: KVDB = KVDB::new(Some(1));
    assert_eq!(Ok(members.len()), db.sadd(key, members.clone()));
    db
}

pub fn setup_common_one_key_hash(key: &String, pairs: &Vec<(String,String)> ) -> KVDB {
    let mut db: KVDB = KVDB::new(Some(1));
    assert_eq!(Ok(DBOk::Ok), db.hmset(key, pairs.clone()));
    assert_eq!(Ok(Some(pairs.len())), db.hlen(key));
    db
}