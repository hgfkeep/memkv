/// 声明测试通用模块
mod common;

use dbcore::{DBError, DBOk, KVDB};
use std::collections::HashSet;
use std::iter::FromIterator;

#[test]
#[ignore]
fn keys_size() {
    let mut db: KVDB = common::setup(Some(1));
    let key = String::from("key");
    let value = String::from("value");
    assert_eq!(
        Ok(DBOk::Ok),
        db.set(&key, value.clone(), false, false, None)
    );

    let key2 = String::from("key2");
    let value2 = String::from("value2");
    assert_eq!(
        Err(DBError::OutOfKeysSize),
        db.set(&key2, value2, false, false, None)
    );

    assert_eq!(Ok(Some(value)), db.get(&key));
    assert_eq!(Ok(None), db.get(&key2));
}

#[test]
#[ignore]
fn string_one_set() {
    let mut db: KVDB = common::setup(None);
    let key = String::from("key");
    let value = String::from("value");
    assert_eq!(
        Ok(DBOk::Ok),
        db.set(&key, value.clone(), false, false, None)
    );

    assert_eq!(Ok(Some(value)), db.get(&key));
}

#[test]
#[ignore]
fn string_ten_add_with_one_key_diff_value() {
    let mut db: KVDB = common::setup(None);
    let key: String = String::from("key");
    let mut last: u32 = 0;
    for x in 0..10 {
        last = x;
        assert_eq!(
            Ok(DBOk::Ok),
            db.set(&key, x.to_string(), false, false, None)
        );
    }
    assert_eq!(Ok(Some(last.to_string())), db.get(&key));
}

#[test]
#[ignore]
fn string_ten_keys() {
    let mut db: KVDB = common::setup(None);
    for x in 0..10 {
        assert_eq!(
            Ok(DBOk::Ok),
            db.set(&x.to_string(), x.to_string(), false, false, None)
        );
    }
    for x in 0..10 {
        assert_eq!(Ok(Some(x.to_string())), db.get(&x.to_string()));
    }
}

#[test]
#[ignore]
fn string_key_not_exists() {
    let not_exists = true;
    let mut db: KVDB = common::setup(None);
    let key: String = String::from("key");
    for x in 0..10 {
        if x == 0 {
            assert_eq!(
                Ok(DBOk::Ok),
                db.set(&key, x.to_string(), not_exists, false, None)
            );
        } else {
            assert_eq!(Ok(Some(0.to_string())), db.get(&key));
            assert_eq!(
                Ok(DBOk::Nil),
                db.set(&key, x.to_string(), not_exists, false, None)
            );
        }
    }
    assert_eq!(Ok(Some(0.to_string())), db.get(&key));
}

#[test]
#[ignore]
fn set_add_one() {
    let key: String = String::from("key");
    let members: Vec<String> = vec![String::from("a"), String::from("b"), String::from("c")];
    let mut db: KVDB = KVDB::new(Some(1));
    assert_eq!(Ok(members.len()), db.sadd(&key, members.clone()));

    let other_key: String = String::from("other_key");
    let other_members: Vec<String> = vec![String::from("a"), String::from("d")];
    assert_eq!(
        Err(DBError::OutOfKeysSize),
        db.sadd(&other_key, other_members.clone())
    );

    assert_eq!(Ok(1), db.sadd(&key, other_members));
}

#[test]
#[ignore]
fn set_randommember() {
    let key: String = String::from("key");
    let members: Vec<String> = vec![String::from("a"), String::from("b"), String::from("c")];

    let mut db: KVDB = common::setup_common_one_key_set(&key, &members);

    let get_count: usize = 1;
    let set: HashSet<String> = HashSet::from_iter(members);
    let res: Result<Option<HashSet<String>>, DBError> = db.srandmember(&key, get_count);
    assert_ne!(Ok(None), res);
    if let Ok(Some(s)) = res {
        assert_eq!(get_count, s.len());
        s.iter().for_each(|r| assert_eq!(true, set.contains(r)));
    }

    assert_eq!(Ok(Some(set.len() - get_count)), db.slen(&key));
}

#[test]
#[ignore]
fn set_add_and_pop() {
    let key: String = String::from("key");
    let members: Vec<String> = vec![String::from("a"), String::from("b"), String::from("c")];

    let mut db: KVDB = common::setup_common_one_key_set(&key, &members);

    let res = db.spop(&key);
    assert_ne!(Err(DBError::WrongValueType), res);
    if let Ok(Some(s)) = res {
        assert_eq!(true, members.contains(&s));
    }

    assert_eq!(Ok(Some(members.len() - 1)), db.slen(&key));
}

#[test]
#[ignore]
fn set_pop_where_not_exists() {
    let key: String = String::from("key");
    let mut db: KVDB = common::setup(None);
    assert_eq!(Ok(None), db.spop(&key));
}

#[test]
#[ignore]
fn set_ismember() {
    let key: String = String::from("key");
    let members: Vec<String> = vec![String::from("a"), String::from("b"), String::from("c")];

    let db: KVDB = common::setup_common_one_key_set(&key, &members);

    assert_eq!(
        Ok(Some(false)),
        db.sismember(&key, &String::from("not_eixsts"))
    );
    assert_eq!(Ok(Some(true)), db.sismember(&key, &String::from("a")));

    let other_key = String::from("other_key");
    assert_eq!(Ok(None), db.sismember(&other_key, &String::from("a")));
}

#[test]
#[ignore]
fn set_remove() {
    let key: String = String::from("key");
    let members: Vec<String> = vec![String::from("a"), String::from("b"), String::from("c")];

    let mut db: KVDB = common::setup_common_one_key_set(&key, &members);

    assert_eq!(Ok(0), db.srem(&key, vec![String::from("d")]));
    assert_eq!(Ok(members.len()), db.srem(&key, members));
}

#[test]
#[ignore]
fn set_remove_where_part_exists() {
    let key: String = String::from("key");
    let members: Vec<String> = vec![String::from("a"), String::from("b"), String::from("c")];

    let mut db: KVDB = common::setup_common_one_key_set(&key, &members);
    let members_to_remove: Vec<String> =
        vec![String::from("a"), String::from("c"), String::from("c")];

    assert_eq!(Ok(2), db.srem(&key, members_to_remove));
}

#[test]
#[ignore]
fn set_members() {
    let key: String = String::from("key");
    let members: Vec<String> = vec![String::from("a"), String::from("b"), String::from("c")];

    let mut db: KVDB = common::setup(None);
    assert_eq!(Ok(None), db.smembers(&key));
    assert_eq!(Ok(members.len()), db.sadd(&key, members.clone()));
    let set: HashSet<String> = HashSet::from_iter(members);
    assert_eq!(Ok(Some(set)), db.smembers(&key));
}

#[test]
#[ignore]
fn hash_add_one() {
    let key: String = String::from("key");
    let pairs: Vec<(String, String)> = vec![
        (String::from("a_key"), String::from("a_value")),
        (String::from("b_key"), String::from("b_value")),
    ];
    let mut db: KVDB = KVDB::new(Some(1));
    assert_eq!(Ok(DBOk::Ok), db.hmset(&key, pairs.clone()));
    assert_eq!(Ok(Some(pairs.len())), db.hlen(&key));
    assert_eq!(
        Ok(Some(String::from("a_value"))),
        db.hget(&key, &String::from("a_key"))
    );
}

#[test]
#[ignore]
fn hash_add_out_of_keys_size() {
    let key: String = String::from("key");
    let pairs: Vec<(String, String)> = vec![(String::from("a_key"), String::from("a_value"))];
    let mut db: KVDB = common::setup_common_one_key_hash(&key, &pairs);

    let other_key: String = String::from("other_key");
    let res = db.hset(&other_key, String::from("field"), String::from("values"));
    assert_eq!(Err(DBError::OutOfKeysSize), res);
}

#[test]
#[ignore]
fn hash_fields_weather_exists() {
    let key: String = String::from("key");
    let pairs: Vec<(String, String)> = vec![(String::from("a_key"), String::from("a_value"))];
    let db: KVDB = common::setup_common_one_key_hash(&key, &pairs);

    let res = db.hexists(&key, &pairs[0].0);
    assert_eq!(Ok(Some(true)), res);

    let res = db.hexists(&key, &String::from("field_not_in_db"));
    assert_eq!(Ok(Some(false)), res);

    let res = db.hexists(&String::from("key_not_in_db"), &pairs[0].0);
    assert_eq!(Ok(None), res);
}

#[test]
#[ignore]
fn hash_multi_process() {
    let key: String = String::from("key");
    let pairs: Vec<(String, String)> = vec![(String::from("a_key"), String::from("a_value"))];
    let db: KVDB = common::setup_common_one_key_hash(&key, &pairs);

    let mut fields: Vec<String> = pairs.iter().map(|(f, _v)| f.to_owned()).collect();
    fields.push(String::from("not_exists_field"));
    let res = db.hmget(&key, &fields);
    let expect: Vec<Option<String>> = vec![Some(String::from("a_value")), None];
    assert_eq!(Ok(expect), res);

    assert_eq!(Ok(Some(vec![String::from("a_key")])), db.hkeys(&key));
    assert_eq!(Ok(Some(vec![String::from("a_value")])), db.hvalues(&key));
}

#[test]
#[ignore]
fn hash_len_and_field_del() {
    let key: String = String::from("key");
    let pairs: Vec<(String, String)> = vec![
        (String::from("a_key"), String::from("a_value")),
        (String::from("b_key"), String::from("b_value")),
    ];
    let mut db: KVDB = common::setup_common_one_key_hash(&key, &pairs);

    assert_eq!(Ok(Some(2)), db.hlen(&key));
    assert_eq!(Ok(Some(1)), db.hdel(&key, &pairs[0].0));
    assert_eq!(Ok(Some(1)), db.hlen(&key));

    let other_key: String = String::from("other_key");
    assert_eq!(Ok(None), db.hdel(&other_key, &pairs[0].0));
    assert_eq!(Ok(Some(1)), db.hlen(&key));
}
