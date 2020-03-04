/// 声明测试通用模块
mod common;

use dbcore::{DBError, DBOk, KVDB};

#[test]
#[ignore]
fn keys_size() {
    let mut db: KVDB = common::setup(Some(1));
    let key = String::from("key");
    let value = String::from("value");
    assert_eq!(
        Ok(DBOk::Ok),
        db.set(&key, value.clone(), None, false, false)
    );

    let key2 = String::from("key2");
    let value2 = String::from("value2");
    assert_eq!(
        Err(DBError::OutOfKeysSize),
        db.set(&key2, value2, None, false, false)
    );

    assert_eq!(Ok(Some(value)), db.get(&key));
    assert_eq!(Ok(None), db.get(&key2));
}

#[test]
#[ignore]
fn set_one() {
    let mut db: KVDB = common::setup(None);
    let key = String::from("key");
    let value = String::from("value");
    assert_eq!(
        Ok(DBOk::Ok),
        db.set(&key, value.clone(), None, false, false)
    );

    assert_eq!(Ok(Some(value)), db.get(&key));
}

#[test]
#[ignore]
fn set_ten_same_key_diff_value() {
    let mut db: KVDB = common::setup(None);
    let key: String = String::from("key");
    let mut last: u32 = 0;
    for x in 0..10 {
        last = x;
        assert_eq!(
            Ok(DBOk::Ok),
            db.set(&key, x.to_string(), None, false, false)
        );
    }
    assert_eq!(Ok(Some(last.to_string())), db.get(&key));
}

#[test]
#[ignore]
fn set_ten() {
    let mut db: KVDB = common::setup(None);
    for x in 0..10 {
        assert_eq!(
            Ok(DBOk::Ok),
            db.set(&x.to_string(), x.to_string(), None, false, false)
        );
    }
    for x in 0..10 {
        assert_eq!(Ok(Some(x.to_string())), db.get(&x.to_string()));
    }
}

#[test]
#[ignore]
fn set_not_exists() {
    let not_exists = true;
    let mut db: KVDB = common::setup(None);
    let key: String = String::from("key");
    for x in 0..10 {
        if x == 0 {
            assert_eq!(
                Ok(DBOk::Ok),
                db.set(&key, x.to_string(), None, not_exists, false)
            );
        } else {
            assert_eq!(Ok(Some(0.to_string())), db.get(&key));
            assert_eq!(
                Ok(DBOk::Nil),
                db.set(&key, x.to_string(), None, not_exists, false)
            );
        }
    }
    assert_eq!(Ok(Some(0.to_string())), db.get(&key));
}
