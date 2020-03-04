use dbcore::KVDB;
#[cfg(test)]
pub fn setup(key_size: Option<usize>) -> KVDB {
    KVDB::new(key_size)
}