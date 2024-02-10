use std::collections::HashMap;
use std::time::SystemTime;

pub struct Value {
    pub value: String,
    pub expiry: Option<SystemTime>,
}
pub struct Database {
    db: HashMap<String, Value>,
}

impl Database {
    pub fn new() -> Self {
        Database { db: HashMap::new() }
    }
    pub fn set(&mut self, key: String, value: Value) {
        self.db.insert(
            key,
            Value {
                value: value.value,
                expiry: value.expiry,
            },
        );
    }
    fn delete_key_if_expired(&mut self, key: &str) {
        let val = self.db.get(key);
        if let Some(t) = val.and_then(|v| v.expiry) {
            if t <= SystemTime::now() {
                self.db.remove(key);
            }
        }
    }
    pub fn get(&mut self, key: &str) -> Option<&String> {
        self.delete_key_if_expired(key);
        let val = self.db.get(key)?;
        Some(&val.value)
    }
}
