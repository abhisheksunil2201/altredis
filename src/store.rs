use std::collections::HashMap;
use std::env::args;
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

pub struct Config {
    dir: Option<String>,
    dbfilename: Option<String>,
}

impl Config {
    pub fn new() -> Self {
        Config {
            dir: None,
            dbfilename: None,
        }
    }

    pub fn set_from_args(&mut self) {
        let args: Vec<String> = args().collect();
        let mut iter = args.iter();
        while let Some(arg) = iter.next() {
            match arg.to_lowercase().as_str() {
                "--dir" => {
                    self.dir = iter.next().map(|s| s.to_owned());
                }
                "--dbfilename" => {
                    self.dbfilename = iter.next().map(|s| s.to_owned());
                }
                _ => {}
            }
        }
    }

    pub fn get(&self, key: &str) -> Option<String> {
        match key {
            "dir" => self.dir.clone(),
            "dbfilename" => self.dbfilename.clone(),
            _ => None,
        }
    }
}
