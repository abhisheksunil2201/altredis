use crate::rdb::{DataType, RdbReader};
use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::env::args;
use std::path::Path;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::RwLock;

pub struct Value {
    pub value: DataType,
    pub expiry: Option<SystemTime>,
}

static CACHE: Lazy<Arc<RwLock<HashMap<usize, Database>>>> = Lazy::new(|| {
    let mut databases = HashMap::new();
    for i in 0..16 {
        databases.insert(i, Database::new());
    }
    Arc::new(RwLock::new(databases))
});
type Database = HashMap<String, Value>;

pub struct Config {
    pub dir: Option<String>,
    pub dbfilename: Option<String>,
    pub port: u16,
}

impl Config {
    pub fn new() -> Self {
        Self {
            dir: None,
            dbfilename: None,
            port: 6379,
        }
    }

    pub async fn set_from_args(&mut self) {
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

pub async fn db_load(db_file: impl AsRef<Path>) -> Result<(), anyhow::Error> {
    let mut cache = CACHE.write().await;
    cache.clear();

    let data = match RdbReader::read(db_file).await {
        Ok(r) => r,
        Err(e) => {
            println!("Failed to open database - {:?}", e);
            return Ok(());
        }
    };

    for (id, map) in data.databases {
        let expirations = data.expirations.get(&id);
        let remapped = map
            .into_iter()
            .map(|(k, v)| {
                let expiry = if let Some(expirations) = expirations {
                    expirations.get(&k).cloned()
                } else {
                    None
                };

                (k, Value { expiry, value: v })
            })
            .collect();
        cache.insert(id, remapped);
    }

    Ok(())
}

pub async fn db_get(db_id: usize, key: &String) -> Result<Option<DataType>, anyhow::Error> {
    let (result, should_remove) = {
        let cache = CACHE.read().await;
        if let Some(database) = cache.get(&db_id) {
            let mut is_valid = true;

            if let Some(entry) = database.get(key) {
                if let Some(expiration) = entry.expiry.as_ref() {
                    if *expiration < SystemTime::now() {
                        is_valid = false;
                    }
                }

                if is_valid {
                    (Some(entry.value.clone()), false)
                } else {
                    (None, true)
                }
            } else {
                (None, false)
            }
        } else {
            (None, false)
        }
    };

    if should_remove {
        let mut cache = CACHE.write().await;
        let database = cache.get_mut(&db_id).unwrap();
        database.remove(key);
    }

    Ok(result)
}

pub async fn db_set(db_id: usize, key: String, value: Value) -> Result<(), anyhow::Error> {
    let mut cache = CACHE.write().await;
    if let Some(database) = cache.get_mut(&db_id) {
        let entry = Value {
            value: value.value,
            expiry: value.expiry,
        };
        database.insert(key, entry);
    }

    Ok(())
}
