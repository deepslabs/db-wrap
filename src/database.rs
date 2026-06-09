use anyhow::{anyhow, bail, Result};
use parking_lot::RwLock;
use rocksdb::{Options, WriteBatch, DB};
use std::collections::HashMap;
use std::path::{Component, Path};
use std::sync::Arc;

const DEFAULT_LEVEL: u8 = 3;
const SEG: u8 = '\n' as u8;

pub trait DataLevel
where
    Self: AsRef<[u8]>,
{
    fn data_lv(&self) -> u8 {
        let d = self.as_ref();
        if d.len() < 3 {
            DEFAULT_LEVEL
        } else if d[0] == SEG && d[2] == SEG {
            d[1]
        } else {
            DEFAULT_LEVEL
        }
    }

    fn data_with_lv(&self, lv: u8) -> Vec<u8> {
        vec![[SEG, lv, SEG].as_ref(), self.as_ref()].concat()
    }

    fn data_without_lv(&self) -> Vec<u8> {
        let d = self.as_ref();
        if d.len() < 3 {
            d.to_vec()
        } else if d[0] == SEG && d[2] == SEG {
            d[3..].to_vec()
        } else {
            d.to_vec()
        }
    }
}

impl<T: AsRef<[u8]>> DataLevel for T {}

pub struct DbWrap {
    path: String,
    opt: Options,
    dbs: RwLock<HashMap<String, Arc<DB>>>,
}

/// Check that an existing value's level does not prevent an overwrite.
fn check_level(old: &[u8], new_lv: u8, force: bool) -> Result<()> {
    if old.data_lv() < new_lv && !force {
        bail!(
            "can't put data with level {} which exists with level {} without force",
            new_lv,
            old.data_lv()
        );
    }
    Ok(())
}

impl DbWrap {
    pub fn new(path: &str, opt: Options) -> Self {
        DbWrap {
            path: path.to_string(),
            opt,
            dbs: RwLock::new(HashMap::new()),
        }
    }

    pub fn db(&self, path: &str) -> Result<Arc<DB>> {
        // Prevent path traversal: reject absolute paths and `..` components
        for component in Path::new(path).components() {
            match component {
                Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                    bail!("invalid db path: {}", path);
                }
                _ => {}
            }
        }
        let full_path = self.path.clone() + "/" + path;

        // Fast path: read lock — avoids write contention when DB is already open
        {
            let dbs = self.dbs.read();
            if let Some(db) = dbs.get(&full_path) {
                return Ok(db.clone());
            }
        }

        // Slow path: write lock — double-checked to handle concurrent openers
        let mut dbs = self.dbs.write();
        if let Some(db) = dbs.get(&full_path) {
            return Ok(db.clone());
        }
        let db = Arc::new(DB::open(&self.opt, &full_path)?);
        dbs.insert(full_path, db.clone());
        Ok(db)
    }

    pub fn flush(&self, path: &str) -> Result<()> {
        self.db(path)?.flush()?;
        Ok(())
    }

    pub fn get<K: AsRef<[u8]>>(&self, k: K, path: &str) -> Result<Option<Vec<u8>>> {
        let db = self.db(path)?;
        match db.get(k)? {
            Some(v) => Ok(Some(v.data_without_lv())),
            None => Ok(None),
        }
    }

    pub fn put<K: AsRef<[u8]>>(&self, k: K, v: Vec<u8>, lv: u8, force: bool, path: &str) -> Result<()> {
        let db = self.db(path)?;
        if let Ok(Some(old)) = db.get(&k) {
            check_level(&old, lv, force)?;
        }
        db.put(k, &v.data_with_lv(lv))?;
        Ok(())
    }

    pub fn put_batch<K: AsRef<[u8]>>(
        &self,
        pairs: Vec<(K, Vec<u8>)>,
        lv: u8,
        force: bool,
        path: &str,
    ) -> Result<()> {
        let db = self.db(path)?;
        let mut batch = WriteBatch::default();
        for (k, v) in pairs {
            if let Ok(Some(old)) = db.get(&k) {
                check_level(&old, lv, force)?;
            }
            batch.put(k, &v.data_with_lv(lv));
        }
        db.write(batch).map_err(|e| anyhow!("{:?}", e))?;
        Ok(())
    }

    pub fn delete<K: AsRef<[u8]>>(&self, k: K, path: &str) -> Result<()> {
        let db = self.db(path)?;
        db.delete(k)?;
        Ok(())
    }

    pub fn delete_batch<K: AsRef<[u8]>>(&self, keys: Vec<K>, path: &str) -> Result<()> {
        let db = self.db(path)?;
        let mut batch = WriteBatch::default();
        for key in &keys {
            batch.delete(key);
        }
        db.write(batch).map_err(|e| anyhow!("{:?}", e))?;
        Ok(())
    }

    pub fn get_prefix<K: AsRef<[u8]>>(&self, k: K, path: &str) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let db = self.db(path)?;
        let keys = search_keys_by_prefix(k.as_ref(), &db)?;
        let mut datas = vec![];
        for key in keys {
            match db.get(&key) {
                Ok(Some(data)) => datas.push((key, data.data_without_lv())),
                Ok(None) => (),
                Err(e) => bail!("database get key error: {e:?}"),
            }
        }
        Ok(datas)
    }

    pub fn delete_prefix<K: AsRef<[u8]>>(&self, k: K, path: &str) -> Result<()> {
        let db = self.db(path)?;
        let keys = search_keys_by_prefix(k.as_ref(), &db)?;
        if !keys.is_empty() {
            let mut batch = WriteBatch::default();
            for key in &keys {
                batch.delete(key);
            }
            db.write(batch).map_err(|e| anyhow!("{:?}", e))?;
        }
        Ok(())
    }
}

/// Collect all keys from `db` whose value starts with `prefix`.
fn search_keys_by_prefix(prefix: &[u8], db: &DB) -> Result<Vec<Vec<u8>>> {
    let mut keys = Vec::new();
    let mut iter = db.raw_iterator();
    iter.seek(prefix);
    while iter.valid() {
        match iter.key() {
            Some(key_slice) if key_slice.starts_with(prefix) => {
                keys.push(key_slice.to_vec());
            }
            _ => break,
        }
        iter.next();
    }
    iter.status().map_err(|e| anyhow!("iterator error: {:?}", e))?;
    Ok(keys)
}
