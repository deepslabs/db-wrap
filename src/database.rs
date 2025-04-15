use anyhow::{anyhow, bail, Result};
use parking_lot::RwLock;
use rocksdb::{Options, WriteBatch, DB};
use std::collections::HashMap;
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

impl DataLevel for &[u8] {}

impl DataLevel for Vec<u8> {}

impl DataLevel for &Vec<u8> {}

pub struct DbWrap {
    path: String,
    opt: Options,
    dbs: RwLock<HashMap<String, Arc<DB>>>,
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
        let path = self.path.clone() + "/" + path;
        let mut dbs = self.dbs.write();
        let db = match dbs.get(&path) {
            Some(db) => db.clone(),
            None => match DB::open(&self.opt, &path) {
                Ok(db) => {
                    let db = Arc::new(db);
                    dbs.insert(path.into(), db.clone());
                    db
                }
                Err(e) => bail!("{}", e.to_string()),
            },
        };

        Ok(db)
    }

    pub fn flush(&self, path: &str) -> Result<()> {
        self.db(path)?.flush()?;
        Ok(())
    }

    pub fn get<K: AsRef<[u8]>>(&self, k: K, path: &str) -> Result<Option<Vec<u8>>> {
        let db = self.db(path)?;
        let value = match db.get(k) {
            Ok(value) => value,
            Err(e) => {
                bail!("database get error: {e:?}");
            }
        };
        match value {
            Some(v) => {
                let v = v.data_without_lv();
                Ok(Some(v))
            }
            None => Ok(None),
        }
    }

    pub fn put<K: AsRef<[u8]>>(&self, k: K, v: Vec<u8>, lv: u8, force: bool, path: &str) -> Result<()> {
        let db = self.db(path)?;
        match db.get(&k) {
            Ok(Some(old)) => {
                if old.data_lv() < lv && !force {
                    bail!("can't put data with level {lv} which exist with DataLevel {} without force", old.data_lv());
                }
            }
            Err(e) => {
                bail!("database put-check error: {:?}", e);
            }
            Ok(None) => (),
        };
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
            match db.get(&k) {
                Ok(Some(old)) => {
                    if old.data_lv() < lv && !force {
                        bail!("can't put data with level {lv} which exist with DataLevel {} without force", old.data_lv());
                    }
                }
                Err(e) => {
                    bail!("database put-check error: {:?}", e);
                }
                Ok(None) => (),
            };
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
        let keys = self.search_keys_by_prefix(&k, &db);
        let mut datas = vec![];
        if !keys.is_empty() {
            for key in keys {
                match db.get(&key) {
                    Ok(Some(data)) => datas.push((key.clone(), data.data_without_lv())),
                    Ok(None) => (),
                    Err(e) => bail!("database get key error: {e:?}"),
                }
            }
        }
        Ok(datas)
    }

    pub fn delete_prefix<K: AsRef<[u8]>>(&self, k: K, path: &str) -> Result<()> {
        let db = self.db(path)?;
        let keys = self.search_keys_by_prefix(&k, &db);

        if !keys.is_empty() {
            for key in keys {
                db.delete(&key)?;
            }
        }
        Ok(())
    }

    fn search_keys_by_prefix<K: AsRef<[u8]>>(&self, prefix: K, db: &Arc<DB>) -> Vec<Vec<u8>> {
        let mut keys = Vec::new();
        let mut prev_iter = db.raw_iterator();
        prev_iter.seek(&prefix);
        while prev_iter.valid() {
            match prev_iter.key() {
                Some(key_slice) => {
                    if is_prefix(prefix.as_ref(), key_slice) {
                        keys.push(key_slice.to_vec());
                    }
                }
                None => (),
            }
            prev_iter.next();
        }
        keys
    }
}

pub fn is_prefix(prefix: &[u8], key: &[u8]) -> bool {
    if key.len() < prefix.len() {
        return false;
    }
    for i in 0..prefix.len() {
        if key[i] != prefix[i] {
            return false;
        }
    }
    true
}
