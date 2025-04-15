#![allow(clippy::all)]

pub mod config;
pub mod database;

pub use config::*;
pub use database::*;
#[cfg(feature = "server")]
pub use db_server::*;
pub use rocksdb;

pub fn sha2_hash256(msg: &[u8]) -> Vec<u8> {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.input(msg);
    hasher.result()[..].to_vec()
}

pub fn bytes_to_hide_hex(bytes: &[u8], hl: usize, el: usize, padding: Option<&str>) -> String {
    let length = bytes.len();
    let hex_str = hex::encode(bytes);
    let mut pad_str = "...";
    if let Some(pad) = padding {
        pad_str = pad;
    }
    let mut hl = hl;
    let mut el = el;
    match hl + el {
        l if l == length => pad_str = "",
        l if l > length => {
            hl = length;
            el = 0;
            pad_str = "";
        }
        _ => (),
    }

    "0x".to_string() + &hex_str[..hl * 2] + pad_str + &hex_str[(length - el) * 2..]
}

pub fn debug_hash_data(data: &[u8]) -> String {
    bytes_to_hide_hex(&sha2_hash256(data), 6, 6, None)
}

#[cfg(feature = "server")]
pub mod db_server {
    use crate::config::RocksdbOptions;
    use crate::database::DbWrap;
    use anyhow::{bail, Result};
    use rocket::{post, routes, State};
    use rocket_contrib::json::Json;
    use rocksdb::Options;
    use serde::{Deserialize, Serialize};
    use std::sync::Arc;

    #[derive(Deserialize, Clone)]
    pub struct DbConfig {
        pub port: u16,
        pub num_workers: u16,
        pub db_path: String,
        pub token: String,
        pub options: Option<RocksdbOptions>,
    }

    impl DbConfig {
        pub fn get_opt(&self) -> Options {
            if let Some(opt) = self.options.clone() {
                opt.into()
            } else {
                Options::default()
            }
        }
    }

    pub fn load_db_server_config() -> Result<DbConfig> {
        let content = std::fs::read_to_string("db_config.toml")?;
        match toml::from_str::<DbConfig>(&content) {
            Ok(config) => Ok(config),
            Err(_) => bail!("failed to load config"),
        }
    }

    fn to_string<V: Serialize>(value: V, at: &str) -> Result<String, String> {
        serde_json::to_string(&value).map_err(|e| format!("{} serialize err: {:?}", at, e))
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct HttpRequest {
        pub token: String,
        pub req: RequestType,
        pub path: String,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub enum RequestType {
        Get(Vec<u8>),
        Put(Vec<u8>, Vec<u8>, u8, bool),
        PutBatch(Vec<(Vec<u8>, Vec<u8>)>, u8, bool),
        Delete(Vec<u8>),
        DeleteBatch(Vec<Vec<u8>>),
        GetPrefix(Vec<u8>),
        DeletePrefix(Vec<u8>),
    }

    #[post("/db_request", format = "json", data = "<request>")]
    fn db_request(
        db_ref: State<Arc<DbWrap>>,
        token: State<Arc<String>>,
        request: Json<HttpRequest>,
    ) -> Json<Result<String, String>> {
        if token.clone().as_str() != request.0.token.as_str() {
            return Json(Err("Invalid token".to_string()));
        }
        let path = request.0.path.clone();
        let res = match request.0.req {
            RequestType::Get(key) => match db_ref.get(key, &path) {
                Ok(res) => to_string(&res, "db_get"),
                Err(e) => Err(format!("db_req failed for: {:?}", e)),
            },
            RequestType::Put(key, value, level, force) => {
                match db_ref.put(key, value, level, force, &path) {
                    Ok(res) => to_string(&res, "db_put"),
                    Err(e) => Err(format!("db_req failed for: {:?}", e)),
                }
            }
            RequestType::PutBatch(pairs, level, force) => {
                match db_ref.put_batch(pairs, level, force, &path) {
                    Ok(res) => to_string(&res, "db_put_batch"),
                    Err(e) => Err(format!("db_req failed for: {:?}", e)),
                }
            }
            RequestType::Delete(key) => match db_ref.delete(key, &path) {
                Ok(res) => to_string(&res, "db_delete"),
                Err(e) => Err(format!("db_req failed for: {:?}", e)),
            },
            RequestType::DeleteBatch(keys) => match db_ref.delete_batch(keys, &path) {
                Ok(res) => to_string(&res, "db_delete_batch"),
                Err(e) => Err(format!("db_req failed for: {:?}", e)),
            },
            RequestType::GetPrefix(key) => match db_ref.get_prefix(key, &path) {
                Ok(res) => to_string(&res, "db_get_prefix"),
                Err(e) => Err(format!("db_req failed for: {:?}", e)),
            },
            RequestType::DeletePrefix(key) => match db_ref.delete_prefix(key, &path) {
                Ok(res) => to_string(&res, "db_delete_prefix"),
                Err(e) => Err(format!("db_req failed for: {:?}", e)),
            },
        };
        log::debug!(target: "database_server", "Handle Request: {:?}, Response: {res:?}", request.0);
        Json(res)
    }

    pub fn mount_db_server(db_config: DbConfig) {
        let db = DbWrap::new(&db_config.db_path, db_config.get_opt());
        let db_ref = Arc::new(db);
        let token = Arc::new(db_config.token.clone());
        /////////////////////////////////////////////////////////////////
        let mut rocket_config = rocket::Config::production();
        rocket_config.set_port(db_config.port);
        rocket_config.set_workers(db_config.num_workers);

        rocket::custom(rocket_config)
            .mount("/", routes![db_request])
            .manage(db_ref)
            .manage(token)
            .launch();
    }
}
