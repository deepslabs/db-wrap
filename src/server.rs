use crate::config::RocksdbOptions;
use crate::database::DbWrap;
use anyhow::{bail, Result};
use axum::{extract::State, routing::post, Json, Router};
use rocksdb::Options;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::sync::Arc;
use subtle::ConstantTimeEq;

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

#[derive(Clone, Serialize, Deserialize)]
pub struct HttpRequest {
    pub token: String,
    pub req: RequestType,
    pub path: String,
}

impl std::fmt::Debug for HttpRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpRequest")
            .field("token", &"[REDACTED]")
            .field("req", &self.req)
            .field("path", &self.path)
            .finish()
    }
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

/// Shared application state holding the database reference and auth token.
#[derive(Clone)]
struct AppState {
    db: Arc<DbWrap>,
    token: Arc<String>,
}

async fn db_request(
    State(state): State<AppState>,
    Json(request): Json<HttpRequest>,
) -> Json<Result<String, String>> {
    // Constant-time token comparison
    if state
        .token
        .as_bytes()
        .ct_eq(request.token.as_bytes())
        .unwrap_u8()
        == 0
    {
        return Json(Err("Invalid token".to_string()));
    }
    let path = request.path.clone();
    let req = request.req.clone();
    let db = state.db.clone();

    // Run blocking DB operations on a dedicated blocking thread
    let res = tokio::task::spawn_blocking(move || handle_request(&db, &req, &path))
        .await
        .unwrap_or(Err("Internal server error".to_string()));

    log::debug!(target: "database_server", "Handle Request: {:?}, Response: {res:?}", request);
    Json(res)
}

fn handle_request(db_ref: &DbWrap, req: &RequestType, path: &str) -> Result<String, String> {
    match req {
        RequestType::Get(key) => call_and_serialize(db_ref.get(key, path), "db_get"),
        RequestType::Put(key, value, level, force) => {
            call_and_serialize(db_ref.put(key, value.clone(), *level, *force, path), "db_put")
        }
        RequestType::PutBatch(pairs, level, force) => {
            call_and_serialize(db_ref.put_batch(pairs.clone(), *level, *force, path), "db_put_batch")
        }
        RequestType::Delete(key) => call_and_serialize(db_ref.delete(key, path), "db_delete"),
        RequestType::DeleteBatch(keys) => {
            call_and_serialize(db_ref.delete_batch(keys.clone(), path), "db_delete_batch")
        }
        RequestType::GetPrefix(key) => {
            call_and_serialize(db_ref.get_prefix(key, path), "db_get_prefix")
        }
        RequestType::DeletePrefix(key) => {
            call_and_serialize(db_ref.delete_prefix(key, path), "db_delete_prefix")
        }
    }
}

fn call_and_serialize<T: Serialize>(
    result: anyhow::Result<T>,
    label: &str,
) -> Result<String, String> {
    result
        .map_err(|e| format!("db_req failed for: {:?}", e))
        .and_then(|v| to_string(&v, label))
}

/// Start the Axum HTTP server.
///
/// Builds a multi-threaded tokio runtime using `num_workers` from the config
/// and blocks the calling thread.
pub fn mount_db_server(db_config: DbConfig) {
    let num_workers = db_config.num_workers as usize;
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(num_workers)
        .enable_all()
        .build()
        .expect("failed to build tokio runtime");

    rt.block_on(async {
        let db = DbWrap::new(&db_config.db_path, db_config.get_opt());
        let state = AppState {
            db: Arc::new(db),
            token: Arc::new(db_config.token.clone()),
        };

        let app = Router::new()
            .route("/db_request", post(db_request))
            .with_state(state);

        let addr = SocketAddr::from(([0, 0, 0, 0], db_config.port));
        log::info!(target: "database_server", "listening on {addr}");
        axum::Server::bind(&addr)
            .serve(app.into_make_service())
            .await
            .expect("server error");
    });
}
