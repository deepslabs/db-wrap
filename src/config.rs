use rocksdb::Options;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct RocksdbOptions {
    // TODO add more options for better usage
    pub create_if_missing: bool,
    pub atomic_flush: bool,
    // default 2
    pub log_file_num: Option<usize>,
    // default 20M
    pub log_file_size: Option<usize>,
}

impl Default for RocksdbOptions {
    fn default() -> Self {
        RocksdbOptions {
            create_if_missing: true,
            atomic_flush: true,
            log_file_num: Some(2),
            log_file_size: Some(20 * 1000 * 1000)
        }
    }
}

impl From<RocksdbOptions> for Options {
    fn from(roc_opt: RocksdbOptions) -> Self {
        let mut opt = Options::default();
        opt.create_if_missing(roc_opt.create_if_missing);
        opt.set_atomic_flush(roc_opt.atomic_flush);
        opt.set_keep_log_file_num(roc_opt.log_file_num.unwrap_or(2));
        opt.set_max_log_file_size(roc_opt.log_file_size.unwrap_or(20 * 1000 * 1000));
        opt
    }
}
