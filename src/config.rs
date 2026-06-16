use rocksdb::{Options, DBRecoveryMode};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct RocksdbOptions {
    pub create_if_missing: bool,
    pub atomic_flush: bool,
    // default 2
    pub log_file_num: Option<usize>,
    // default 20M
    pub log_file_size: Option<usize>,
    // Use fsync instead of fdatasync when flushing writes to disk.
    // Critical in Occlum/SGX environments where data must survive
    // multiple buffering layers (PAL → host filesystem).
    pub use_fsync: Option<bool>,
    // WAL TTL in seconds. When > 0, RocksDB will try to clean up
    // WAL files older than this. 0 = never auto-delete (safest).
    pub wal_ttl_seconds: Option<u64>,
    // WAL size limit in MB. When > 0, RocksDB will try to limit
    // total WAL size to this amount. 0 = no limit (safest).
    pub wal_size_limit_mb: Option<u64>,
    // WAL recovery mode. Valid values:
    //   "TolerateCorruptedTailRecords" (0)
    //   "AbsoluteConsistency" (1)
    //   "PointInTime" (2) — RocksDB default
    //   "SkipAnyCorruptedRecord" (3)
    // In SGX environments, "AbsoluteConsistency" is recommended.
    pub wal_recovery_mode: Option<String>,
    // When true, disable automatic WAL flush after writes.
    // You must call flush_wal() manually to durably persist writes.
    pub manual_wal_flush: Option<bool>,
}

fn parse_recovery_mode(mode: Option<&str>) -> DBRecoveryMode {
    match mode.map(|s| s.trim()) {
        Some("0") | Some("TolerateCorruptedTailRecords") => DBRecoveryMode::TolerateCorruptedTailRecords,
        Some("1") | Some("AbsoluteConsistency") => DBRecoveryMode::AbsoluteConsistency,
        Some("3") | Some("SkipAnyCorruptedRecord") => DBRecoveryMode::SkipAnyCorruptedRecord,
        // Default is PointInTime (RocksDB's default)
        _ => DBRecoveryMode::PointInTime,
    }
}

impl Default for RocksdbOptions {
    fn default() -> Self {
        RocksdbOptions {
            create_if_missing: true,
            atomic_flush: true,
            log_file_num: Some(2),
            log_file_size: Some(20 * 1000 * 1000),
            // Durability defaults — safe for SGX/Occlum
            use_fsync: Some(true),
            wal_ttl_seconds: Some(0),
            wal_size_limit_mb: Some(0),
            wal_recovery_mode: Some("AbsoluteConsistency".to_string()),
            manual_wal_flush: Some(false),
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

        // Durability settings
        if roc_opt.use_fsync.unwrap_or(true) {
            opt.set_use_fsync(true);
        }
        let wal_ttl = roc_opt.wal_ttl_seconds.unwrap_or(0);
        if wal_ttl > 0 {
            opt.set_wal_ttl_seconds(wal_ttl);
        }
        let wal_limit = roc_opt.wal_size_limit_mb.unwrap_or(0);
        if wal_limit > 0 {
            opt.set_wal_size_limit_mb(wal_limit);
        }
        opt.set_wal_recovery_mode(parse_recovery_mode(
            roc_opt.wal_recovery_mode.as_deref(),
        ));
        if roc_opt.manual_wal_flush.unwrap_or(false) {
            opt.set_manual_wal_flush(true);
        }

        opt
    }
}
