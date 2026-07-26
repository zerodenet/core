use thiserror::Error;

pub type ConnectorResult<T> = Result<T, ConnectorError>;

#[derive(Debug, Error)]
pub enum ConnectorError {
    #[error("connector feature `{feature}` is disabled for `{sink_type}` event sink `{tag}`")]
    FeatureDisabled {
        feature: &'static str,
        sink_type: &'static str,
        tag: String,
    },
    #[error("failed to open jsonl event sink `{tag}` at `{path}`: {source}")]
    OpenJsonLineSink {
        tag: String,
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to open delivery outbox at `{path}`: {source}")]
    OpenOutbox {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("invalid delivery outbox journal at `{path}`: {message}")]
    InvalidOutbox { path: String, message: String },
    #[error(
        "delivery outbox at `{path}` paused to preserve disk space: {available_bytes} bytes available, {reserve_bytes} bytes reserved, {attempted_write_bytes} bytes requested"
    )]
    OutboxStorageReserve {
        path: String,
        available_bytes: u64,
        reserve_bytes: u64,
        attempted_write_bytes: u64,
    },
    #[error(
        "persistent state `{path}` is already owned by another Zero process (lock `{lock_path}`)"
    )]
    PersistentStateInUse { path: String, lock_path: String },
    #[error("failed to acquire persistent state lock `{lock_path}` for `{path}`: {source}")]
    LockPersistentState {
        path: String,
        lock_path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("event dispatcher failed to start")]
    DispatcherStart,
    #[error("api error while building connector: {0}")]
    Api(#[from] zero_api::ApiError),
    #[error("connector config error: {0}")]
    Config(String),
}
