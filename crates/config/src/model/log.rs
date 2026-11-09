use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LogConfig {
    /// Same logging owner: preparing, queued and currently written chunks.
    /// Chunks are Write calls, not complete log records. Default128000.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_pending_chunks: Option<usize>,
    /// Same owner payload bytes, including a dequeued/in-flight write.
    /// Default16MiB; data is allocated only after real admission.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_pending_bytes: Option<usize>,
    /// Actual queue capacity, output/path metadata and retained fault storage
    /// envelope; independent from payload, not a process RSS estimate.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_storage_bytes: Option<usize>,
    /// Actual declared output graphs, each with at most one open original.
    /// Rotation closes the old original before acquiring its successor.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_outputs: Option<usize>,
    /// Default minimum log level (trace, debug, info, warn, error).
    #[serde(default = "default_log_level")]
    pub level: String,
    /// File output targets.  Omit / empty array = stderr only.
    #[serde(default)]
    pub files: Vec<LogFileConfig>,
    /// Per-second rate limit (optional).  0 = unlimited.
    #[serde(default)]
    pub rate_limit: Option<LogRateLimit>,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            max_pending_chunks: None,
            max_pending_bytes: None,
            max_storage_bytes: None,
            max_outputs: None,
            level: default_log_level(),
            files: Vec::new(),
            rate_limit: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LogFileConfig {
    pub path: String,
    /// Per-file minimum level override.  Defaults to `log.level`.
    #[serde(default)]
    pub level: Option<String>,
    #[serde(default = "default_log_max_bytes")]
    pub max_bytes: u64,
    #[serde(default = "default_log_max_files")]
    pub max_files: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LogRateLimit {
    pub max_per_second: u64,
}

fn default_log_level() -> String {
    "info".to_owned()
}
fn default_log_max_bytes() -> u64 {
    10 * 1024 * 1024
}
fn default_log_max_files() -> usize {
    5
}

impl LogConfig {
    pub fn effective_max_pending_chunks(&self) -> usize {
        self.max_pending_chunks.unwrap_or(128_000)
    }
    pub fn effective_max_pending_bytes(&self) -> usize {
        self.max_pending_bytes.unwrap_or(16 * 1024 * 1024)
    }
    pub fn effective_max_storage_bytes(&self) -> usize {
        self.max_storage_bytes.unwrap_or(16 * 1024 * 1024)
    }
    pub fn effective_max_outputs(&self) -> usize {
        self.max_outputs.unwrap_or(128)
    }
    /// No arbitrary upper policy ceiling. The concrete owner also validates
    /// the actual representable Frame/output layouts before any allocation.
    pub fn validate_owner_limits(&self) -> Result<(), crate::ConfigError> {
        let fields = [
            ("max_pending_chunks", self.effective_max_pending_chunks()),
            ("max_pending_bytes", self.effective_max_pending_bytes()),
            ("max_storage_bytes", self.effective_max_storage_bytes()),
            ("max_outputs", self.effective_max_outputs()),
        ];
        for (name, value) in fields {
            if value == 0 {
                return Err(crate::ConfigError::InvalidRuntime(format!(
                    "runtime.log.{name} must be positive and representable"
                )));
            }
        }
        if self.files.len() > self.effective_max_outputs() {
            return Err(crate::ConfigError::InvalidRuntime(
                "runtime.log.files exceeds max_outputs".into(),
            ));
        }
        Ok(())
    }
}
