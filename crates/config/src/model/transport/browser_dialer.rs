use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct BrowserDialerConfig {
    pub listen: String,
    pub idle_capacity: u32,
    pub task_timeout_ms: u64,
    pub max_task_bytes: u32,
    pub max_payload_bytes: u32,
}

impl Default for BrowserDialerConfig {
    fn default() -> Self {
        Self {
            listen: "127.0.0.1:16888".into(),
            idle_capacity: 64,
            task_timeout_ms: 30_000,
            max_task_bytes: 64 * 1024,
            max_payload_bytes: 16 * 1024 * 1024,
        }
    }
}

impl BrowserDialerConfig {
    pub fn settings(&self) -> zero_traits::BrowserDialerSettings {
        zero_traits::BrowserDialerSettings {
            listen: self.listen.clone(),
            idle_capacity: self.idle_capacity,
            task_timeout_ms: self.task_timeout_ms,
            max_task_bytes: self.max_task_bytes,
            max_payload_bytes: self.max_payload_bytes,
        }
    }

    pub(crate) fn validate(&self) -> Result<(), &'static str> {
        let Ok(address) = self.listen.parse::<std::net::SocketAddr>() else {
            return Err("browser_dialer.listen must be an IP socket address");
        };
        if !address.ip().is_loopback() || address.port() == 0 {
            return Err("browser_dialer.listen must use loopback and a nonzero port");
        }
        if !(1..=4096).contains(&self.idle_capacity)
            || self.task_timeout_ms == 0
            || self.max_task_bytes == 0
            || self.max_payload_bytes == 0
        {
            return Err("browser_dialer resource limits must be nonzero and capacity <= 4096");
        }
        Ok(())
    }
}
