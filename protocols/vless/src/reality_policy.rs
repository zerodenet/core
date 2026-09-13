//! Protocol-owned admission policy for authenticated REALITY ClientHellos.
use alloc::{string::String, vec::Vec};

#[derive(Debug, Clone, Copy, Default)]
pub struct PolicyRef<'a> {
    pub server_names: &'a [String],
    pub min_client_version: Option<&'a str>,
    pub max_client_version: Option<&'a str>,
    pub max_time_diff_ms: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ServerPolicy {
    names: Vec<String>,
    minimum: Option<[u8; 3]>,
    maximum: Option<[u8; 3]>,
    time_diff_ms: u64,
}

pub fn version(value: &str) -> Result<[u8; 3], &'static str> {
    let mut version = [0; 3];
    if value.is_empty() {
        return Err("REALITY client version must not be empty");
    }
    for (index, part) in value.split('.').enumerate() {
        if index >= 3 || part.is_empty() || !part.bytes().all(|b| b.is_ascii_digit()) {
            return Err("REALITY client version requires one to three decimal bytes");
        }
        version[index] = part
            .parse()
            .map_err(|_| "REALITY client version component exceeds 255")?;
    }
    Ok(version)
}

impl ServerPolicy {
    pub fn for_name(name: Option<&str>) -> Self {
        Self {
            names: name.into_iter().map(String::from).collect(),
            ..Default::default()
        }
    }
    pub fn new(options: PolicyRef<'_>, legacy_name: Option<&str>) -> Result<Self, &'static str> {
        let minimum = options.min_client_version.map(version).transpose()?;
        let maximum = options.max_client_version.map(version).transpose()?;
        if minimum.zip(maximum).is_some_and(|(low, high)| low > high) {
            return Err("REALITY minimum client version exceeds maximum");
        }
        let names = if options.server_names.is_empty() {
            legacy_name.into_iter().map(String::from).collect()
        } else {
            options.server_names.to_vec()
        };
        if names.iter().any(|name| name.contains('*')) {
            return Err("REALITY server names must be exact names, without wildcards");
        }
        Ok(Self {
            names,
            minimum,
            maximum,
            time_diff_ms: options.max_time_diff_ms,
        })
    }

    /// Called only after authenticating the SessionId against the complete hello.
    pub fn accepts(&self, server_name: Option<&str>, session_id: &[u8; 16], now_ms: u128) -> bool {
        let version = [session_id[0], session_id[1], session_id[2]];
        let timestamp_ms =
            u128::from(u32::from_be_bytes(session_id[4..8].try_into().unwrap())) * 1000;
        (self.names.is_empty()
            || self
                .names
                .iter()
                .any(|name| name == server_name.unwrap_or("")))
            && self.minimum.is_none_or(|minimum| version >= minimum)
            && self.maximum.is_none_or(|maximum| version <= maximum)
            && (self.time_diff_ms == 0
                || now_ms.abs_diff(timestamp_ms) <= u128::from(self.time_diff_ms))
    }
}
