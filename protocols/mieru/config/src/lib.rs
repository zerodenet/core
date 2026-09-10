//! Protocol-owned configuration and validation, without Mieru data-plane dependencies.
#![no_std]

extern crate alloc;
use alloc::{string::String, vec::Vec};
use core::fmt;
use serde::{Deserialize, Serialize};

pub const DEFAULT_MTU: u16 = 1400;
pub const MIN_MTU: u16 = 1280;
pub const MAX_MTU: u16 = 1500;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MieruTransportOptions {
    #[serde(default = "default_mtu")]
    pub mtu: u16,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub traffic_pattern: Option<MieruTrafficPatternConfig>,
    #[serde(default)]
    pub receive: MieruReceivePolicy,
}
const fn default_mtu() -> u16 {
    DEFAULT_MTU
}
impl Default for MieruTransportOptions {
    fn default() -> Self {
        Self {
            mtu: DEFAULT_MTU,
            traffic_pattern: None,
            receive: Default::default(),
        }
    }
}
impl MieruTransportOptions {
    pub fn validate(&self) -> Result<(), ValidationError> {
        if !(MIN_MTU..=MAX_MTU).contains(&self.mtu) {
            return Err(ValidationError("mieru mtu must be between 1280 and 1500"));
        }
        if let Some(pattern) = &self.traffic_pattern {
            pattern.validate()?;
        }
        self.receive.validate()?;
        Ok(())
    }
    /// Conservative IPv6 + UDP + nonce + metadata/tag + payload/tag overhead.
    /// A configured MTU denotes the outer IP packet, not only its UDP payload.
    pub fn udp_payload_limit(&self, ipv6: bool) -> usize {
        (self.mtu as usize).saturating_sub(if ipv6 { 48 } else { 28 })
    }
    pub fn fragment_size(&self, ipv6: bool) -> usize {
        self.udp_payload_limit(ipv6).saturating_sub(88)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct MieruReceivePolicy {
    pub stall_timeout_ms: u64,
    pub max_pending_bytes: usize,
    pub max_pending_frames: usize,
    pub max_connection_pending_bytes: usize,
}
impl Default for MieruReceivePolicy {
    fn default() -> Self {
        Self {
            stall_timeout_ms: 30_000,
            max_pending_bytes: 512 * 1024,
            max_pending_frames: 64,
            max_connection_pending_bytes: 4 * 1024 * 1024,
        }
    }
}
impl MieruReceivePolicy {
    pub fn validate(&self) -> Result<(), ValidationError> {
        if !(1_000..=300_000).contains(&self.stall_timeout_ms) {
            return Err(ValidationError(
                "mieru receive.stall_timeout_ms must be between 1000 and 300000",
            ));
        }
        if !(1..=16 * 1024 * 1024).contains(&self.max_pending_bytes)
            || !(1..=4096).contains(&self.max_pending_frames)
            || self.max_connection_pending_bytes < self.max_pending_bytes
            || self.max_connection_pending_bytes > 64 * 1024 * 1024
        {
            return Err(ValidationError("mieru receive buffers must have positive bounded per-session and connection limits"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MieruTrafficPatternConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seed: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unlock_all: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tcp_fragment: Option<MieruTcpFragmentConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nonce: Option<MieruNoncePatternConfig>,
}
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MieruTcpFragmentConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enable: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_sleep_ms: Option<u16>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MieruNonceType {
    Random,
    Printable,
    PrintableSubset,
    Fixed,
}
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MieruNoncePatternConfig {
    #[serde(rename = "type", default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<MieruNonceType>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub apply_to_all_udp_packet: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_len: Option<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_len: Option<u8>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub custom_hex_strings: Vec<String>,
}
impl MieruTrafficPatternConfig {
    pub fn validate(&self) -> Result<(), ValidationError> {
        if self
            .tcp_fragment
            .as_ref()
            .and_then(|f| f.max_sleep_ms)
            .is_some_and(|ms| ms > 100)
        {
            return Err(ValidationError(
                "mieru tcp_fragment.max_sleep_ms must not exceed 100",
            ));
        }
        if let Some(nonce) = &self.nonce {
            if nonce.min_len.is_some_and(|n| n > 12) || nonce.max_len.is_some_and(|n| n > 12) {
                return Err(ValidationError("mieru nonce lengths must not exceed 12"));
            }
            if matches!((nonce.min_len, nonce.max_len), (Some(min), Some(max)) if min > max) {
                return Err(ValidationError(
                    "mieru nonce min_len must not exceed max_len",
                ));
            }
            for hex in &nonce.custom_hex_strings {
                if hex.len() > 24
                    || hex.len() % 2 != 0
                    || !hex.bytes().all(|b| b.is_ascii_hexdigit())
                {
                    return Err(ValidationError(
                        "mieru nonce prefix must be valid hex of at most 12 bytes",
                    ));
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ValidationError(pub &'static str);
impl fmt::Display for ValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}
impl core::error::Error for ValidationError {}
