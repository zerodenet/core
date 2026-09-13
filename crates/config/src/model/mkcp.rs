use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct MkcpConfig {
    pub mtu: u32,
    pub tti_ms: u32,
    pub uplink_capacity_mib: u32,
    pub downlink_capacity_mib: u32,
    pub congestion: bool,
    pub write_buffer_bytes: u32,
}
impl Default for MkcpConfig {
    fn default() -> Self {
        Self {
            mtu: 1350,
            tti_ms: 50,
            uplink_capacity_mib: 5,
            downlink_capacity_mib: 20,
            congestion: false,
            write_buffer_bytes: 2 * 1024 * 1024,
        }
    }
}
impl MkcpConfig {
    pub(crate) fn validate(&self) -> Result<(), &'static str> {
        if !(19..=65507).contains(&self.mtu)
            || !(10..=5000).contains(&self.tti_ms)
            || self.write_buffer_bytes > 64 * 1024 * 1024
        {
            return Err("invalid mKCP MTU, TTI or write buffer");
        }
        Ok(())
    }
}
