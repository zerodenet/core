use serde::{Deserialize, Serialize};

/// Declarative TUN runtime configuration.
///
/// Presence of `runtime.tun` enables the TUN inbound for the lifetime of the
/// proxy. Omitting it leaves TUN under explicit control-plane commands.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TunConfig {
    #[serde(default)]
    pub name: Option<String>,
    pub addr: String,
    #[serde(default = "default_tun_mask")]
    pub mask: String,
    /// Optional address for the other IP family when `dual_stack` is enabled.
    /// This value must use CIDR notation. When omitted, Zero uses its reserved
    /// TUN-local default (`10.66.0.1/24` or `fd66::1/64`).
    #[serde(default)]
    pub secondary_addr: Option<String>,
    /// Optional TUN-local override for `runtime.network.mtu`.
    #[serde(default)]
    pub mtu: Option<u16>,
    #[serde(default = "default_tun_tag")]
    pub tag: String,
    #[serde(default = "default_true")]
    pub auto_route: bool,
    /// Install both IPv4 and IPv6 split-default routes. Disable only on a
    /// deliberately single-stack host.
    #[serde(default = "default_true")]
    pub dual_stack: bool,
    #[serde(default = "default_true")]
    pub strict_route: bool,
    #[serde(default = "default_true")]
    pub dns_hijack: bool,
}

impl TunConfig {
    pub fn effective_mtu(&self, network_mtu: u16) -> u16 {
        self.mtu.unwrap_or(network_mtu)
    }
}

fn default_true() -> bool {
    true
}

fn default_tun_mask() -> String {
    "255.255.255.0".to_owned()
}

fn default_tun_tag() -> String {
    "tun".to_owned()
}
