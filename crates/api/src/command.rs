use serde::{Deserialize, Serialize};

use crate::Permission;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "method", content = "params")]
pub enum CommandRequest {
    #[serde(rename = "config.validate")]
    ConfigValidate(ConfigValidateCommand),
    #[serde(rename = "config.apply")]
    ConfigApply(ConfigApplyCommand),
    #[serde(rename = "config.apply_runtime")]
    ConfigApplyRuntime(ConfigApplyCommand),
    #[serde(rename = "flows.close")]
    FlowClose(FlowCloseCommand),
    #[serde(rename = "policies.select")]
    PolicySelect(PolicySelectCommand),
    #[serde(rename = "policies.probe")]
    PolicyProbe(PolicyProbeCommand),
    #[serde(rename = "diagnostics.probe_target")]
    DiagnosticsProbeTarget(DiagnosticsProbeTargetCommand),
    #[serde(rename = "diagnostics.probe_outbound")]
    DiagnosticsProbeOutbound(DiagnosticsProbeOutboundCommand),
    #[serde(rename = "diagnostics.dns_lookup")]
    DiagnosticsDnsLookup(DiagnosticsDnsLookupCommand),
    #[serde(rename = "diagnostics.dns_cache")]
    DiagnosticsDnsCache(DiagnosticsDnsCacheCommand),
    #[serde(rename = "diagnostics.fakeip_lookup")]
    DiagnosticsFakeipLookup(DiagnosticsFakeipLookupCommand),
    #[serde(rename = "diagnostics.trace_route")]
    DiagnosticsTraceRoute(DiagnosticsTraceRouteCommand),
    #[serde(rename = "mode.set")]
    ModeSet(ModeSetCommand),
    #[serde(rename = "tun.start")]
    TunStart(TunStartCommand),
    #[serde(rename = "tun.stop")]
    TunStop(TunStopCommand),
}

impl CommandRequest {
    pub fn required_permission(&self) -> Permission {
        match self {
            Self::ConfigValidate(_) | Self::ConfigApply(_) | Self::ConfigApplyRuntime(_) => {
                Permission::Config
            }
            Self::FlowClose(_) | Self::PolicySelect(_) | Self::PolicyProbe(_) => {
                Permission::Control
            }
            Self::DiagnosticsProbeTarget(_)
            | Self::DiagnosticsProbeOutbound(_)
            | Self::DiagnosticsDnsLookup(_)
            | Self::DiagnosticsDnsCache(_)
            | Self::DiagnosticsFakeipLookup(_)
            | Self::DiagnosticsTraceRoute(_)
            | Self::ModeSet(_)
            | Self::TunStart(_)
            | Self::TunStop(_) => Permission::Admin,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CommandResponse {
    #[serde(default)]
    pub accepted: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,
}

impl CommandResponse {
    pub fn accepted() -> Self {
        Self {
            accepted: true,
            result: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConfigValidateCommand {
    pub config: serde_json::Value,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlowCloseCommand {
    #[serde(default)]
    pub flow_id: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolicySelectCommand {
    #[serde(default)]
    pub policy_tag: String,
    #[serde(default)]
    pub target_tag: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolicyProbeCommand {
    #[serde(default)]
    pub policy_tag: String,
    /// Optional caller-supplied correlation identity. An empty value is
    /// treated as absent and the core generates an operation identity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigApplyCommand {
    #[serde(default)]
    pub config: serde_json::Value,
}

impl Default for ConfigApplyCommand {
    fn default() -> Self {
        Self {
            config: serde_json::Value::Null,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiagnosticsProbeTargetCommand {
    #[serde(default)]
    pub target_tag: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation_id: Option<String>,
}

/// Probe a single outbound **through the proxy** (full TLS + protocol
/// handshake, then an HTTP HEAD to `url`, measuring time to first byte).
///
/// Unlike [`DiagnosticsProbeTargetCommand`] (direct TCP reachability) and
/// `policies.probe` (async, group-level), this is **synchronous** and
/// measures a single node's end-to-end proxy latency — what a GUI needs for
/// "tap one node to test it".
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiagnosticsProbeOutboundCommand {
    /// Tag of the outbound (leaf or fallback group) to probe.
    #[serde(default)]
    pub target_tag: String,
    /// Legacy command-level URL override. `runtime.latency_test_url` takes
    /// precedence when configured; otherwise this value is used before the
    /// built-in default.
    #[serde(default)]
    pub url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation_id: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModeSetCommand {
    /// One of: "rule", "global", "direct"
    #[serde(default)]
    pub mode: String,
    /// Required when mode is "global" — the outbound tag to route all traffic to.
    #[serde(default)]
    pub outbound: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiagnosticsDnsLookupCommand {
    #[serde(default)]
    pub hostname: String,
}

/// Inspect the DNS resolver cache (`diagnostics.dns_cache`).
///
/// With `domain`, reports whether that domain is cached plus its addresses
/// and remaining TTL. Without `domain`, lists live cache entries (capped).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiagnosticsDnsCacheCommand {
    #[serde(default)]
    pub domain: Option<String>,
    #[serde(default)]
    pub limit: Option<usize>,
}

/// Query the fake-IP mapping (`diagnostics.fakeip_lookup`).
///
/// Exactly one of `domain` (forward: domain → fake IP, no allocation) or
/// `ip` (reverse: fake IP → domain) should be set.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiagnosticsFakeipLookupCommand {
    #[serde(default)]
    pub domain: Option<String>,
    #[serde(default)]
    pub ip: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiagnosticsTraceRouteCommand {
    #[serde(default)]
    pub target: String,
    #[serde(default)]
    pub port: u16,
    #[serde(default)]
    pub protocol: Option<String>,
    #[serde(default)]
    pub inbound_tag: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TunStartCommand {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub addr: String,
    /// Optional command-level override. When omitted, the runtime uses
    /// `runtime.network.mtu` from the active configuration.
    #[serde(default)]
    pub mtu: Option<u16>,
    #[serde(default = "default_tun_mask")]
    pub mask: String,
    /// Optional CIDR address for the other IP family in dual-stack mode.
    #[serde(default)]
    pub secondary_addr: Option<String>,
    #[serde(default)]
    pub tag: String,
    /// Install split default routes through the TUN device.
    #[serde(default = "default_true")]
    pub auto_route: bool,
    /// Destination CIDRs captured by automatic TUN routes. Empty means full tunnel.
    #[serde(default)]
    pub include_cidrs: Vec<String>,
    /// Destination CIDRs removed from the automatic capture plan.
    #[serde(default)]
    pub exclude_cidrs: Vec<String>,
    /// Install split-default routes for both IPv4 and IPv6.
    #[serde(default = "default_true")]
    pub dual_stack: bool,
    /// Fail startup and roll back if automatic route installation fails.
    #[serde(default = "default_true")]
    pub strict_route: bool,
    /// Intercept UDP/TCP port 53 from the TUN stack and answer through Zero DNS.
    #[serde(default = "default_true")]
    pub dns_hijack: bool,
}

impl Default for TunStartCommand {
    fn default() -> Self {
        Self {
            name: None,
            addr: String::new(),
            mtu: None,
            mask: default_tun_mask(),
            secondary_addr: None,
            tag: String::new(),
            auto_route: true,
            include_cidrs: Vec::new(),
            exclude_cidrs: Vec::new(),
            dual_stack: true,
            strict_route: true,
            dns_hijack: true,
        }
    }
}

fn default_true() -> bool {
    true
}

fn default_tun_mask() -> String {
    "255.255.255.0".to_owned()
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TunStopCommand {}

#[cfg(test)]
mod tests {
    use super::{CommandRequest, TunStopCommand};
    use serde_json::json;

    #[test]
    fn tun_stop_accepts_and_serializes_empty_object_params() {
        let request: CommandRequest = serde_json::from_value(json!({
            "method": "tun.stop",
            "params": {}
        }))
        .expect("deserialize tun.stop with standard command object params");

        assert_eq!(request, CommandRequest::TunStop(TunStopCommand {}));
        let value = serde_json::to_value(request).expect("serialize tun.stop command");
        assert_eq!(value["method"], "tun.stop");
        assert_eq!(value["params"], json!({}));
    }
}
