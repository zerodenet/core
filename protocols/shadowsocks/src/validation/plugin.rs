//! SIP003/SIP003u options, available without compiling the data plane.
use alloc::{string::String, vec::Vec};
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum PluginMode {
    #[default]
    TcpOnly,
    UdpOnly,
    TcpAndUdp,
}
impl PluginMode {
    pub const fn tcp(self) -> bool {
        !matches!(self, Self::UdpOnly)
    }
    pub const fn udp(self) -> bool {
        !matches!(self, Self::TcpOnly)
    }
}
#[derive(Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PluginConfig {
    pub command: String,
    #[serde(default)]
    pub options: Option<String>,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub mode: PluginMode,
}
impl core::fmt::Debug for PluginConfig {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("PluginConfig")
            .field("command", &self.command)
            .field("mode", &self.mode)
            .finish_non_exhaustive()
    }
}
impl PluginConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.command.trim().is_empty() || self.command.contains('\0') {
            return Err("plugin command must be nonempty and contain no NUL".into());
        }
        if self
            .options
            .as_ref()
            .is_some_and(|value| value.contains('\0'))
            || self.args.iter().any(|value| value.contains('\0'))
        {
            return Err("plugin arguments and options must contain no NUL".into());
        }
        Ok(())
    }
}
