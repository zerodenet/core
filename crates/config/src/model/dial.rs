use std::net::IpAddr;

use serde::{Deserialize, Deserializer, Serialize};

/// Per-outbound constraints for sockets created by the selected Direct leaf.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutboundDialConfig {
    #[serde(default)]
    pub address_family: OutboundAddressFamily,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interface: Option<String>,
    #[serde(
        default,
        deserialize_with = "deserialize_source_ip",
        skip_serializing_if = "Option::is_none"
    )]
    pub source_ip: Option<IpAddr>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutboundAddressFamily {
    #[default]
    Auto,
    OnlyIpv4,
    OnlyIpv6,
}

impl OutboundDialConfig {
    pub fn is_default(&self) -> bool {
        *self == Self::default()
    }

    /// Project config into the runtime-neutral socket contract. Local interface
    /// and address ownership are deliberately checked by the platform at use.
    pub fn to_policy(&self) -> zero_traits::DialPolicy {
        zero_traits::DialPolicy {
            address_family: match self.address_family {
                OutboundAddressFamily::Auto => zero_traits::AddressFamily::Auto,
                OutboundAddressFamily::OnlyIpv4 => zero_traits::AddressFamily::OnlyIpv4,
                OutboundAddressFamily::OnlyIpv6 => zero_traits::AddressFamily::OnlyIpv6,
            },
            interface: self.interface.clone(),
            source_ip: self.source_ip.map(zero_traits::canonicalize_ip),
        }
    }
}

fn deserialize_source_ip<'de, D>(deserializer: D) -> Result<Option<IpAddr>, D::Error>
where
    D: Deserializer<'de>,
{
    Option::<IpAddr>::deserialize(deserializer)
        .map(|source| source.map(zero_traits::canonicalize_ip))
}
