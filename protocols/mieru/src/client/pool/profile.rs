use super::PoolKey;
use std::{
    collections::hash_map::RandomState,
    hash::{BuildHasher, Hash},
    sync::OnceLock,
};
use zero_core::{request::Destination, Address, Error};
use zero_platform_tokio::contract::network::UpstreamBinding;
use zero_traits::DialPolicy;

#[derive(Debug, Clone, Copy)]
pub struct MieruOutboundOptionsRef<'a> {
    pub udp: bool,
    pub username: &'a str,
    pub password: &'a str,
    pub options: &'a mieru_config::MieruTransportOptions,
}
pub struct TcpPreparation<'a> {
    pub tag: &'a str,
    pub server: &'a str,
    pub port: u16,
    pub protocol: MieruOutboundOptionsRef<'a>,
    pub dial: DialPolicy,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CarrierPlan {
    pub endpoint: Destination,
    pub binding: UpstreamBinding,
    pub udp: bool,
    pub tag: Option<std::sync::Arc<str>>,
    pub wire_endpoint: Destination,
}

/// Protocol-owned credentials and carrier recipe. Debug never prints secrets.
#[derive(Clone)]
pub struct PoolProfile {
    pub(crate) key: PoolKey,
    pub(crate) options: mieru_config::MieruTransportOptions,
    pub(crate) carrier: CarrierPlan,
}
impl std::fmt::Debug for PoolProfile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MieruPoolProfile")
            .field("key", &self.key)
            .finish_non_exhaustive()
    }
}
impl PoolProfile {
    pub fn tcp(input: TcpPreparation<'_>, binding: UpstreamBinding) -> Result<Self, Error> {
        if input.protocol.udp {
            return Err(Error::Config("Mieru TCP profile requires a TCP endpoint"));
        }
        Self::new(input, binding)
    }
    pub fn new(input: TcpPreparation<'_>, binding: UpstreamBinding) -> Result<Self, Error> {
        if input.tag.is_empty() || input.server.is_empty() {
            return Err(Error::Config("Mieru profile requires a tag and endpoint"));
        }
        input
            .protocol
            .options
            .validate()
            .map_err(|_| Error::Config("invalid Mieru transport options"))?;
        if input.dial != binding.dial {
            return Err(Error::Config("Mieru profile dial differs from carrier"));
        }
        let address = match zero_core::address::parse_ip_literal(input.server) {
            Some(std::net::IpAddr::V4(ip)) => Address::Ipv4(ip.octets()),
            Some(std::net::IpAddr::V6(ip)) => Address::Ipv6(ip.octets()),
            None => Address::Domain(input.server.into()),
        };
        Ok(Self {
            key: PoolKey::new(
                input.tag,
                input.server,
                input.port,
                input.protocol.username,
                input.protocol.password,
                input.protocol.udp,
            ),
            options: input.protocol.options.clone(),
            carrier: CarrierPlan {
                udp: input.protocol.udp,
                tag: Some(input.tag.into()),
                wire_endpoint: Destination::new(Address::Domain(input.server.into()), input.port),
                ..CarrierPlan::new(Destination::new(address, input.port), binding)?
            },
        })
    }
    pub(crate) fn matches(
        &self,
        key: &PoolKey,
        options: &mieru_config::MieruTransportOptions,
    ) -> bool {
        self.key.tag == key.tag
            && self.key.server == key.server
            && self.key.port == key.port
            && self.key.username == key.username
            && self.key.password == key.password
            && self.key.udp == key.udp
            && &self.options == options
    }
    pub(crate) fn identity(&self) -> [u8; 16] {
        opaque_identity((
            "mieru.profile",
            &self.key.tag,
            &self.key.server,
            self.key.port,
            &self.key.username,
            &self.key.password,
            self.key.udp,
            format!("{:?}:{:?}", self.options, self.carrier),
        ))
    }
}
/// Process-local, secret-free equality key; bounded independently of set size.
pub(crate) fn opaque_identity(value: impl Hash) -> [u8; 16] {
    static KEYS: OnceLock<[RandomState; 2]> = OnceLock::new();
    let mut identity = [0; 16];
    for (index, key) in KEYS
        .get_or_init(|| [RandomState::new(), RandomState::new()])
        .iter()
        .enumerate()
    {
        identity[index * 8..index * 8 + 8].copy_from_slice(&key.hash_one(&value).to_le_bytes());
    }
    identity
}

impl CarrierPlan {
    pub(crate) fn new(endpoint: Destination, binding: UpstreamBinding) -> Result<Self, Error> {
        if endpoint.port == 0 {
            return Err(Error::Config("invalid Mieru background endpoint"));
        }
        if !binding.prefix.is_empty() {
            return Err(Error::Unsupported(
                "Mieru prefix carrier ownership is not integrated",
            ));
        }
        binding
            .dial
            .validate()
            .map_err(|_| Error::Config("invalid Mieru background dial policy"))?;
        Ok(Self {
            wire_endpoint: endpoint.clone(),
            endpoint,
            binding,
            udp: false,
            tag: None,
        })
    }
}
