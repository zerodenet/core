//! Runtime-neutral requirements for a native outbound socket.
use alloc::{string::String, vec::Vec};
use core::{fmt, net::{IpAddr, SocketAddr}};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub enum AddressFamily {
    #[default]
    Auto,
    OnlyIpv4,
    OnlyIpv6,
}
impl AddressFamily {
    pub fn allows(self, address: IpAddr) -> bool {
        match self {
            Self::Auto => true,
            Self::OnlyIpv4 => canonicalize_ip(address).is_ipv4(),
            Self::OnlyIpv6 => canonicalize_ip(address).is_ipv6(),
        }
    }
}
/// Hard requirements, never preferences that a retry may discard.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct DialPolicy {
    pub address_family: AddressFamily,
    pub interface: Option<String>,
    pub source_ip: Option<IpAddr>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DialPolicyError {
    InvalidInterface,
    InvalidSourceIp,
    SourceFamilyConflict,
    DestinationFamilyMismatch,
}
impl fmt::Display for DialPolicyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidInterface => "dial interface must be a non-empty name without surrounding whitespace, control characters or slash",
            Self::InvalidSourceIp => "dial source_ip must be a unicast, non-wildcard local address",
            Self::SourceFamilyConflict => "dial source_ip conflicts with address_family",
            Self::DestinationFamilyMismatch => "destination does not satisfy the dial address family",
        })
    }
}
impl core::error::Error for DialPolicyError {}
impl DialPolicy {
    /// Structural validation only. The platform validates local ownership.
    pub fn validate(&self) -> Result<(), DialPolicyError> {
        if self.interface.as_ref().is_some_and(|name| {
            name.is_empty() || name.trim() != name || name.chars().any(char::is_control) || name.contains('/')
        }) {
            return Err(DialPolicyError::InvalidInterface);
        }
        if let Some(source) = self.source_ip.map(canonicalize_ip) {
            if source.is_unspecified() || source.is_multicast()
                || matches!(source, IpAddr::V4(address) if address.is_broadcast()) {
                return Err(DialPolicyError::InvalidSourceIp);
            }
            if !self.address_family.allows(source) {
                return Err(DialPolicyError::SourceFamilyConflict);
            }
        }
        Ok(())
    }
    pub fn effective_family(&self) -> Result<AddressFamily, DialPolicyError> {
        self.validate()?;
        Ok(match self.source_ip.map(canonicalize_ip) {
            Some(IpAddr::V4(_)) => AddressFamily::OnlyIpv4,
            Some(IpAddr::V6(_)) => AddressFamily::OnlyIpv6,
            None => self.address_family,
        })
    }
    pub fn allows_ip(&self, address: IpAddr) -> bool {
        self.effective_family().is_ok_and(|family| family.allows(address))
    }
    /// Preserve IPv6 scope IDs while collapsing IPv4-mapped addresses.
    pub fn normalize_peer(&self, address: SocketAddr) -> Result<SocketAddr, DialPolicyError> {
        let family = self.effective_family()?;
        let ip = canonicalize_ip(address.ip());
        if !family.allows(ip) { return Err(DialPolicyError::DestinationFamilyMismatch); }
        Ok(if ip != address.ip() { SocketAddr::new(ip, address.port()) } else { address })
    }
    /// Filter without changing resolver order or adding fallback candidates.
    pub fn filter_candidates(&self, addresses: impl IntoIterator<Item = SocketAddr>) -> Result<Vec<SocketAddr>, DialPolicyError> {
        self.validate()?;
        Ok(addresses.into_iter().filter_map(|address| self.normalize_peer(address).ok()).collect())
    }
}
pub fn canonicalize_ip(address: IpAddr) -> IpAddr {
    match address {
        IpAddr::V6(address) => address.to_ipv4_mapped().map(IpAddr::V4).unwrap_or(IpAddr::V6(address)),
        address => address,
    }
}
