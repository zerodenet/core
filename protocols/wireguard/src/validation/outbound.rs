use alloc::vec::Vec;

use super::{
    parse_endpoint, parse_key, parse_network, Endpoint, EndpointError, IpNetwork, Key, KeyError,
    NetworkError,
};

pub const DEFAULT_MTU: u16 = 1420;
pub const MIN_IPV6_MTU: u16 = 1280;

#[derive(Debug, Clone, Copy)]
pub struct PeerInput<'a> {
    pub public_key: &'a str,
    pub pre_shared_key: Option<&'a str>,
    pub endpoint: &'a str,
    pub allowed_ips: &'a [&'a str],
    pub keepalive_secs: u16,
    pub reserved: &'a [u8],
}

#[derive(Debug, Clone, Copy)]
pub struct OutboundInput<'a> {
    pub private_key: &'a str,
    pub addresses: &'a [&'a str],
    pub mtu: u16,
    pub peers: &'a [PeerInput<'a>],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatedPeer<'a> {
    pub public_key: Key,
    pub pre_shared_key: Option<Key>,
    pub endpoint: Endpoint<'a>,
    pub allowed_ips: Vec<IpNetwork>,
    pub keepalive_secs: u16,
    pub reserved: Option<[u8; 3]>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatedOutbound<'a> {
    pub private_key: Key,
    pub addresses: Vec<IpNetwork>,
    pub mtu: u16,
    pub peers: Vec<ValidatedPeer<'a>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValidationError {
    Key {
        peer: Option<usize>,
        source: KeyError,
    },
    MissingAddresses,
    Address {
        index: usize,
        source: NetworkError,
    },
    DuplicateAddress {
        first: usize,
        second: usize,
    },
    InvalidMtu,
    Ipv6MtuTooSmall {
        mtu: u16,
    },
    MissingPeers,
    Endpoint {
        peer: usize,
        source: EndpointError,
    },
    MissingAllowedIps {
        peer: usize,
    },
    AllowedIp {
        peer: usize,
        index: usize,
        source: NetworkError,
    },
    InvalidReservedLength {
        peer: usize,
    },
    DuplicatePeerKey {
        first: usize,
        second: usize,
    },
    PrivateKeyMatchesPeer {
        peer: usize,
    },
    ConflictingAllowedIp {
        first_peer: usize,
        second_peer: usize,
    },
}

impl core::fmt::Display for ValidationError {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Key { peer: None, source } => {
                write!(formatter, "private key is invalid: {source:?}")
            }
            Self::Key {
                peer: Some(peer),
                source,
            } => write!(formatter, "peer {peer} key is invalid: {source:?}"),
            Self::MissingAddresses => formatter.write_str("requires at least one tunnel address"),
            Self::Address { index, source } => {
                write!(formatter, "address {index} is invalid: {source:?}")
            }
            Self::DuplicateAddress { first, second } => {
                write!(formatter, "addresses {first} and {second} are duplicates")
            }
            Self::InvalidMtu => formatter.write_str("MTU must be greater than zero"),
            Self::Ipv6MtuTooSmall { mtu } => {
                write!(formatter, "IPv6 requires MTU >= {MIN_IPV6_MTU}, got {mtu}")
            }
            Self::MissingPeers => formatter.write_str("requires at least one peer"),
            Self::Endpoint { peer, source } => {
                write!(formatter, "peer {peer} endpoint is invalid: {source:?}")
            }
            Self::MissingAllowedIps { peer } => {
                write!(formatter, "peer {peer} requires at least one allowed IP")
            }
            Self::AllowedIp {
                peer,
                index,
                source,
            } => write!(
                formatter,
                "peer {peer} allowed IP {index} is invalid: {source:?}"
            ),
            Self::InvalidReservedLength { peer } => {
                write!(
                    formatter,
                    "peer {peer} reserved must contain exactly 3 bytes"
                )
            }
            Self::DuplicatePeerKey { first, second } => {
                write!(
                    formatter,
                    "peers {first} and {second} use the same public key"
                )
            }
            Self::PrivateKeyMatchesPeer { peer } => {
                write!(formatter, "peer {peer} public key matches the private key")
            }
            Self::ConflictingAllowedIp {
                first_peer,
                second_peer,
            } => write!(
                formatter,
                "peers {first_peer} and {second_peer} declare the same allowed-IP prefix"
            ),
        }
    }
}

pub fn validate_outbound(
    input: OutboundInput<'_>,
) -> Result<ValidatedOutbound<'_>, ValidationError> {
    let private_key = parse_key(input.private_key)
        .map_err(|source| ValidationError::Key { peer: None, source })?;
    let addresses = validate_addresses(input.addresses, input.mtu)?;
    if input.peers.is_empty() {
        return Err(ValidationError::MissingPeers);
    }

    let mut peers: Vec<ValidatedPeer<'_>> = Vec::with_capacity(input.peers.len());
    for (peer_index, peer) in input.peers.iter().enumerate() {
        let public_key = parse_key(peer.public_key).map_err(|source| ValidationError::Key {
            peer: Some(peer_index),
            source,
        })?;
        if public_key == private_key {
            return Err(ValidationError::PrivateKeyMatchesPeer { peer: peer_index });
        }
        if let Some(first) = peers.iter().position(|item| item.public_key == public_key) {
            return Err(ValidationError::DuplicatePeerKey {
                first,
                second: peer_index,
            });
        }
        let pre_shared_key = peer
            .pre_shared_key
            .map(parse_key)
            .transpose()
            .map_err(|source| ValidationError::Key {
                peer: Some(peer_index),
                source,
            })?;
        let endpoint =
            parse_endpoint(peer.endpoint).map_err(|source| ValidationError::Endpoint {
                peer: peer_index,
                source,
            })?;
        let allowed_ips = validate_allowed_ips(peer_index, peer.allowed_ips, &peers)?;
        let reserved = match peer.reserved {
            [] => None,
            [first, second, third] => Some([*first, *second, *third]),
            _ => return Err(ValidationError::InvalidReservedLength { peer: peer_index }),
        };
        peers.push(ValidatedPeer {
            public_key,
            pre_shared_key,
            endpoint,
            allowed_ips,
            keepalive_secs: peer.keepalive_secs,
            reserved,
        });
    }

    Ok(ValidatedOutbound {
        private_key,
        addresses,
        mtu: input.mtu,
        peers,
    })
}

fn validate_addresses(values: &[&str], mtu: u16) -> Result<Vec<IpNetwork>, ValidationError> {
    if values.is_empty() {
        return Err(ValidationError::MissingAddresses);
    }
    if mtu == 0 {
        return Err(ValidationError::InvalidMtu);
    }
    let mut addresses = Vec::with_capacity(values.len());
    for (index, value) in values.iter().enumerate() {
        let network =
            parse_network(value).map_err(|source| ValidationError::Address { index, source })?;
        if network.address().is_ipv6() && mtu < MIN_IPV6_MTU {
            return Err(ValidationError::Ipv6MtuTooSmall { mtu });
        }
        if let Some(first) = addresses
            .iter()
            .position(|item: &IpNetwork| item == &network)
        {
            return Err(ValidationError::DuplicateAddress {
                first,
                second: index,
            });
        }
        addresses.push(network);
    }
    Ok(addresses)
}

fn validate_allowed_ips(
    peer_index: usize,
    values: &[&str],
    peers: &[ValidatedPeer<'_>],
) -> Result<Vec<IpNetwork>, ValidationError> {
    if values.is_empty() {
        return Err(ValidationError::MissingAllowedIps { peer: peer_index });
    }
    let mut allowed_ips = Vec::with_capacity(values.len());
    for (index, value) in values.iter().enumerate() {
        let network = parse_network(value).map_err(|source| ValidationError::AllowedIp {
            peer: peer_index,
            index,
            source,
        })?;
        let previous_peer = peers.iter().position(|existing| {
            existing
                .allowed_ips
                .iter()
                .any(|candidate| candidate.same_prefix(network))
        });
        if previous_peer.is_some()
            || allowed_ips
                .iter()
                .any(|candidate: &IpNetwork| candidate.same_prefix(network))
        {
            return Err(ValidationError::ConflictingAllowedIp {
                first_peer: previous_peer.unwrap_or(peer_index),
                second_peer: peer_index,
            });
        }
        allowed_ips.push(network);
    }
    Ok(allowed_ips)
}
