use alloc::vec::Vec;

use super::{
    parse_key, parse_network, IpNetwork, Key, KeyError, NetworkError, MAX_MTU, MIN_IPV4_MTU,
    MIN_IPV6_MTU,
};

pub const MAX_INBOUND_PEERS: usize = 128;

#[derive(Debug, Clone, Copy)]
pub struct InboundPeerInput<'a> {
    pub public_key: &'a str,
    pub pre_shared_key: Option<&'a str>,
    pub allowed_ips: &'a [&'a str],
    pub keepalive_secs: u16,
    pub reserved: &'a [u8],
}

#[derive(Debug, Clone, Copy)]
pub struct InboundInput<'a> {
    pub private_key: &'a str,
    pub mtu: u16,
    pub peers: &'a [InboundPeerInput<'a>],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatedInboundPeer {
    pub public_key: Key,
    pub pre_shared_key: Option<Key>,
    pub allowed_ips: Vec<IpNetwork>,
    pub keepalive_secs: u16,
    pub reserved: Option<[u8; 3]>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatedInbound {
    pub private_key: Key,
    pub mtu: u16,
    pub peers: Vec<ValidatedInboundPeer>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InboundValidationError {
    Key {
        peer: Option<usize>,
        source: KeyError,
    },
    InvalidMtu,
    Ipv6MtuTooSmall {
        mtu: u16,
    },
    MissingPeers,
    TooManyPeers,
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
    UnsupportedReserved {
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

impl core::fmt::Display for InboundValidationError {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Key { peer: None, source } => {
                write!(formatter, "private key is invalid: {source:?}")
            }
            Self::Key {
                peer: Some(peer),
                source,
            } => write!(formatter, "peer {peer} key is invalid: {source:?}"),
            Self::InvalidMtu => write!(
                formatter,
                "MTU must be between {MIN_IPV4_MTU} and {MAX_MTU}"
            ),
            Self::Ipv6MtuTooSmall { mtu } => {
                write!(formatter, "IPv6 requires MTU >= {MIN_IPV6_MTU}, got {mtu}")
            }
            Self::MissingPeers => formatter.write_str("requires at least one peer"),
            Self::TooManyPeers => write!(formatter, "supports at most {MAX_INBOUND_PEERS} peers"),
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
            Self::InvalidReservedLength { peer } => write!(
                formatter,
                "peer {peer} reserved must contain exactly 3 bytes"
            ),
            Self::UnsupportedReserved { peer } => write!(
                formatter,
                "peer {peer} non-zero reserved bytes are not supported"
            ),
            Self::DuplicatePeerKey { first, second } => write!(
                formatter,
                "peers {first} and {second} use the same public key"
            ),
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

pub fn validate_inbound(
    input: InboundInput<'_>,
) -> Result<ValidatedInbound, InboundValidationError> {
    let private_key = parse_key(input.private_key)
        .map_err(|source| InboundValidationError::Key { peer: None, source })?;
    if !(MIN_IPV4_MTU..=MAX_MTU).contains(&input.mtu) {
        return Err(InboundValidationError::InvalidMtu);
    }
    if input.peers.is_empty() {
        return Err(InboundValidationError::MissingPeers);
    }
    if input.peers.len() > MAX_INBOUND_PEERS {
        return Err(InboundValidationError::TooManyPeers);
    }
    let mut peers: Vec<ValidatedInboundPeer> = Vec::with_capacity(input.peers.len());
    for (peer_index, peer) in input.peers.iter().enumerate() {
        let public_key =
            parse_key(peer.public_key).map_err(|source| InboundValidationError::Key {
                peer: Some(peer_index),
                source,
            })?;
        if public_key == private_key {
            return Err(InboundValidationError::PrivateKeyMatchesPeer { peer: peer_index });
        }
        if let Some(first) = peers
            .iter()
            .position(|entry| entry.public_key == public_key)
        {
            return Err(InboundValidationError::DuplicatePeerKey {
                first,
                second: peer_index,
            });
        }
        let pre_shared_key = peer
            .pre_shared_key
            .map(parse_key)
            .transpose()
            .map_err(|source| InboundValidationError::Key {
                peer: Some(peer_index),
                source,
            })?;
        if peer.allowed_ips.is_empty() {
            return Err(InboundValidationError::MissingAllowedIps { peer: peer_index });
        }
        let mut allowed_ips = Vec::with_capacity(peer.allowed_ips.len());
        for (index, value) in peer.allowed_ips.iter().enumerate() {
            let network =
                parse_network(value).map_err(|source| InboundValidationError::AllowedIp {
                    peer: peer_index,
                    index,
                    source,
                })?;
            if network.address().is_ipv6() && input.mtu < MIN_IPV6_MTU {
                return Err(InboundValidationError::Ipv6MtuTooSmall { mtu: input.mtu });
            }
            let previous_peer = peers.iter().position(|entry| {
                entry
                    .allowed_ips
                    .iter()
                    .any(|candidate| candidate.same_prefix(network))
            });
            if previous_peer.is_some()
                || allowed_ips
                    .iter()
                    .any(|candidate: &IpNetwork| candidate.same_prefix(network))
            {
                return Err(InboundValidationError::ConflictingAllowedIp {
                    first_peer: previous_peer.unwrap_or(peer_index),
                    second_peer: peer_index,
                });
            }
            allowed_ips.push(network);
        }
        let reserved = match peer.reserved {
            [] => None,
            [first, second, third] => Some([*first, *second, *third]),
            _ => return Err(InboundValidationError::InvalidReservedLength { peer: peer_index }),
        };
        if reserved.is_some_and(|bytes| bytes != [0; 3]) {
            return Err(InboundValidationError::UnsupportedReserved { peer: peer_index });
        }
        peers.push(ValidatedInboundPeer {
            public_key,
            pre_shared_key,
            allowed_ips,
            keepalive_secs: peer.keepalive_secs,
            reserved,
        });
    }
    Ok(ValidatedInbound {
        private_key,
        mtu: input.mtu,
        peers,
    })
}
