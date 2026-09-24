use alloc::{string::String, vec::Vec};
use core::net::IpAddr;

use crate::{
    routing::PeerRoutes,
    validation::{validate_outbound, Key, OutboundInput, ValidationError},
};

/// Validated, owned protocol state shared by all connections using one outbound.
/// Key material is zeroized by `Key` when the profile is dropped.
pub struct PreparedOutbound {
    private_key: Key,
    addresses: Vec<IpAddr>,
    mtu: u16,
    peers: Vec<PreparedPeer>,
    routes: PeerRoutes,
}

pub struct PreparedPeer {
    public_key: Key,
    pre_shared_key: Option<Key>,
    endpoint_host: String,
    endpoint_port: u16,
    keepalive_secs: u16,
    reserved: Option<[u8; 3]>,
}

impl PreparedOutbound {
    pub fn from_input(input: OutboundInput<'_>) -> Result<Self, ValidationError> {
        let validated = validate_outbound(input)?;
        let routes = PeerRoutes::from_validated(&validated);
        Ok(Self {
            private_key: validated.private_key,
            addresses: validated
                .addresses
                .iter()
                .map(|address| address.address())
                .collect(),
            mtu: validated.mtu,
            peers: validated
                .peers
                .into_iter()
                .map(|peer| PreparedPeer {
                    public_key: peer.public_key,
                    pre_shared_key: peer.pre_shared_key,
                    endpoint_host: peer.endpoint.host.into(),
                    endpoint_port: peer.endpoint.port,
                    keepalive_secs: peer.keepalive_secs,
                    reserved: peer.reserved,
                })
                .collect(),
            routes,
        })
    }

    pub fn peer_for_destination(&self, destination: IpAddr) -> Option<usize> {
        self.routes.peer_for_destination(destination)
    }

    pub fn allows_authenticated_source(&self, peer: usize, source: IpAddr) -> bool {
        self.routes.allows_authenticated_source(peer, source)
    }

    pub fn local_address_for(&self, destination: IpAddr) -> Option<IpAddr> {
        self.addresses
            .iter()
            .copied()
            .find(|address| address.is_ipv4() == destination.is_ipv4())
    }

    pub fn local_addresses(&self) -> &[IpAddr] {
        &self.addresses
    }

    pub fn mtu(&self) -> u16 {
        self.mtu
    }

    pub fn peer(&self, index: usize) -> Option<&PreparedPeer> {
        self.peers.get(index)
    }

    pub fn peer_count(&self) -> usize {
        self.peers.len()
    }

    pub(super) fn private_key(&self) -> &Key {
        &self.private_key
    }
}

impl PreparedPeer {
    pub fn endpoint_host(&self) -> &str {
        &self.endpoint_host
    }

    pub fn endpoint_port(&self) -> u16 {
        self.endpoint_port
    }

    pub(super) fn public_key(&self) -> &Key {
        &self.public_key
    }

    pub(super) fn pre_shared_key(&self) -> Option<&Key> {
        self.pre_shared_key.as_ref()
    }

    pub(super) fn keepalive_secs(&self) -> u16 {
        self.keepalive_secs
    }

    pub(super) fn reserved(&self) -> Option<[u8; 3]> {
        self.reserved
    }
}
