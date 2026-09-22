use alloc::vec::Vec;
use core::net::IpAddr;

use crate::validation::{IpNetwork, ValidatedOutbound};

/// Protocol-owned allowed-IP selection for encrypted outbound and authenticated inbound packets.
///
/// The table holds no key material. It is built only from a validated profile,
/// where identical prefixes across peers have already been rejected.
#[derive(Debug, Clone)]
pub struct PeerRoutes {
    routes: Vec<(IpNetwork, usize)>,
}

impl PeerRoutes {
    pub fn from_validated(profile: &ValidatedOutbound<'_>) -> Self {
        let routes = profile
            .peers
            .iter()
            .enumerate()
            .flat_map(|(peer, entry)| {
                entry
                    .allowed_ips
                    .iter()
                    .copied()
                    .map(move |network| (network, peer))
            })
            .collect();
        Self { routes }
    }

    /// Select the peer owning the longest matching prefix for an outbound IP packet.
    pub fn peer_for_destination(&self, destination: IpAddr) -> Option<usize> {
        self.select(destination)
    }

    /// Accept an inbound source only if its longest matching prefix belongs to
    /// the peer that authenticated and decrypted the WireGuard packet.
    pub fn allows_authenticated_source(&self, peer: usize, source: IpAddr) -> bool {
        self.select(source) == Some(peer)
    }

    fn select(&self, address: IpAddr) -> Option<usize> {
        self.routes
            .iter()
            .filter(|(network, _)| network.contains(address))
            .max_by_key(|(network, _)| network.prefix_len())
            .map(|(_, peer)| *peer)
    }
}
