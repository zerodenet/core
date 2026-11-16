//! Exact addresses owned by this TUN, never a blanket private-network exclusion.
use std::{io, net::IpAddr};

/// Local interface addresses and the synthetic DNS/point-to-point peers must
/// remain inside TUN. Recompute from the same addresses used to create it.
pub fn tun_local_addresses(addresses: &[(IpAddr, IpAddr)]) -> io::Result<Vec<IpAddr>> {
    let mut local = Vec::new();
    for &(address, mask) in addresses {
        if address.is_ipv4() != mask.is_ipv4() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "TUN address/mask family mismatch",
            ));
        }
        let prefix = crate::mask_to_prefix(mask)?;
        local.push(address);
        // The client publishes a synthetic IPv4 DNS peer. IPv6 only reserves
        // actual local addresses; another VPN may own the adjacent IPv6 host.
        if let IpAddr::V4(ip) = address {
            if let (IpAddr::V4(mask), Some(peer)) = (mask, u32::from(ip).checked_add(1)) {
                let mask = u32::from(mask);
                // The client only publishes a usable IPv4 host peer.
                if prefix <= 30 && u32::from(ip) & mask == peer & mask && peer & !mask != !mask {
                    local.push(std::net::Ipv4Addr::from(peer).into());
                }
            }
            #[cfg(target_os = "macos")]
            local.push(crate::macos::ipv4_peer(ip, mask)?.into());
        }
    }
    local.sort_unstable();
    local.dedup();
    Ok(local)
}

#[cfg(test)]
mod tests;
