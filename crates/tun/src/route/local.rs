//! Exact addresses owned by this TUN, never a blanket private-network exclusion.
use std::{io, net::IpAddr};

/// Bounded local facts for one actual interface address, without allocation.
pub fn tun_local_address_facts(address: IpAddr, mask: IpAddr) -> io::Result<[Option<IpAddr>; 3]> {
    if address.is_ipv4() != mask.is_ipv4() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "TUN address/mask family mismatch",
        ));
    }
    let prefix = crate::mask_to_prefix(mask)?;
    let mut local = [Some(address), None, None];
    // IPv6 reserves only the actual local address; another VPN may own its neighbor.
    if let IpAddr::V4(ip) = address {
        if let (IpAddr::V4(mask), Some(peer)) = (mask, u32::from(ip).checked_add(1)) {
            let mask = u32::from(mask);
            if prefix <= 30 && u32::from(ip) & mask == peer & mask && peer & !mask != !mask {
                local[1] = Some(std::net::Ipv4Addr::from(peer).into());
            }
        }
        #[cfg(target_os = "macos")]
        {
            local[2] = Some(crate::macos::ipv4_peer(ip, mask)?.into());
        }
    }
    Ok(local)
}

/// Local interface addresses and synthetic DNS/point-to-point peers must stay in TUN.
pub fn tun_local_addresses(addresses: &[(IpAddr, IpAddr)]) -> io::Result<Vec<IpAddr>> {
    let mut local = Vec::new();
    for &(address, mask) in addresses {
        local.extend(
            tun_local_address_facts(address, mask)?
                .into_iter()
                .flatten(),
        );
    }
    local.sort_unstable();
    local.dedup();
    Ok(local)
}
#[cfg(test)]
mod tests;
