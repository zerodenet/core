//! Existing point-to-point address derivation.
use super::*;

pub(crate) fn ipv4_peer(
    address: std::net::Ipv4Addr,
    mask: IpAddr,
) -> io::Result<std::net::Ipv4Addr> {
    let IpAddr::V4(mask) = mask else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "macOS IPv4 TUN address requires an IPv4 netmask",
        ));
    };
    let address = u32::from(address);
    let mask = u32::from(mask);
    let network = address & mask;
    let last = network | !mask;
    let peer = if address < last {
        address + 1
    } else if address > network {
        address - 1
    } else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "macOS IPv4 TUN subnet must contain a distinct point-to-point peer",
        ));
    };
    Ok(peer.into())
}
