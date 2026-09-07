use std::ffi::CStr;
use std::io;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use ipnet::IpNet;

use super::{family, run_route};

#[cfg(test)]
mod tests;

/// Host bypasses must not replace a DNS server's existing on-link route on
/// another interface. Inspect connected networks independently of our host
/// routes so a topology change can also remove a bypass that we used to own.
pub(super) fn connected_networks(tun_name: &str) -> io::Result<Vec<IpNet>> {
    let mut head = std::ptr::null_mut();
    // SAFETY: getifaddrs initializes head; the guard frees the entire list.
    if unsafe { libc::getifaddrs(&mut head) } != 0 {
        return Err(io::Error::last_os_error());
    }
    struct Addresses(*mut libc::ifaddrs);
    impl Drop for Addresses {
        fn drop(&mut self) {
            unsafe { libc::freeifaddrs(self.0) };
        }
    }
    let _addresses = Addresses(head);
    let mut networks = Vec::new();
    let mut current = head;
    while !current.is_null() {
        // SAFETY: current is a live entry in the getifaddrs list.
        let entry = unsafe { &*current };
        current = entry.ifa_next;
        let active = (libc::IFF_UP | libc::IFF_RUNNING) as u32;
        if entry.ifa_flags & active != active || entry.ifa_name.is_null() {
            continue;
        }
        let name = unsafe { CStr::from_ptr(entry.ifa_name) }.to_string_lossy();
        if name == tun_name || entry.ifa_flags & libc::IFF_POINTOPOINT as u32 != 0 {
            continue;
        }
        if let (Some(address), Some(mask)) = (unsafe { socket_address(entry.ifa_addr) }, unsafe {
            socket_address(entry.ifa_netmask)
        }) {
            if let Ok(network) = IpNet::with_netmask(address, mask) {
                networks.push(network.trunc());
            }
        }
    }
    Ok(networks)
}

unsafe fn socket_address(address: *const libc::sockaddr) -> Option<IpAddr> {
    if address.is_null() {
        return None;
    }
    // BSD netmasks may have a shortened sa_len with omitted trailing zeros.
    // Read only that storage rather than borrowing a full sockaddr_in{,6}.
    let length = unsafe { (*address).sa_len as usize };
    if length < 2 {
        return None;
    }
    let family = unsafe { (*address).sa_family as i32 };
    let bytes = unsafe { std::slice::from_raw_parts(address.cast::<u8>(), length) };
    sockaddr_ip(family, bytes)
}

fn sockaddr_ip(family: i32, bytes: &[u8]) -> Option<IpAddr> {
    match family {
        libc::AF_INET => {
            let mut octets = [0; 4];
            for (target, value) in octets.iter_mut().zip(bytes.iter().skip(4)) {
                *target = *value;
            }
            Some(Ipv4Addr::from(octets).into())
        }
        libc::AF_INET6 => {
            let mut octets = [0; 16];
            for (target, value) in octets.iter_mut().zip(bytes.iter().skip(8)) {
                *target = *value;
            }
            Some(Ipv6Addr::from(octets).into())
        }
        _ => None,
    }
}

pub(super) fn requires_host_bypass(peer: IpAddr, captured: &[IpNet], connected: &[IpNet]) -> bool {
    let Some(capture_length) = captured
        .iter()
        .filter(|prefix| prefix.contains(&peer))
        .map(IpNet::prefix_len)
        .max()
    else {
        return false;
    };
    !connected
        .iter()
        .any(|prefix| prefix.contains(&peer) && prefix.prefix_len() > capture_length)
}

pub(super) fn has_native_route(peer: IpAddr, captured: &[IpNet], tun_name: &str) -> bool {
    let arguments = [
        "-n".to_owned(),
        "get".to_owned(),
        family(peer.is_ipv6()).to_owned(),
        peer.to_string(),
    ];
    run_route(&arguments)
        .is_ok_and(|output| native_route_outside_capture(&output, peer, captured, tun_name))
}

pub(super) fn native_route_outside_capture(
    output: &[u8],
    peer: IpAddr,
    captured: &[IpNet],
    tun_name: &str,
) -> bool {
    let text = String::from_utf8_lossy(output);
    let field = |key: &str| {
        text.lines()
            .find_map(|line| line.trim().strip_prefix(key))
            .map(str::trim)
    };
    let Some(interface) = field("interface:") else {
        return false;
    };
    if interface == tun_name || interface.is_empty() {
        return false;
    }
    let flags = field("flags:").unwrap_or("");
    let flags = flags
        .trim_matches(['<', '>'])
        .split(',')
        .collect::<Vec<_>>();
    if flags.contains(&"REJECT") || flags.contains(&"BLACKHOLE") {
        return false;
    }
    // A scoped-only route does not protect an ordinary unscoped lookup.
    if flags.contains(&"IFSCOPE") {
        return false;
    }
    let prefix_length = if flags.contains(&"HOST") {
        if peer.is_ipv4() {
            32
        } else {
            128
        }
    } else {
        let Some(mask) = field("mask:").and_then(|mask| mask.parse().ok()) else {
            return false;
        };
        let Ok(prefix) = IpNet::with_netmask(peer, mask) else {
            return false;
        };
        prefix.prefix_len()
    };
    captured
        .iter()
        .filter(|prefix| prefix.contains(&peer))
        .all(|prefix| prefix_length > prefix.prefix_len())
}
