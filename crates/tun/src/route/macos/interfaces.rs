use std::ffi::CStr;
use std::io;
use std::net::IpAddr;

use crate::route::EgressUnavailableReason;

pub(super) fn unavailable_reason(
    name: &str,
    ipv6: bool,
) -> io::Result<Option<EgressUnavailableReason>> {
    let mut head = std::ptr::null_mut();
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
    let mut active = false;
    let mut usable = false;
    let mut current = head;
    while !current.is_null() {
        // SAFETY: current and address storage belong to the guarded list.
        let entry = unsafe { &*current };
        current = entry.ifa_next;
        if entry.ifa_name.is_null()
            || unsafe { CStr::from_ptr(entry.ifa_name) }.to_bytes() != name.as_bytes()
        {
            continue;
        }
        let required = (libc::IFF_UP | libc::IFF_RUNNING) as u32;
        if entry.ifa_flags & required != required
            || entry.ifa_flags & libc::IFF_LOOPBACK as u32 != 0
        {
            continue;
        }
        active = true;
        usable |= unsafe { super::exclusions::socket_address(entry.ifa_addr) }
            .is_some_and(|address| address.is_ipv6() == ipv6 && usable_address(address));
    }
    Ok(classify(active, usable))
}

fn classify(active: bool, usable: bool) -> Option<EgressUnavailableReason> {
    if !active {
        Some(EgressUnavailableReason::InterfaceDown)
    } else if !usable {
        Some(EgressUnavailableReason::NoUsableAddress)
    } else {
        None
    }
}

fn usable_address(address: IpAddr) -> bool {
    match address {
        IpAddr::V4(address) => {
            !address.is_unspecified()
                && !address.is_loopback()
                && !address.is_link_local()
                && !address.is_multicast()
                && !address.is_broadcast()
        }
        IpAddr::V6(address) => {
            !address.is_unspecified()
                && !address.is_loopback()
                && !address.is_unicast_link_local()
                && !address.is_multicast()
        }
    }
}

#[cfg(test)]
mod tests;
