//! Read-only host interface inventory for explicit socket constraints.
use std::{io, net::IpAddr};
use zero_traits::{canonicalize_ip, AddressFamily, DialPolicy};
#[derive(Debug)]
pub(crate) struct LocalInterface {
    pub name: String,
    pub aliases: Vec<String>,
    pub ipv4_index: u32,
    pub ipv6_index: u32,
    pub addresses: Vec<IpAddr>,
}
impl LocalInterface {
    pub fn matches(&self, name: &str) -> bool {
        self.name == name || self.aliases.iter().any(|alias| alias == name)
    }
    pub fn index(&self, ipv6: bool) -> u32 {
        if ipv6 {
            self.ipv6_index
        } else {
            self.ipv4_index
        }
    }
}
pub(crate) fn inventory_for(policy: &DialPolicy) -> io::Result<Vec<LocalInterface>> {
    policy.validate().map_err(invalid_policy)?;
    if policy.interface.is_none() && policy.source_ip.is_none() {
        return Ok(Vec::new());
    }
    let interfaces = read()?;
    let selected = if let Some(name) = &policy.interface {
        Some(
            interfaces
                .iter()
                .find(|interface| interface.matches(name))
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::NotFound,
                        format!("dial interface {name:?} does not exist"),
                    )
                })?,
        )
    } else {
        None
    };
    let source = policy.source_ip.map(canonicalize_ip);
    if let Some(source) = source {
        if !interfaces
            .iter()
            .any(|interface| interface.addresses.contains(&source))
        {
            return Err(io::Error::new(
                io::ErrorKind::AddrNotAvailable,
                format!("dial source_ip {source} is not assigned locally"),
            ));
        }
        if selected.is_none()
            && matches!(source, IpAddr::V6(address) if address.is_unicast_link_local())
            && interfaces
                .iter()
                .filter(|interface| interface.addresses.contains(&source))
                .count()
                > 1
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "link-local source_ip requires an unambiguous interface",
            ));
        }
        if selected.is_some_and(|interface| !interface.addresses.contains(&source)) {
            return Err(io::Error::new(
                io::ErrorKind::AddrNotAvailable,
                format!("dial source_ip {source} is not owned by the requested interface"),
            ));
        }
    }
    if let Some(interface) = selected {
        let family = policy.effective_family().map_err(invalid_policy)?;
        if family != AddressFamily::Auto
            && !interface
                .addresses
                .iter()
                .any(|address| family.allows(*address))
        {
            return Err(io::Error::new(
                io::ErrorKind::AddrNotAvailable,
                "dial interface has no address in the required family",
            ));
        }
    }
    Ok(interfaces)
}
pub(crate) fn invalid_policy(error: zero_traits::DialPolicyError) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, error)
}
#[cfg(unix)]
fn read() -> io::Result<Vec<LocalInterface>> {
    use std::ffi::CStr;
    let mut head = std::ptr::null_mut();
    // SAFETY: getifaddrs writes a list owned by the guard below.
    if unsafe { libc::getifaddrs(&mut head) } != 0 {
        return Err(io::Error::last_os_error());
    }
    struct Addresses(*mut libc::ifaddrs);
    impl Drop for Addresses {
        fn drop(&mut self) {
            // SAFETY: this is the original allocation returned by getifaddrs.
            unsafe { libc::freeifaddrs(self.0) };
        }
    }
    let _guard = Addresses(head);
    let mut result: Vec<LocalInterface> = Vec::new();
    let mut cursor = head;
    while !cursor.is_null() {
        // SAFETY: all pointers stay within the live guarded OS list.
        let entry = unsafe { &*cursor };
        cursor = entry.ifa_next;
        if entry.ifa_name.is_null() {
            continue;
        }
        let name = unsafe { CStr::from_ptr(entry.ifa_name) }
            .to_string_lossy()
            .into_owned();
        let index = unsafe { libc::if_nametoindex(entry.ifa_name) };
        if index == 0 {
            return Err(io::Error::last_os_error());
        }
        let slot = if let Some(slot) = result.iter().position(|interface| interface.name == name) {
            slot
        } else {
            result.push(LocalInterface {
                name,
                aliases: Vec::new(),
                ipv4_index: index,
                ipv6_index: index,
                addresses: Vec::new(),
            });
            result.len() - 1
        };
        if entry.ifa_addr.is_null() {
            continue;
        }
        let address = unsafe {
            match (*entry.ifa_addr).sa_family as i32 {
                libc::AF_INET => {
                    let address = &*entry.ifa_addr.cast::<libc::sockaddr_in>();
                    Some(IpAddr::V4(address.sin_addr.s_addr.to_ne_bytes().into()))
                }
                libc::AF_INET6 => {
                    let address = &*entry.ifa_addr.cast::<libc::sockaddr_in6>();
                    Some(IpAddr::V6(address.sin6_addr.s6_addr.into()))
                }
                _ => None,
            }
        };
        if let Some(address) = address {
            result[slot].addresses.push(canonicalize_ip(address));
        }
    }
    Ok(result)
}
#[cfg(windows)]
#[path = "interfaces_windows.rs"]
mod windows;
#[cfg(windows)]
use windows::read;
#[cfg(not(any(unix, windows)))]
fn read() -> io::Result<Vec<LocalInterface>> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "explicit dial constraints are unsupported on this platform",
    ))
}
