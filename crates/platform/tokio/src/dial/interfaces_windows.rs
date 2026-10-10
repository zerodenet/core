use super::LocalInterface;
use std::{ffi::CStr, io, net::IpAddr};
use windows_sys::Win32::{
    Foundation::{ERROR_BUFFER_OVERFLOW, ERROR_SUCCESS},
    NetworkManagement::IpHelper::{
        GetAdaptersAddresses, GAA_FLAG_SKIP_ANYCAST, GAA_FLAG_SKIP_DNS_SERVER,
        GAA_FLAG_SKIP_MULTICAST, IP_ADAPTER_ADDRESSES_LH,
    },
    Networking::WinSock::{AF_INET, AF_INET6, AF_UNSPEC, SOCKADDR_IN, SOCKADDR_IN6},
};
pub(super) fn read() -> io::Result<Vec<LocalInterface>> {
    let mut size = 16_384_u32;
    for _ in 0..3 {
        let mut buffer = vec![0_u64; (size as usize).div_ceil(std::mem::size_of::<u64>())];
        let adapters = buffer.as_mut_ptr().cast::<IP_ADAPTER_ADDRESSES_LH>();
        // SAFETY: buffer is aligned and has the requested byte capacity.
        let status = unsafe {
            GetAdaptersAddresses(
                AF_UNSPEC as u32,
                GAA_FLAG_SKIP_ANYCAST | GAA_FLAG_SKIP_MULTICAST | GAA_FLAG_SKIP_DNS_SERVER,
                std::ptr::null(),
                adapters,
                &mut size,
            )
        };
        if status == ERROR_BUFFER_OVERFLOW {
            continue;
        }
        if status != ERROR_SUCCESS {
            return Err(io::Error::from_raw_os_error(status as i32));
        }
        let mut result = Vec::new();
        let mut cursor = adapters;
        // SAFETY: successful GetAdaptersAddresses returned this linked list
        // and its strings/socket addresses within the live buffer.
        unsafe {
            while let Some(adapter) = cursor.as_ref() {
                cursor = adapter.Next;
                if adapter.AdapterName.is_null() {
                    continue;
                }
                let name = CStr::from_ptr(adapter.AdapterName.cast())
                    .to_string_lossy()
                    .into_owned();
                let mut aliases = Vec::new();
                if !adapter.FriendlyName.is_null() {
                    let mut length = 0;
                    while *adapter.FriendlyName.add(length) != 0 {
                        length += 1;
                    }
                    aliases.push(String::from_utf16_lossy(std::slice::from_raw_parts(
                        adapter.FriendlyName,
                        length,
                    )));
                }
                let mut addresses = Vec::new();
                let mut unicast = adapter.FirstUnicastAddress;
                while let Some(entry) = unicast.as_ref() {
                    unicast = entry.Next;
                    let address = entry.Address.lpSockaddr;
                    if address.is_null() {
                        continue;
                    }
                    match (*address).sa_family {
                        AF_INET => addresses.push(IpAddr::V4(
                            (*address.cast::<SOCKADDR_IN>())
                                .sin_addr
                                .S_un
                                .S_addr
                                .to_ne_bytes()
                                .into(),
                        )),
                        AF_INET6 => addresses.push(IpAddr::V6(
                            (*address.cast::<SOCKADDR_IN6>()).sin6_addr.u.Byte.into(),
                        )),
                        _ => {}
                    }
                }
                result.push(LocalInterface {
                    name,
                    aliases,
                    ipv4_index: adapter.Anonymous1.Anonymous.IfIndex,
                    ipv6_index: adapter.Ipv6IfIndex,
                    addresses,
                });
            }
        }
        return Ok(result);
    }
    Err(io::Error::other(
        "Windows interface inventory changed during dial validation",
    ))
}
