use std::io;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use windows_sys::Win32::Foundation::{ERROR_BUFFER_OVERFLOW, ERROR_SUCCESS};
use windows_sys::Win32::NetworkManagement::IpHelper::{
    GetAdaptersAddresses, GAA_FLAG_SKIP_ANYCAST, GAA_FLAG_SKIP_MULTICAST, GAA_FLAG_SKIP_UNICAST,
    IF_TYPE_SOFTWARE_LOOPBACK, IP_ADAPTER_ADDRESSES_LH,
};
use windows_sys::Win32::NetworkManagement::Ndis::IfOperStatusUp;
use windows_sys::Win32::Networking::WinSock::{
    AF_INET, AF_INET6, AF_UNSPEC, SOCKADDR_IN, SOCKADDR_IN6, SOCKET_ADDRESS,
};

pub(super) fn system_dns_servers() -> io::Result<Vec<IpAddr>> {
    let flags = GAA_FLAG_SKIP_UNICAST | GAA_FLAG_SKIP_ANYCAST | GAA_FLAG_SKIP_MULTICAST;
    let mut size = 0_u32;
    let initial = unsafe {
        GetAdaptersAddresses(
            AF_UNSPEC as u32,
            flags,
            std::ptr::null(),
            std::ptr::null_mut(),
            &mut size,
        )
    };
    if initial != ERROR_BUFFER_OVERFLOW && initial != ERROR_SUCCESS {
        return Err(win32_error("size Windows adapter DNS table", initial));
    }
    if size == 0 {
        return Ok(Vec::new());
    }

    for _ in 0..3 {
        let words = (size as usize).div_ceil(std::mem::size_of::<u64>());
        let mut buffer = vec![0_u64; words];
        let adapters = buffer.as_mut_ptr().cast::<IP_ADAPTER_ADDRESSES_LH>();
        let status = unsafe {
            GetAdaptersAddresses(
                AF_UNSPEC as u32,
                flags,
                std::ptr::null(),
                adapters,
                &mut size,
            )
        };
        if status == ERROR_BUFFER_OVERFLOW {
            continue;
        }
        if status != ERROR_SUCCESS {
            return Err(win32_error("read Windows adapter DNS table", status));
        }
        return Ok(unsafe { collect_servers(adapters) });
    }

    Err(io::Error::other(
        "Windows adapter DNS table changed repeatedly during discovery",
    ))
}

unsafe fn collect_servers(mut adapter: *mut IP_ADAPTER_ADDRESSES_LH) -> Vec<IpAddr> {
    let mut servers = Vec::new();
    while let Some(current) = adapter.as_ref() {
        if current.OperStatus == IfOperStatusUp && current.IfType != IF_TYPE_SOFTWARE_LOOPBACK {
            let mut server = current.FirstDnsServerAddress;
            while let Some(dns) = server.as_ref() {
                if let Some(address) = socket_address_ip(&dns.Address) {
                    servers.push(address);
                }
                server = dns.Next;
            }
        }
        adapter = current.Next;
    }
    servers
}

unsafe fn socket_address_ip(address: &SOCKET_ADDRESS) -> Option<IpAddr> {
    let sockaddr = address.lpSockaddr.as_ref()?;
    match sockaddr.sa_family {
        AF_INET => {
            let address = &*address.lpSockaddr.cast::<SOCKADDR_IN>();
            Some(IpAddr::V4(Ipv4Addr::from(
                address.sin_addr.S_un.S_addr.to_ne_bytes(),
            )))
        }
        AF_INET6 => {
            let address = &*address.lpSockaddr.cast::<SOCKADDR_IN6>();
            Some(IpAddr::V6(Ipv6Addr::from(address.sin6_addr.u.Byte)))
        }
        _ => None,
    }
}

fn win32_error(context: &str, status: u32) -> io::Error {
    io::Error::other(format!(
        "{context}: {}",
        io::Error::from_raw_os_error(status as i32)
    ))
}
