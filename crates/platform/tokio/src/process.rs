use std::net::SocketAddr;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalProcessInfo {
    pub pid: u32,
    pub name: Option<String>,
    pub path: Option<String>,
}

/// Resolve the process that owns a local TCP endpoint.
///
/// The socket may disappear between packet capture and lookup, and platform
/// permissions can hide process metadata, so absence is a normal result.
pub async fn lookup_local_tcp_process(source: SocketAddr) -> Option<LocalProcessInfo> {
    tokio::task::spawn_blocking(move || platform::lookup_tcp(source))
        .await
        .ok()
        .flatten()
}

/// Resolve the process that owns a local UDP endpoint.
pub async fn lookup_local_udp_process(source: SocketAddr) -> Option<LocalProcessInfo> {
    tokio::task::spawn_blocking(move || platform::lookup_udp(source))
        .await
        .ok()
        .flatten()
}

#[cfg(target_os = "linux")]
mod platform {
    use super::{LocalProcessInfo, SocketAddr};

    pub(super) fn lookup_tcp(source: SocketAddr) -> Option<LocalProcessInfo> {
        lookup(source, "tcp", "tcp6")
    }

    pub(super) fn lookup_udp(source: SocketAddr) -> Option<LocalProcessInfo> {
        lookup(source, "udp", "udp6")
    }

    fn lookup(source: SocketAddr, ipv4_table: &str, ipv6_table: &str) -> Option<LocalProcessInfo> {
        let inode = find_socket_inode(source, ipv4_table, ipv6_table)?;
        find_process_by_inode(inode)
    }

    fn find_socket_inode(addr: SocketAddr, ipv4_table: &str, ipv6_table: &str) -> Option<u64> {
        let (table, encoded_ip, wildcard_ip) = match addr.ip() {
            std::net::IpAddr::V4(ip) => {
                let octets = ip.octets();
                (
                    ipv4_table,
                    format!(
                        "{:02X}{:02X}{:02X}{:02X}",
                        octets[3], octets[2], octets[1], octets[0]
                    ),
                    "00000000",
                )
            }
            std::net::IpAddr::V6(ip) => {
                let mut encoded = String::with_capacity(32);
                for chunk in ip.octets().chunks_exact(4) {
                    for byte in chunk.iter().rev() {
                        use std::fmt::Write;
                        write!(&mut encoded, "{byte:02X}").ok()?;
                    }
                }
                (ipv6_table, encoded, "00000000000000000000000000000000")
            }
        };
        let table = std::fs::read_to_string(format!("/proc/net/{table}")).ok()?;
        let encoded_port = format!("{:04X}", addr.port());

        for line in table.lines().skip(1) {
            let mut fields = line.split_whitespace();
            fields.next()?;
            let local = fields.next()?;
            let (local_ip, local_port) = local.split_once(':')?;
            if (local_ip != encoded_ip && local_ip != wildcard_ip) || local_port != encoded_port {
                continue;
            }
            for _ in 0..7 {
                fields.next()?;
            }
            return fields.next()?.parse().ok();
        }
        None
    }

    fn find_process_by_inode(target_inode: u64) -> Option<LocalProcessInfo> {
        let socket_link = format!("socket:[{target_inode}]");
        for entry in std::fs::read_dir("/proc").ok()? {
            let entry = entry.ok()?;
            let pid: u32 = match entry.file_name().to_str()?.parse() {
                Ok(pid) if pid != 0 => pid,
                _ => continue,
            };
            let fd_dir = match std::fs::read_dir(format!("/proc/{pid}/fd")) {
                Ok(entries) => entries,
                Err(_) => continue,
            };
            for fd_entry in fd_dir.flatten() {
                let Ok(link) = std::fs::read_link(fd_entry.path()) else {
                    continue;
                };
                if link.to_string_lossy() != socket_link {
                    continue;
                }
                let name = std::fs::read_to_string(format!("/proc/{pid}/comm"))
                    .ok()
                    .map(|name| name.trim().to_owned());
                let path = std::fs::read_link(format!("/proc/{pid}/exe"))
                    .ok()
                    .map(|path| path.to_string_lossy().into_owned());
                return Some(LocalProcessInfo { pid, name, path });
            }
        }
        None
    }
}

#[cfg(windows)]
mod platform {
    use std::ffi::c_void;
    use std::mem::size_of;
    use std::net::{IpAddr, SocketAddr};

    use windows_sys::Win32::Foundation::{CloseHandle, ERROR_INSUFFICIENT_BUFFER, NO_ERROR};
    use windows_sys::Win32::NetworkManagement::IpHelper::{
        GetExtendedTcpTable, GetExtendedUdpTable, MIB_TCP6ROW_OWNER_PID, MIB_TCP6TABLE_OWNER_PID,
        MIB_TCPROW_OWNER_PID, MIB_TCPTABLE_OWNER_PID, MIB_UDP6ROW_OWNER_PID,
        MIB_UDP6TABLE_OWNER_PID, MIB_UDPROW_OWNER_PID, MIB_UDPTABLE_OWNER_PID,
        TCP_TABLE_OWNER_PID_ALL, UDP_TABLE_OWNER_PID,
    };
    use windows_sys::Win32::Networking::WinSock::{AF_INET, AF_INET6};
    use windows_sys::Win32::System::Threading::{
        OpenProcess, QueryFullProcessImageNameW, PROCESS_QUERY_LIMITED_INFORMATION,
    };

    use super::LocalProcessInfo;

    pub(super) fn lookup_tcp(source: SocketAddr) -> Option<LocalProcessInfo> {
        let pid = match source.ip() {
            IpAddr::V4(ip) => find_ipv4_owner(ip.octets(), source.port()),
            IpAddr::V6(ip) => find_ipv6_owner(ip.octets(), source.port()),
        }?;
        Some(process_info(pid))
    }

    pub(super) fn lookup_udp(source: SocketAddr) -> Option<LocalProcessInfo> {
        let pid = match source.ip() {
            IpAddr::V4(ip) => find_ipv4_udp_owner(ip.octets(), source.port()),
            IpAddr::V6(ip) => find_ipv6_udp_owner(ip.octets(), source.port()),
        }?;
        Some(process_info(pid))
    }

    fn tcp_table(address_family: u32) -> Option<Vec<u32>> {
        let mut size = 0_u32;
        let first = unsafe {
            GetExtendedTcpTable(
                std::ptr::null_mut(),
                &mut size,
                0,
                address_family,
                TCP_TABLE_OWNER_PID_ALL,
                0,
            )
        };
        if first != ERROR_INSUFFICIENT_BUFFER || size == 0 {
            return None;
        }
        let mut buffer = vec![0_u32; (size as usize).div_ceil(size_of::<u32>())];
        let result = unsafe {
            GetExtendedTcpTable(
                buffer.as_mut_ptr().cast::<c_void>(),
                &mut size,
                0,
                address_family,
                TCP_TABLE_OWNER_PID_ALL,
                0,
            )
        };
        (result == NO_ERROR).then_some(buffer)
    }

    fn udp_table(address_family: u32) -> Option<Vec<u32>> {
        let mut size = 0_u32;
        let first = unsafe {
            GetExtendedUdpTable(
                std::ptr::null_mut(),
                &mut size,
                0,
                address_family,
                UDP_TABLE_OWNER_PID,
                0,
            )
        };
        if first != ERROR_INSUFFICIENT_BUFFER || size == 0 {
            return None;
        }
        let mut buffer = vec![0_u32; (size as usize).div_ceil(size_of::<u32>())];
        let result = unsafe {
            GetExtendedUdpTable(
                buffer.as_mut_ptr().cast::<c_void>(),
                &mut size,
                0,
                address_family,
                UDP_TABLE_OWNER_PID,
                0,
            )
        };
        (result == NO_ERROR).then_some(buffer)
    }

    fn find_ipv4_owner(address: [u8; 4], port: u16) -> Option<u32> {
        let table = tcp_table(AF_INET as u32)?;
        let header = unsafe { &*table.as_ptr().cast::<MIB_TCPTABLE_OWNER_PID>() };
        let rows = unsafe {
            std::slice::from_raw_parts(header.table.as_ptr(), header.dwNumEntries as usize)
        };
        rows.iter()
            .find(|row| {
                row.dwLocalAddr.to_ne_bytes() == address && decode_port(row.dwLocalPort) == port
            })
            .or_else(|| {
                rows.iter()
                    .find(|row| row.dwLocalAddr == 0 && decode_port(row.dwLocalPort) == port)
            })
            .map(|row: &MIB_TCPROW_OWNER_PID| row.dwOwningPid)
    }

    fn find_ipv6_owner(address: [u8; 16], port: u16) -> Option<u32> {
        let table = tcp_table(AF_INET6 as u32)?;
        let header = unsafe { &*table.as_ptr().cast::<MIB_TCP6TABLE_OWNER_PID>() };
        let rows = unsafe {
            std::slice::from_raw_parts(header.table.as_ptr(), header.dwNumEntries as usize)
        };
        rows.iter()
            .find(|row| row.ucLocalAddr == address && decode_port(row.dwLocalPort) == port)
            .or_else(|| {
                rows.iter()
                    .find(|row| row.ucLocalAddr == [0; 16] && decode_port(row.dwLocalPort) == port)
            })
            .map(|row: &MIB_TCP6ROW_OWNER_PID| row.dwOwningPid)
    }

    fn find_ipv4_udp_owner(address: [u8; 4], port: u16) -> Option<u32> {
        let table = udp_table(AF_INET as u32)?;
        let header = unsafe { &*table.as_ptr().cast::<MIB_UDPTABLE_OWNER_PID>() };
        let rows = unsafe {
            std::slice::from_raw_parts(header.table.as_ptr(), header.dwNumEntries as usize)
        };
        rows.iter()
            .find(|row| {
                row.dwLocalAddr.to_ne_bytes() == address && decode_port(row.dwLocalPort) == port
            })
            .or_else(|| {
                rows.iter()
                    .find(|row| row.dwLocalAddr == 0 && decode_port(row.dwLocalPort) == port)
            })
            .map(|row: &MIB_UDPROW_OWNER_PID| row.dwOwningPid)
    }

    fn find_ipv6_udp_owner(address: [u8; 16], port: u16) -> Option<u32> {
        let table = udp_table(AF_INET6 as u32)?;
        let header = unsafe { &*table.as_ptr().cast::<MIB_UDP6TABLE_OWNER_PID>() };
        let rows = unsafe {
            std::slice::from_raw_parts(header.table.as_ptr(), header.dwNumEntries as usize)
        };
        rows.iter()
            .find(|row| row.ucLocalAddr == address && decode_port(row.dwLocalPort) == port)
            .or_else(|| {
                rows.iter()
                    .find(|row| row.ucLocalAddr == [0; 16] && decode_port(row.dwLocalPort) == port)
            })
            .map(|row: &MIB_UDP6ROW_OWNER_PID| row.dwOwningPid)
    }

    fn decode_port(port: u32) -> u16 {
        u16::from_be_bytes((port as u16).to_ne_bytes())
    }

    fn process_info(pid: u32) -> LocalProcessInfo {
        let path = process_path(pid);
        let name = path.as_deref().and_then(|path| {
            std::path::Path::new(path)
                .file_name()
                .map(|name| name.to_string_lossy().into_owned())
        });
        LocalProcessInfo { pid, name, path }
    }

    fn process_path(pid: u32) -> Option<String> {
        let handle = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid) };
        if handle.is_null() {
            return None;
        }
        let mut buffer = vec![0_u16; 32_768];
        let mut length = buffer.len() as u32;
        let succeeded =
            unsafe { QueryFullProcessImageNameW(handle, 0, buffer.as_mut_ptr(), &mut length) } != 0;
        unsafe {
            CloseHandle(handle);
        }
        if !succeeded || length == 0 || length as usize > buffer.len() {
            return None;
        }
        buffer.truncate(length as usize);
        String::from_utf16(&buffer).ok()
    }
}

#[cfg(target_os = "macos")]
mod platform {
    use std::net::SocketAddr;

    use libproc::libproc::proc_pid;
    use netstat2::{get_sockets_info, AddressFamilyFlags, ProtocolFlags, ProtocolSocketInfo};

    use super::LocalProcessInfo;

    pub(super) fn lookup_tcp(source: SocketAddr) -> Option<LocalProcessInfo> {
        lookup(source, ProtocolFlags::TCP)
    }

    pub(super) fn lookup_udp(source: SocketAddr) -> Option<LocalProcessInfo> {
        lookup(source, ProtocolFlags::UDP)
    }

    fn lookup(source: SocketAddr, protocol: ProtocolFlags) -> Option<LocalProcessInfo> {
        let sockets = get_sockets_info(
            AddressFamilyFlags::IPV4 | AddressFamilyFlags::IPV6,
            protocol,
        )
        .ok()?;
        let pid = sockets.into_iter().find_map(|socket| {
            let matches = match socket.protocol_socket_info {
                ProtocolSocketInfo::Tcp(tcp) => {
                    (tcp.local_addr == source.ip() || tcp.local_addr.is_unspecified())
                        && tcp.local_port == source.port()
                }
                ProtocolSocketInfo::Udp(udp) => {
                    (udp.local_addr == source.ip() || udp.local_addr.is_unspecified())
                        && udp.local_port == source.port()
                }
            };
            matches
                .then(|| socket.associated_pids.into_iter().next())
                .flatten()
        })?;
        let path = proc_pid::pidpath(pid as i32).ok();
        let name = proc_pid::name(pid as i32).ok().or_else(|| {
            path.as_deref().and_then(|path| {
                std::path::Path::new(path)
                    .file_name()
                    .map(|name| name.to_string_lossy().into_owned())
            })
        });
        Some(LocalProcessInfo { pid, name, path })
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
mod platform {
    use super::{LocalProcessInfo, SocketAddr};

    pub(super) fn lookup_tcp(_source: SocketAddr) -> Option<LocalProcessInfo> {
        None
    }

    pub(super) fn lookup_udp(_source: SocketAddr) -> Option<LocalProcessInfo> {
        None
    }
}
