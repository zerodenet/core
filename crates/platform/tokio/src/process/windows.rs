use std::ffi::c_void;
use std::mem::size_of;
use std::net::{IpAddr, SocketAddr};

use windows_sys::Win32::Foundation::{ERROR_INSUFFICIENT_BUFFER, NO_ERROR};
use windows_sys::Win32::NetworkManagement::IpHelper::{
    GetExtendedTcpTable, GetExtendedUdpTable, MIB_TCP6ROW_OWNER_PID, MIB_TCP6TABLE_OWNER_PID,
    MIB_TCPROW_OWNER_PID, MIB_TCPTABLE_OWNER_PID, MIB_UDP6ROW_OWNER_PID, MIB_UDP6TABLE_OWNER_PID,
    MIB_UDPROW_OWNER_PID, MIB_UDPTABLE_OWNER_PID, TCP_TABLE_OWNER_PID_ALL, UDP_TABLE_OWNER_PID,
};
use windows_sys::Win32::Networking::WinSock::{AF_INET, AF_INET6};
use windows_sys::Win32::System::Threading::{
    OpenProcess, QueryFullProcessImageNameW, PROCESS_QUERY_LIMITED_INFORMATION,
};

use super::{LocalProcessInfo, LocalProcessLookup, LocalProcessTransport};
use crate::contract::{ChannelError, Impact, Reason, Stage};
use std::sync::Arc;

pub(super) fn lookup(
    original: &LocalProcessLookup,
) -> Result<Option<LocalProcessInfo>, Arc<ChannelError>> {
    if original.cancelled() {
        return Ok(None);
    }
    let source = original.source();
    let limit = original.read_limit();
    let pid = match (original.transport(), source.ip()) {
        (LocalProcessTransport::Tcp, IpAddr::V4(ip)) => {
            find_ipv4_owner(ip.octets(), source.port(), limit)?
        }
        (LocalProcessTransport::Tcp, IpAddr::V6(ip)) => {
            find_ipv6_owner(ip.octets(), source.port(), limit)?
        }
        (LocalProcessTransport::Udp, IpAddr::V4(ip)) => {
            find_ipv4_udp_owner(ip.octets(), source.port(), limit)?
        }
        (LocalProcessTransport::Udp, IpAddr::V6(ip)) => {
            find_ipv6_udp_owner(ip.octets(), source.port(), limit)?
        }
    };
    let Some(pid) = pid else {
        return Ok(None);
    };
    let path = process_path(original, pid)?;
    let name = path
        .as_deref()
        .and_then(|path| std::path::Path::new(path).file_name())
        .map(|name| name.to_string_lossy().into_owned());
    Ok(Some(LocalProcessInfo { pid, name, path }))
}
fn tcp_table(family: u32, limit: usize) -> Result<Option<Vec<u32>>, Arc<ChannelError>> {
    let mut size = 0_u32;
    let first = unsafe {
        GetExtendedTcpTable(
            std::ptr::null_mut(),
            &mut size,
            0,
            family,
            TCP_TABLE_OWNER_PID_ALL,
            0,
        )
    };
    if first != ERROR_INSUFFICIENT_BUFFER || size == 0 {
        return Ok(None);
    }
    let words = (size as usize).div_ceil(size_of::<u32>());
    if words
        .checked_mul(size_of::<u32>())
        .is_none_or(|bytes| bytes > limit)
    {
        return Err(capacity());
    }
    let mut buffer = vec![0_u32; words];
    let result = unsafe {
        GetExtendedTcpTable(
            buffer.as_mut_ptr().cast::<c_void>(),
            &mut size,
            0,
            family,
            TCP_TABLE_OWNER_PID_ALL,
            0,
        )
    };
    if result == ERROR_INSUFFICIENT_BUFFER {
        return Err(capacity());
    }
    Ok((result == NO_ERROR).then_some(buffer))
}
fn udp_table(family: u32, limit: usize) -> Result<Option<Vec<u32>>, Arc<ChannelError>> {
    let mut size = 0_u32;
    let first = unsafe {
        GetExtendedUdpTable(
            std::ptr::null_mut(),
            &mut size,
            0,
            family,
            UDP_TABLE_OWNER_PID,
            0,
        )
    };
    if first != ERROR_INSUFFICIENT_BUFFER || size == 0 {
        return Ok(None);
    }
    let words = (size as usize).div_ceil(size_of::<u32>());
    if words
        .checked_mul(size_of::<u32>())
        .is_none_or(|bytes| bytes > limit)
    {
        return Err(capacity());
    }
    let mut buffer = vec![0_u32; words];
    let result = unsafe {
        GetExtendedUdpTable(
            buffer.as_mut_ptr().cast::<c_void>(),
            &mut size,
            0,
            family,
            UDP_TABLE_OWNER_PID,
            0,
        )
    };
    if result == ERROR_INSUFFICIENT_BUFFER {
        return Err(capacity());
    }
    Ok((result == NO_ERROR).then_some(buffer))
}
fn find_ipv4_owner(
    address: [u8; 4],
    port: u16,
    limit: usize,
) -> Result<Option<u32>, Arc<ChannelError>> {
    let Some(table) = tcp_table(AF_INET as u32, limit)? else {
        return Ok(None);
    };
    let count = *table.first().ok_or_else(capacity)? as usize;
    let offset = std::mem::offset_of!(MIB_TCPTABLE_OWNER_PID, table);
    count
        .checked_mul(size_of::<MIB_TCPROW_OWNER_PID>())
        .and_then(|bytes| bytes.checked_add(offset))
        .filter(|bytes| *bytes <= table.len() * size_of::<u32>())
        .ok_or_else(capacity)?;
    let rows = unsafe {
        std::slice::from_raw_parts(
            table
                .as_ptr()
                .cast::<u8>()
                .add(offset)
                .cast::<MIB_TCPROW_OWNER_PID>(),
            count,
        )
    };
    Ok(rows
        .iter()
        .find(|row| {
            row.dwLocalAddr.to_ne_bytes() == address && decode_port(row.dwLocalPort) == port
        })
        .or_else(|| {
            rows.iter()
                .find(|row| row.dwLocalAddr == 0 && decode_port(row.dwLocalPort) == port)
        })
        .map(|row| row.dwOwningPid))
}
fn find_ipv6_owner(
    address: [u8; 16],
    port: u16,
    limit: usize,
) -> Result<Option<u32>, Arc<ChannelError>> {
    let Some(table) = tcp_table(AF_INET6 as u32, limit)? else {
        return Ok(None);
    };
    let count = *table.first().ok_or_else(capacity)? as usize;
    let offset = std::mem::offset_of!(MIB_TCP6TABLE_OWNER_PID, table);
    count
        .checked_mul(size_of::<MIB_TCP6ROW_OWNER_PID>())
        .and_then(|bytes| bytes.checked_add(offset))
        .filter(|bytes| *bytes <= table.len() * size_of::<u32>())
        .ok_or_else(capacity)?;
    let rows = unsafe {
        std::slice::from_raw_parts(
            table
                .as_ptr()
                .cast::<u8>()
                .add(offset)
                .cast::<MIB_TCP6ROW_OWNER_PID>(),
            count,
        )
    };
    Ok(rows
        .iter()
        .find(|row| row.ucLocalAddr == address && decode_port(row.dwLocalPort) == port)
        .or_else(|| {
            rows.iter()
                .find(|row| row.ucLocalAddr == [0; 16] && decode_port(row.dwLocalPort) == port)
        })
        .map(|row| row.dwOwningPid))
}
fn find_ipv4_udp_owner(
    address: [u8; 4],
    port: u16,
    limit: usize,
) -> Result<Option<u32>, Arc<ChannelError>> {
    let Some(table) = udp_table(AF_INET as u32, limit)? else {
        return Ok(None);
    };
    let count = *table.first().ok_or_else(capacity)? as usize;
    let offset = std::mem::offset_of!(MIB_UDPTABLE_OWNER_PID, table);
    count
        .checked_mul(size_of::<MIB_UDPROW_OWNER_PID>())
        .and_then(|bytes| bytes.checked_add(offset))
        .filter(|bytes| *bytes <= table.len() * size_of::<u32>())
        .ok_or_else(capacity)?;
    let rows = unsafe {
        std::slice::from_raw_parts(
            table
                .as_ptr()
                .cast::<u8>()
                .add(offset)
                .cast::<MIB_UDPROW_OWNER_PID>(),
            count,
        )
    };
    Ok(rows
        .iter()
        .find(|row| {
            row.dwLocalAddr.to_ne_bytes() == address && decode_port(row.dwLocalPort) == port
        })
        .or_else(|| {
            rows.iter()
                .find(|row| row.dwLocalAddr == 0 && decode_port(row.dwLocalPort) == port)
        })
        .map(|row| row.dwOwningPid))
}
fn find_ipv6_udp_owner(
    address: [u8; 16],
    port: u16,
    limit: usize,
) -> Result<Option<u32>, Arc<ChannelError>> {
    let Some(table) = udp_table(AF_INET6 as u32, limit)? else {
        return Ok(None);
    };
    let count = *table.first().ok_or_else(capacity)? as usize;
    let offset = std::mem::offset_of!(MIB_UDP6TABLE_OWNER_PID, table);
    count
        .checked_mul(size_of::<MIB_UDP6ROW_OWNER_PID>())
        .and_then(|bytes| bytes.checked_add(offset))
        .filter(|bytes| *bytes <= table.len() * size_of::<u32>())
        .ok_or_else(capacity)?;
    let rows = unsafe {
        std::slice::from_raw_parts(
            table
                .as_ptr()
                .cast::<u8>()
                .add(offset)
                .cast::<MIB_UDP6ROW_OWNER_PID>(),
            count,
        )
    };
    Ok(rows
        .iter()
        .find(|row| row.ucLocalAddr == address && decode_port(row.dwLocalPort) == port)
        .or_else(|| {
            rows.iter()
                .find(|row| row.ucLocalAddr == [0; 16] && decode_port(row.dwLocalPort) == port)
        })
        .map(|row| row.dwOwningPid))
}
fn decode_port(port: u32) -> u16 {
    u16::from_be_bytes((port as u16).to_ne_bytes())
}
fn process_path(
    original: &LocalProcessLookup,
    pid: u32,
) -> Result<Option<String>, Arc<ChannelError>> {
    let units = original
        .read_limit()
        .checked_div(2)
        .filter(|units| *units != 0)
        .ok_or_else(capacity)?
        .min(32_768);
    if original.cancelled() {
        return Ok(None);
    }
    original.begin_native()?;
    let handle = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid) };
    original.opened_native(handle as usize);
    if handle.is_null() {
        return Ok(None);
    }
    let mut buffer = vec![0_u16; units];
    let mut length = buffer.len() as u32;
    let succeeded =
        unsafe { QueryFullProcessImageNameW(handle, 0, buffer.as_mut_ptr(), &mut length) } != 0;
    let query_error = (!succeeded).then(std::io::Error::last_os_error);
    original.close_native(handle as usize)?;
    if query_error
        .as_ref()
        .is_some_and(|source| source.raw_os_error() == Some(ERROR_INSUFFICIENT_BUFFER as i32))
    {
        return Err(capacity());
    }
    if !succeeded || length == 0 || length as usize > buffer.len() {
        return Ok(None);
    }
    buffer.truncate(length as usize);
    Ok(String::from_utf16(&buffer).ok())
}
fn capacity() -> Arc<ChannelError> {
    Arc::new(ChannelError::new(
        Stage::Prepare,
        Reason::Capacity,
        Impact::Session,
    ))
}
