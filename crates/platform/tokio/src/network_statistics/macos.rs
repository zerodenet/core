//! NET_RT_IFLIST2 exposes if_data64; unlike getifaddrs, counters do not wrap at 32 bits.
use super::{invalid, InterfaceStatistics, MAX_BYTES, MAX_INTERFACES};
use std::{ffi::CStr, io, mem::size_of};

pub(super) fn read() -> io::Result<Vec<InterfaceStatistics>> {
    let mut mib = [libc::CTL_NET, libc::PF_ROUTE, 0, 0, libc::NET_RT_IFLIST2, 0];
    for _ in 0..3 {
        let mut size = 0;
        // SAFETY: correctly sized MIB and writable size; read-only query.
        if unsafe {
            libc::sysctl(
                mib.as_mut_ptr(),
                6,
                std::ptr::null_mut(),
                &mut size,
                std::ptr::null_mut(),
                0,
            )
        } != 0
        {
            return Err(io::Error::last_os_error());
        }
        if size > MAX_BYTES {
            return Err(invalid("interface statistics exceed byte limit"));
        }
        let mut buffer = vec![0u8; size];
        // SAFETY: buffer has the requested length; sysctl receives its byte capacity.
        if unsafe {
            libc::sysctl(
                mib.as_mut_ptr(),
                6,
                buffer.as_mut_ptr().cast(),
                &mut size,
                std::ptr::null_mut(),
                0,
            )
        } != 0
        {
            let error = io::Error::last_os_error();
            if error.raw_os_error() == Some(libc::ENOMEM) {
                continue;
            }
            return Err(error);
        }
        buffer.truncate(size);
        return parse(&buffer);
    }
    Err(io::Error::new(
        io::ErrorKind::WouldBlock,
        "interface inventory changed during capture",
    ))
}
fn parse(buffer: &[u8]) -> io::Result<Vec<InterfaceStatistics>> {
    let mut offset = 0;
    let mut result = Vec::new();
    while offset < buffer.len() {
        let record = &buffer[offset..];
        if record.len() < 4 {
            return Err(invalid("truncated interface record"));
        }
        let length = u16::from_ne_bytes([record[0], record[1]]) as usize;
        if length < 4 || length > record.len() {
            return Err(invalid("invalid interface record length"));
        }
        if record[2] != libc::RTM_VERSION as u8 {
            return Err(invalid("unsupported interface record version"));
        }
        if record[3] == libc::RTM_IFINFO2 as u8 {
            if length < size_of::<libc::if_msghdr2>() || result.len() >= MAX_INTERFACES {
                return Err(invalid("invalid or excessive interface records"));
            }
            // SAFETY: length validated above; source can be unaligned, and the
            // C header is plain data. No pointer into buffer escapes.
            let header = unsafe { record.as_ptr().cast::<libc::if_msghdr2>().read_unaligned() };
            let mut name = [0 as libc::c_char; libc::IF_NAMESIZE];
            // SAFETY: sufficient writable buffer, index supplied by OS.
            if unsafe { libc::if_indextoname(header.ifm_index.into(), name.as_mut_ptr()) }.is_null()
            {
                return Err(io::Error::last_os_error());
            }
            // SAFETY: if_indextoname succeeded and nul-terminated this buffer.
            let name = unsafe { CStr::from_ptr(name.as_ptr()) }
                .to_str()
                .map_err(|_| invalid("invalid interface name"))?
                .to_owned();
            let data = header.ifm_data;
            result.push(InterfaceStatistics {
                name,
                index: header.ifm_index.into(),
                accounting_basis:
                    "macos_if_data64_host_interface_rx_queue_drops_tx_drops_unavailable",
                rx_bytes: data.ifi_ibytes,
                tx_bytes: data.ifi_obytes,
                rx_packets: data.ifi_ipackets,
                tx_packets: data.ifi_opackets,
                rx_dropped_packets: Some(data.ifi_iqdrops),
                tx_dropped_packets: None,
                rx_errors: Some(data.ifi_ierrors),
                tx_errors: Some(data.ifi_oerrors),
            });
        }
        offset += length;
    }
    Ok(result)
}
#[cfg(test)]
#[path = "../../tests/network_statistics/macos.rs"]
mod tests;
