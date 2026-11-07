//! Darwin libproc queries report target FDs; they never acquire/duplicate them.
//! Bounded PID/FD streaming replaces netstat2's eager unbounded result Vec.
use super::{LocalProcessInfo, LocalProcessLookup, LocalProcessTransport, SocketAddr};
use crate::contract::{ChannelError, Impact, Reason, Stage};
use libproc::libproc::{
    file_info::pidfdinfo,
    net_info::{InSockInfo, SocketFDInfo},
};
use std::{
    ffi::c_void,
    mem::size_of,
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
    sync::Arc,
};
#[link(name = "proc")]
unsafe extern "C" {
    fn proc_listpids(kind: u32, id: u32, buffer: *mut c_void, size: i32) -> i32;
    fn proc_pidinfo(pid: i32, flavor: i32, arg: u64, buffer: *mut c_void, size: i32) -> i32;
    fn proc_pidpath(pid: i32, buffer: *mut c_void, size: u32) -> i32;
    fn proc_name(pid: i32, buffer: *mut c_void, size: u32) -> i32;
}
#[repr(C)]
#[derive(Clone, Copy, Default)]
struct Fd {
    descriptor: i32,
    kind: u32,
}

pub(super) fn lookup(
    original: &LocalProcessLookup,
) -> Result<Option<LocalProcessInfo>, Arc<ChannelError>> {
    let limit = original.read_limit().min(i32::MAX as usize);
    if limit < size_of::<SocketFDInfo>() {
        return Err(capacity());
    }
    let needed = unsafe { proc_listpids(1, 0, std::ptr::null_mut(), 0) };
    if needed <= 0 {
        return Ok(None);
    }
    let count = (needed as usize)
        .div_ceil(size_of::<i32>())
        .checked_add(1)
        .ok_or_else(capacity)?;
    let bytes = count
        .checked_mul(size_of::<i32>())
        .filter(|bytes| *bytes <= limit)
        .ok_or_else(capacity)?;
    let mut pids = vec![0_i32; count];
    let read = unsafe { proc_listpids(1, 0, pids.as_mut_ptr().cast(), bytes as i32) };
    if read <= 0 {
        return Ok(None);
    }
    if read as usize >= bytes {
        return Err(capacity());
    }
    pids.truncate(read as usize / size_of::<i32>());
    for pid in pids.into_iter().filter(|pid| *pid > 0) {
        if original.cancelled() {
            return Ok(None);
        }
        let needed = unsafe { proc_pidinfo(pid, 1, 0, std::ptr::null_mut(), 0) };
        if needed <= 0 {
            continue;
        }
        let count = (needed as usize)
            .div_ceil(size_of::<Fd>())
            .checked_add(1)
            .ok_or_else(capacity)?;
        let bytes = count
            .checked_mul(size_of::<Fd>())
            .filter(|bytes| *bytes <= limit)
            .ok_or_else(capacity)?;
        let mut descriptors = vec![Fd::default(); count];
        let read =
            unsafe { proc_pidinfo(pid, 1, 0, descriptors.as_mut_ptr().cast(), bytes as i32) };
        if read <= 0 {
            continue;
        }
        if read as usize >= bytes {
            return Err(capacity());
        }
        descriptors.truncate(read as usize / size_of::<Fd>());
        for fd in descriptors.into_iter().filter(|fd| fd.kind == 2) {
            if original.cancelled() {
                return Ok(None);
            }
            let Ok(info) = pidfdinfo::<SocketFDInfo>(pid, fd.descriptor) else {
                continue;
            };
            let socket = &info.psi;
            let address = match original.transport() {
                LocalProcessTransport::Tcp
                    if socket.soi_protocol == libc::IPPROTO_TCP && socket.soi_kind == 2 =>
                unsafe { socket.soi_proto.pri_tcp.tcpsi_ini },
                LocalProcessTransport::Udp
                    if socket.soi_protocol == libc::IPPROTO_UDP && socket.soi_kind == 1 =>
                unsafe { socket.soi_proto.pri_in },
                _ => continue,
            };
            if !matches_source(original.source(), socket.soi_family, address) {
                continue;
            }
            let path = text(pid, limit, false)?;
            let name = text(pid, limit, true)?.or_else(|| {
                path.as_deref()
                    .and_then(|path| std::path::Path::new(path).file_name())
                    .map(|name| name.to_string_lossy().into_owned())
            });
            return Ok(Some(LocalProcessInfo {
                pid: pid as u32,
                name,
                path,
            }));
        }
    }
    Ok(None)
}
fn matches_source(source: SocketAddr, family: i32, info: InSockInfo) -> bool {
    if u16::from_be(info.insi_lport as u16) != source.port() {
        return false;
    }
    let address = match family {
        libc::AF_INET => IpAddr::V4(Ipv4Addr::from(
            unsafe { info.insi_laddr.ina_46.i46a_addr4.s_addr }.to_ne_bytes(),
        )),
        libc::AF_INET6 => IpAddr::V6(Ipv6Addr::from(unsafe { info.insi_laddr.ina_6.s6_addr })),
        _ => return false,
    };
    address == source.ip() || address.is_unspecified()
}
fn text(pid: i32, limit: usize, name: bool) -> Result<Option<String>, Arc<ChannelError>> {
    // Darwin's own PROC_PIDPATHINFO_MAXSIZE is 4096; configured smaller bounds
    // still apply. No whole-system result list or opaque handle is retained.
    let mut bytes = vec![0_u8; limit.min(4096)];
    let read = unsafe {
        if name {
            proc_name(pid, bytes.as_mut_ptr().cast(), bytes.len() as u32)
        } else {
            proc_pidpath(pid, bytes.as_mut_ptr().cast(), bytes.len() as u32)
        }
    };
    if read <= 0 {
        return match std::io::Error::last_os_error().raw_os_error() {
            Some(libc::ENOMEM | libc::ERANGE) => Err(capacity()),
            Some(libc::EINVAL) if bytes.len() < 4096 => Err(capacity()),
            _ => Ok(None),
        };
    }
    let length = bytes
        .iter()
        .position(|byte| *byte == 0)
        .unwrap_or(bytes.len());
    if length == bytes.len() {
        return Err(capacity());
    }
    bytes.truncate(length);
    Ok(String::from_utf8(bytes).ok())
}
fn capacity() -> Arc<ChannelError> {
    Arc::new(ChannelError::new(
        Stage::Prepare,
        Reason::Capacity,
        Impact::Session,
    ))
}
