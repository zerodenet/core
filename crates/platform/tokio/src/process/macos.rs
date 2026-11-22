//! Darwin libproc queries report target FDs; they never acquire/duplicate them.
//! Bounded PID/FD streaming replaces netstat2's eager unbounded result Vec.
use super::{LocalProcessInfo, LocalProcessLookup, LocalProcessTransport, SocketAddr};
use crate::contract::{ChannelError, Impact, Reason, Stage};
use libproc::libproc::{
    bsd_info::BSDInfo,
    file_info::pidfdinfo,
    net_info::{InSockInfo, SocketFDInfo},
    proc_pid::pidinfo,
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

const MAX_TAIL_SOCKET_PROBES: usize = 4096;

pub(super) fn lookup(
    original: &LocalProcessLookup,
) -> Result<Option<LocalProcessInfo>, Arc<ChannelError>> {
    let limit = original.read_limit().min(i32::MAX as usize);
    if limit < size_of::<SocketFDInfo>().max(size_of::<BSDInfo>()) {
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
    let mut tail_probes = MAX_TAIL_SOCKET_PROBES;
    for pid in pids.into_iter().filter(|pid| *pid > 0) {
        if original.cancelled() {
            return Ok(None);
        }
        if !has_source(original, pid, limit, &mut tail_probes)? {
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
    Ok(None)
}

fn has_source(
    original: &LocalProcessLookup,
    pid: i32,
    limit: usize,
    tail_probes: &mut usize,
) -> Result<bool, Arc<ChannelError>> {
    let needed = unsafe { proc_pidinfo(pid, 1, 0, std::ptr::null_mut(), 0) };
    if needed <= 0 {
        return Ok(false);
    }
    let count = ((needed as usize).div_ceil(size_of::<Fd>()) + 1).min(limit / size_of::<Fd>());
    let bytes = count * size_of::<Fd>();
    let mut descriptors = vec![Fd::default(); count];
    let read = unsafe { proc_pidinfo(pid, 1, 0, descriptors.as_mut_ptr().cast(), bytes as i32) };
    if read <= 0 {
        return Ok(false);
    }
    descriptors.truncate(read as usize / size_of::<Fd>());
    for fd in descriptors.iter().filter(|fd| fd.kind == 2) {
        if original.cancelled() {
            return Ok(false);
        }
        if socket_matches(original, pid, fd.descriptor) {
            return Ok(true);
        }
    }
    if (read as usize) < bytes {
        return Ok(false);
    }

    // PROC_PIDLISTFDS returns an ascending prefix when the buffer fills.
    // BSDInfo exposes the allocated table extent, including any vacant slots.
    let last = descriptors.last().ok_or_else(capacity)?.descriptor;
    let extent = fd_extent(pid)?;
    for fd in last.saturating_add(1)..extent {
        if original.cancelled() {
            return Ok(false);
        }
        *tail_probes = tail_probes.checked_sub(1).ok_or_else(capacity)?;
        if socket_matches(original, pid, fd) {
            return Ok(true);
        }
    }
    if fd_extent(pid)? > extent {
        return Err(capacity());
    }
    Ok(false)
}

fn fd_extent(pid: i32) -> Result<i32, Arc<ChannelError>> {
    let info = pidinfo::<BSDInfo>(pid, 0).map_err(|_| capacity())?;
    i32::try_from(info.pbi_nfiles).map_err(|_| capacity())
}

fn socket_matches(original: &LocalProcessLookup, pid: i32, fd: i32) -> bool {
    let Ok(info) = pidfdinfo::<SocketFDInfo>(pid, fd) else {
        return false;
    };
    let socket = &info.psi;
    let address = match original.transport() {
        LocalProcessTransport::Tcp
            if socket.soi_protocol == libc::IPPROTO_TCP && socket.soi_kind == 2 =>
        unsafe { socket.soi_proto.pri_tcp.tcpsi_ini },
        LocalProcessTransport::Udp
            if socket.soi_protocol == libc::IPPROTO_UDP && socket.soi_kind == 1 =>
        unsafe { socket.soi_proto.pri_in },
        _ => return false,
    };
    matches_source(original.source(), socket.soi_family, address)
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
