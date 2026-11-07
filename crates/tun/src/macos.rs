//! macOS utun device via `SYSPROTO_CONTROL` socket.
//!
//! Creates a virtual network interface using XNU's built-in utun driver.
//! The socket provides raw IP packet I/O — no Ethernet header.
//!
//! Reference: <https://developer.apple.com/documentation/networkextension>

use std::io;
use std::net::IpAddr;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};

use tokio::io::unix::AsyncFd;

use crate::TunDevice;
mod address;
mod creation;
use address::ipv4_peer;
mod io_driver;
pub(crate) mod memory;

// XNU system controls
const SYSPROTO_CONTROL: libc::c_int = 2; // SYSPROTO_CONTROL
const AF_SYSTEM: libc::c_int = 32; // AF_SYSTEM
const CTLIOCGINFO: libc::c_ulong = 0xc064_4e03;
const AF_SYS_CONTROL: u16 = 2;
const UTUN_CONTROL_NAME: &str = "com.apple.net.utun_control";
const UTUN_OPT_IFNAME: libc::c_int = 2;

/// A macOS utun device backed by an `AsyncFd`.
pub struct Utun {
    name: String,
    fd: AsyncFd<OwnedFd>,
    owned: crate::OwnedTunIdentity,
    read_buffer: memory::Buffer,
    write_buffer: memory::Buffer,
    read_start: usize,
    read_end: usize,
}

impl Utun {
    pub(crate) fn adopt(fd: std::os::fd::OwnedFd, expected: &str) -> io::Result<Self> {
        let name = Self::validate_adopt(fd.as_raw_fd(), expected)?;
        let identity = crate::OwnedTunIdentity::from_fd(fd.as_raw_fd());
        Self::register_adopted(fd, name, identity)
    }

    pub(crate) fn validate_adopt(fd: RawFd, expected: &str) -> io::Result<String> {
        let name = interface_name(fd)?;
        if name != expected {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "host descriptor interface mismatch",
            ));
        }
        let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
        if flags < 0 || flags & libc::O_NONBLOCK == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "host packet descriptor must already be nonblocking",
            ));
        }
        Ok(name)
    }

    pub(crate) fn register_adopted(
        fd: OwnedFd,
        name: String,
        owned: crate::OwnedTunIdentity,
    ) -> io::Result<Self> {
        Self::register_with_memory(fd, name, owned, None)
    }
    pub(crate) fn register_with_memory(
        fd: OwnedFd,
        name: String,
        owned: crate::OwnedTunIdentity,
        memory: Option<memory::Buffers>,
    ) -> io::Result<Self> {
        let memory = memory.unwrap_or_else(memory::Buffers::library);
        // SAFETY: the owned original is stable for this async registration.
        let fd = unsafe { AsyncFd::register(fd) }?;
        Ok(Self {
            name,
            owned,
            fd,
            read_buffer: memory.read,
            write_buffer: memory.write,
            read_start: 0,
            read_end: 0,
        })
    }

    /// Create a new utun device. When a name such as `utun8` is supplied,
    /// request that unit; otherwise let the kernel choose an available unit.
    pub fn create(name: Option<&str>) -> io::Result<Self> {
        // The same authorized helper must remain available for interface,
        // route, and PF configuration, even on macOS versions that allow the
        // initial control socket to be opened by an unprivileged process.
        let sock = if unsafe { libc::geteuid() } == 0 {
            create_raw_utun(name)?
        } else {
            crate::macos_privilege::request_utun(name)?
        };

        // SAFETY: create_raw_utun/request_utun transfers one owned descriptor.
        let socket = unsafe { OwnedFd::from_raw_fd(sock) };
        let name = interface_name(socket.as_raw_fd())?;

        // Set non-blocking
        unsafe {
            let flags = libc::fcntl(sock, libc::F_GETFL, 0);
            if flags >= 0 {
                libc::fcntl(sock, libc::F_SETFL, flags | libc::O_NONBLOCK);
            }
        }

        // SAFETY: OwnedFd keeps the descriptor stable and closes it only after
        // AsyncFd deregisters it; construction errors also retain ownership.
        let fd = unsafe { AsyncFd::register(socket) }?;
        Ok(Self {
            name,
            owned: crate::OwnedTunIdentity::from_fd(fd.get_ref().as_raw_fd()),
            fd,
            read_buffer: memory::Buffer::library(),
            write_buffer: memory::Buffer::library(),
            read_start: 0,
            read_end: 0,
        })
    }
}

pub(crate) fn create_raw_utun(name: Option<&str>) -> io::Result<RawFd> {
    let requested_unit = requested_unit(name)?;

    // Find the utun control ID
    let ctl_id = find_utun_control()?;

    // Create system socket
    let sock = unsafe { libc::socket(AF_SYSTEM, libc::SOCK_DGRAM, SYSPROTO_CONTROL) };
    if sock < 0 {
        return Err(io::Error::last_os_error());
    }

    // Connect using sockaddr_ctl. sc_unit is one greater than the utun
    // suffix; zero asks the kernel to allocate the next available unit.
    let mut address: libc::sockaddr_ctl = unsafe { std::mem::zeroed() };
    address.sc_len = std::mem::size_of::<libc::sockaddr_ctl>() as u8;
    address.sc_family = AF_SYSTEM as u8;
    address.ss_sysaddr = AF_SYS_CONTROL;
    address.sc_id = ctl_id;
    address.sc_unit = requested_unit;

    let ret = unsafe {
        libc::connect(
            sock,
            &address as *const _ as *const libc::sockaddr,
            std::mem::size_of::<libc::sockaddr_ctl>() as libc::socklen_t,
        )
    };
    if ret < 0 {
        unsafe { libc::close(sock) };
        return Err(io::Error::last_os_error());
    }

    Ok(sock)
}

fn interface_name(sock: RawFd) -> io::Result<String> {
    let mut ifname: [libc::c_char; 16] = unsafe { std::mem::zeroed() };
    let mut ifname_len: libc::socklen_t = ifname.len() as libc::socklen_t;
    let ret = unsafe {
        libc::getsockopt(
            sock,
            SYSPROTO_CONTROL,
            UTUN_OPT_IFNAME,
            ifname.as_mut_ptr() as *mut libc::c_void,
            &mut ifname_len,
        )
    };
    if ret < 0 {
        return Err(io::Error::last_os_error());
    }

    Ok(ifname
        .iter()
        .take_while(|&&b| b != 0)
        .map(|&b| b as u8 as char)
        .collect::<String>())
}

fn requested_unit(name: Option<&str>) -> io::Result<u32> {
    let Some(name) = name else {
        return Ok(0);
    };
    let suffix = name.strip_prefix("utun").ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "macOS TUN name must use the `utunN` form",
        )
    })?;
    let index: u32 = suffix.parse().map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid macOS utun unit `{name}`: {error}"),
        )
    })?;
    index
        .checked_add(1)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "utun unit overflows"))
}

fn find_utun_control() -> io::Result<u32> {
    let fd = unsafe { libc::socket(AF_SYSTEM, libc::SOCK_DGRAM, SYSPROTO_CONTROL) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }

    let mut info: libc::ctl_info = unsafe { std::mem::zeroed() };
    let name_bytes = UTUN_CONTROL_NAME.as_bytes();
    let copy_len = name_bytes.len().min(info.ctl_name.len() - 1);
    for (i, &b) in name_bytes.iter().take(copy_len).enumerate() {
        info.ctl_name[i] = b as libc::c_char;
    }

    let ret = unsafe { libc::ioctl(fd, CTLIOCGINFO, &mut info as *mut _) };
    unsafe { libc::close(fd) };
    if ret < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(info.ctl_id)
}

impl AsRawFd for Utun {
    fn as_raw_fd(&self) -> RawFd {
        self.fd.get_ref().as_raw_fd()
    }
}

impl crate::OwnedTunDevice for Utun {
    fn owned_identity(&self) -> crate::OwnedTunIdentity {
        self.owned.clone()
    }
    fn close_owned(self) -> io::Result<crate::TunCloseReceipt> {
        crate::owned::close_fd(self.fd.into_inner(), self.name, self.owned)
    }
}

impl TunDevice for Utun {
    fn configure(&self, addr: IpAddr, mask: IpAddr, mtu: u16) -> io::Result<()> {
        if addr.is_ipv4() != mask.is_ipv4() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "TUN address and mask families differ",
            ));
        }
        let mut arguments = vec![self.name.clone()];
        match addr {
            IpAddr::V4(address) => arguments.extend([
                "inet".to_owned(),
                address.to_string(),
                ipv4_peer(address, mask)?.to_string(),
                "netmask".to_owned(),
                mask.to_string(),
            ]),
            IpAddr::V6(_) => arguments.extend([
                "inet6".to_owned(),
                addr.to_string(),
                "prefixlen".to_owned(),
                crate::mask_to_prefix(mask)?.to_string(),
            ]),
        }
        arguments.extend(["mtu".to_owned(), mtu.to_string(), "up".to_owned()]);
        run_ifconfig(&arguments)
    }
    fn name(&self) -> &str {
        &self.name
    }
}

#[cfg(test)]
mod tests;
