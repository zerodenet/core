//! macOS utun device via `SYSPROTO_CONTROL` socket.
//!
//! Creates a virtual network interface using XNU's built-in utun driver.
//! The socket provides raw IP packet I/O — no Ethernet header.
//!
//! Reference: <https://developer.apple.com/documentation/networkextension>

use std::io;
use std::net::IpAddr;
use std::os::unix::io::{AsRawFd, RawFd};
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::unix::AsyncFd;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use crate::TunDevice;

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
    fd: AsyncFd<RawFd>,
    read_buffer: Vec<u8>,
    read_start: usize,
    read_end: usize,
}

impl Utun {
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

        let name = interface_name(sock).inspect_err(|_| unsafe {
            libc::close(sock);
        })?;

        // Set non-blocking
        unsafe {
            let flags = libc::fcntl(sock, libc::F_GETFL, 0);
            if flags >= 0 {
                libc::fcntl(sock, libc::F_SETFL, flags | libc::O_NONBLOCK);
            }
        }

        let fd = AsyncFd::new(sock).inspect_err(|_| {
            // SAFETY: the raw socket has not yet been transferred into Utun.
            unsafe { libc::close(sock) };
        })?;
        Ok(Self {
            name,
            fd,
            read_buffer: vec![0_u8; 65_540],
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
        *self.fd.get_ref()
    }
}

impl Drop for Utun {
    fn drop(&mut self) {
        unsafe { libc::close(*self.fd.get_ref()) };
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

pub(crate) fn ipv4_peer(
    address: std::net::Ipv4Addr,
    mask: IpAddr,
) -> io::Result<std::net::Ipv4Addr> {
    let IpAddr::V4(mask) = mask else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "macOS IPv4 TUN address requires an IPv4 netmask",
        ));
    };
    let address = u32::from(address);
    let mask = u32::from(mask);
    let network = address & mask;
    let last = network | !mask;
    let peer = if address < last {
        address + 1
    } else if address > network {
        address - 1
    } else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "macOS IPv4 TUN subnet must contain a distinct point-to-point peer",
        ));
    };
    Ok(peer.into())
}

#[cfg(test)]
mod tests;

// ── Async I/O ─────────────────────────────────────────────────────────

impl AsyncRead for Utun {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if this.read_start < this.read_end {
            let size = (this.read_end - this.read_start).min(buf.remaining());
            buf.put_slice(&this.read_buffer[this.read_start..this.read_start + size]);
            this.read_start += size;
            return Poll::Ready(Ok(()));
        }
        loop {
            let packet_ptr = this.read_buffer.as_mut_ptr();
            let packet_capacity = this.read_buffer.len();
            let mut guard = match this.fd.poll_read_ready(cx) {
                Poll::Ready(Ok(g)) => g,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => return Poll::Pending,
            };
            match guard.try_io(|inner| {
                let ret = unsafe {
                    libc::read(
                        *inner.get_ref(),
                        packet_ptr as *mut libc::c_void,
                        packet_capacity,
                    )
                };
                if ret < 0 {
                    Err(io::Error::last_os_error())
                } else {
                    Ok(ret as usize)
                }
            }) {
                Ok(Ok(n)) => {
                    if n <= 4 {
                        return Poll::Ready(Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "utun packet is missing the address-family header",
                        )));
                    }
                    this.read_start = 4;
                    this.read_end = n;
                    let size = (n - 4).min(buf.remaining());
                    buf.put_slice(&this.read_buffer[4..4 + size]);
                    this.read_start += size;
                    return Poll::Ready(Ok(()));
                }
                Ok(Err(e)) => return Poll::Ready(Err(e)),
                Err(_) => continue,
            }
        }
    }
}

impl AsyncWrite for Utun {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let family = match buf.first().map(|byte| byte >> 4) {
            Some(4) => libc::AF_INET as u32,
            Some(6) => libc::AF_INET6 as u32,
            _ => {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "utun write requires an IPv4 or IPv6 packet",
                )))
            }
        };
        let mut framed = Vec::with_capacity(buf.len() + 4);
        framed.extend_from_slice(&family.to_be_bytes());
        framed.extend_from_slice(buf);
        loop {
            let mut guard = match self.fd.poll_write_ready(cx) {
                Poll::Ready(Ok(g)) => g,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => return Poll::Pending,
            };
            match guard.try_io(|inner| {
                let ret = unsafe {
                    libc::write(
                        *inner.get_ref(),
                        framed.as_ptr() as *const libc::c_void,
                        framed.len(),
                    )
                };
                if ret < 0 {
                    Err(io::Error::last_os_error())
                } else {
                    Ok(ret as usize)
                }
            }) {
                Ok(Ok(n)) if n == framed.len() => return Poll::Ready(Ok(buf.len())),
                Ok(Ok(_)) => {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "partial utun packet write",
                    )))
                }
                Ok(Err(e)) => return Poll::Ready(Err(e)),
                Err(_) => continue,
            }
        }
    }
    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

fn run_ifconfig(arguments: &[String]) -> io::Result<()> {
    let program = if std::path::Path::new("/sbin/ifconfig").exists() {
        "/sbin/ifconfig"
    } else {
        "ifconfig"
    };
    let output = crate::macos_privilege::output(program, arguments)
        .map_err(|error| io::Error::new(error.kind(), format!("execute `{program}`: {error}")))?;
    if output.status.success() {
        return Ok(());
    }
    Err(io::Error::other(format!(
        "`ifconfig {}` failed: {}",
        arguments.join(" "),
        String::from_utf8_lossy(&output.stderr).trim()
    )))
}
