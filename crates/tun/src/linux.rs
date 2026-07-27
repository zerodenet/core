//! Linux TUN device via `/dev/net/tun`.

use std::ffi::CString;
use std::io;
use std::net::IpAddr;
use std::os::unix::io::{AsRawFd, RawFd};
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::unix::AsyncFd;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use crate::TunDevice;

const IFF_TUN: libc::c_int = 0x0001;
const IFF_NO_PI: libc::c_int = 0x1000;

/// A Linux TUN device backed by an `AsyncFd`.
pub struct LinuxTun {
    name: String,
    fd: AsyncFd<RawFd>,
}

impl LinuxTun {
    /// Create a new TUN device.  `name` is the desired interface name
    /// (e.g. `"tun%d"`); the kernel may assign a different index.
    pub fn create(name: Option<&str>) -> io::Result<Self> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open("/dev/net/tun")
            .map_err(|e| io::Error::new(e.kind(), format!("open /dev/net/tun: {e}")))?;

        let fd = file.as_raw_fd();
        let if_name = name.unwrap_or("tun%d");

        let mut ifr: libc::ifreq = unsafe { std::mem::zeroed() };
        let name_c = CString::new(if_name).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "interface name contains nul byte",
            )
        })?;
        let name_bytes = name_c.as_bytes_with_nul();
        let copy_len = name_bytes.len().min(libc::IFNAMSIZ);
        // SAFETY: c_char is i8 on Linux; casting &[u8] → &[i8] is safe.
        let name_chars: &[libc::c_char] = unsafe {
            std::slice::from_raw_parts(name_bytes.as_ptr() as *const libc::c_char, copy_len)
        };
        ifr.ifr_name[..copy_len].copy_from_slice(name_chars);

        ifr.ifr_ifru.ifru_flags = (IFF_TUN | IFF_NO_PI) as i16;

        // SAFETY: ifr is correctly sized and initialized.
        let ret = unsafe { libc::ioctl(fd, libc::TUNSETIFF, &ifr as *const _) };
        if ret < 0 {
            return Err(io::Error::last_os_error());
        }

        // Don't close the fd when `file` drops.
        std::mem::forget(file);

        // Set non-blocking so AsyncFd's epoll works correctly.
        unsafe {
            let flags = libc::fcntl(fd, libc::F_GETFL, 0);
            if flags >= 0 {
                libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK);
            }
        }

        let actual_name = ifr
            .ifr_name
            .iter()
            .take_while(|&&b| b != 0)
            .map(|&b| b as u8 as char)
            .collect::<String>();

        let async_fd = AsyncFd::new(fd)?;

        Ok(Self {
            name: actual_name,
            fd: async_fd,
        })
    }
}

impl AsRawFd for LinuxTun {
    fn as_raw_fd(&self) -> RawFd {
        *self.fd.get_ref()
    }
}

impl Drop for LinuxTun {
    fn drop(&mut self) {
        // SAFETY: fd is valid.
        unsafe { libc::close(*self.fd.get_ref()) };
    }
}

impl TunDevice for LinuxTun {
    fn configure(&self, _addr: IpAddr, _mask: IpAddr, _mtu: u16) -> io::Result<()> {
        Ok(())
    }

    fn name(&self) -> &str {
        &self.name
    }
}

impl AsyncRead for LinuxTun {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        loop {
            let mut guard = match self.fd.poll_read_ready(cx) {
                Poll::Ready(Ok(g)) => g,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => return Poll::Pending,
            };

            match guard.try_io(|inner| {
                let fd = inner.get_ref();
                // SAFETY: read from a valid fd into a caller-provided buffer.
                let ret = unsafe {
                    libc::read(
                        *fd,
                        buf.initialize_unfilled().as_mut_ptr() as *mut libc::c_void,
                        buf.remaining(),
                    )
                };
                if ret < 0 {
                    return Err(io::Error::last_os_error());
                }
                Ok(ret as usize)
            }) {
                Ok(Ok(n)) => {
                    // SAFETY: kernel wrote `n` bytes into the buffer.
                    unsafe { buf.assume_init(n) };
                    buf.advance(n);
                    return Poll::Ready(Ok(()));
                }
                Ok(Err(e)) => return Poll::Ready(Err(e)),
                Err(_would_block) => continue,
            }
        }
    }
}

impl AsyncWrite for LinuxTun {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        loop {
            let mut guard = match self.fd.poll_write_ready(cx) {
                Poll::Ready(Ok(g)) => g,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => return Poll::Pending,
            };

            match guard.try_io(|inner| {
                let fd = inner.get_ref();
                // SAFETY: write to a valid fd.
                let ret =
                    unsafe { libc::write(*fd, buf.as_ptr() as *const libc::c_void, buf.len()) };
                if ret < 0 {
                    return Err(io::Error::last_os_error());
                }
                Ok(ret as usize)
            }) {
                Ok(Ok(n)) => return Poll::Ready(Ok(n)),
                Ok(Err(e)) => return Poll::Ready(Err(e)),
                Err(_would_block) => continue,
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Requires root: `sudo cargo test -p zero-tun -- --ignored`
    #[test]
    #[ignore]
    fn test_create_linux_tun() {
        let tun = LinuxTun::create(Some("tun%d")).expect("create tun");
        assert!(!tun.name().is_empty());
        assert!(tun.name().starts_with("tun"));
        drop(tun);
    }
}
