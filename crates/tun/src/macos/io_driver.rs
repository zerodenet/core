//! Fixed original utun workspaces; no per-poll framing allocation.
use super::*;
use std::{
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

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
                        inner.get_ref().as_raw_fd(),
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
        let this = self.get_mut();
        let Some(length) = buf
            .len()
            .checked_add(4)
            .filter(|length| *length <= this.write_buffer.len())
        else {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "utun packet exceeds its original fixed write workspace",
            )));
        };
        this.write_buffer[..4].copy_from_slice(&family.to_be_bytes());
        this.write_buffer[4..length].copy_from_slice(buf);
        let framed = &this.write_buffer[..length];
        loop {
            let mut guard = match this.fd.poll_write_ready(cx) {
                Poll::Ready(Ok(g)) => g,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => return Poll::Pending,
            };
            match guard.try_io(|inner| {
                let ret = unsafe {
                    libc::write(
                        inner.get_ref().as_raw_fd(),
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
