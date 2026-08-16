use std::fs::File;
use std::io;
use std::os::fd::{AsRawFd, FromRawFd};

use tokio::io::unix::AsyncFd;

#[derive(Debug)]
pub(super) struct RouteChangeMonitor {
    socket: AsyncFd<File>,
}

impl RouteChangeMonitor {
    pub(super) fn new() -> io::Result<Self> {
        let descriptor = unsafe { libc::socket(libc::PF_ROUTE, libc::SOCK_RAW, libc::AF_UNSPEC) };
        if descriptor < 0 {
            return Err(io::Error::last_os_error());
        }
        let socket = unsafe { File::from_raw_fd(descriptor) };
        let flags = unsafe { libc::fcntl(socket.as_raw_fd(), libc::F_GETFL) };
        if flags < 0
            || unsafe { libc::fcntl(socket.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) }
                < 0
        {
            return Err(io::Error::last_os_error());
        }
        AsyncFd::new(socket).map(|socket| Self { socket })
    }

    pub(super) async fn changed(&mut self) -> io::Result<()> {
        let mut buffer = [0_u8; 8192];
        loop {
            let mut ready = self.socket.readable().await?;
            match ready.try_io(|socket| {
                let received = unsafe {
                    libc::recv(
                        socket.get_ref().as_raw_fd(),
                        buffer.as_mut_ptr().cast(),
                        buffer.len(),
                        0,
                    )
                };
                if received > 0 {
                    Ok(())
                } else if received == 0 {
                    Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "macOS route notification socket closed",
                    ))
                } else {
                    Err(io::Error::last_os_error())
                }
            }) {
                Ok(result) => return result,
                Err(_would_block) => continue,
            }
        }
    }

    pub(super) fn coalesce(&mut self) -> io::Result<()> {
        let mut buffer = [0_u8; 8192];
        loop {
            let received = unsafe {
                libc::recv(
                    self.socket.get_ref().as_raw_fd(),
                    buffer.as_mut_ptr().cast(),
                    buffer.len(),
                    0,
                )
            };
            if received > 0 {
                continue;
            }
            if received == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "macOS route notification socket closed",
                ));
            }
            let error = io::Error::last_os_error();
            return if error.kind() == io::ErrorKind::WouldBlock {
                Ok(())
            } else {
                Err(error)
            };
        }
    }
}
