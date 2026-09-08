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
                    Ok(is_topology_change(&buffer[..received as usize]))
                } else if received == 0 {
                    Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "macOS route notification socket closed",
                    ))
                } else {
                    Err(io::Error::last_os_error())
                }
            }) {
                Ok(Ok(true)) => return Ok(()),
                Ok(Ok(false)) => continue,
                Ok(Err(error)) => return Err(error),
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

// Route queries also arrive on PF_ROUTE. Treating RTM_GET/GET2 responses as
// changes makes audits trigger more audits. Only actual topology mutations
// invalidate our view; a watchdog covers missed or unfamiliar notifications.
fn is_topology_change(message: &[u8]) -> bool {
    if message.len() < 4 || message[2] as i32 != libc::RTM_VERSION {
        return false;
    }
    let length = u16::from_ne_bytes([message[0], message[1]]) as usize;
    if length < 4 || length > message.len() {
        return false;
    }
    match message[3] as i32 {
        libc::RTM_ADD | libc::RTM_DELETE | libc::RTM_CHANGE | libc::RTM_REDIRECT => {
            if length < std::mem::size_of::<libc::rt_msghdr>() {
                return false;
            }
            let flags_at = std::mem::offset_of!(libc::rt_msghdr, rtm_flags);
            let error_at = std::mem::offset_of!(libc::rt_msghdr, rtm_errno);
            let flags = i32::from_ne_bytes(message[flags_at..flags_at + 4].try_into().unwrap());
            let error = i32::from_ne_bytes(message[error_at..error_at + 4].try_into().unwrap());
            // Neighbor-cache churn is not a new network. In particular, DNS
            // traffic must not invalidate its own in-flight query generation.
            error == 0 && flags & (libc::RTF_LLINFO | libc::RTF_WASCLONED) == 0
        }
        libc::RTM_NEWADDR | libc::RTM_DELADDR | libc::RTM_IFINFO | libc::RTM_IFINFO2 => true,
        _ => false,
    }
}

#[cfg(test)]
#[path = "macos/tests.rs"]
mod tests;
