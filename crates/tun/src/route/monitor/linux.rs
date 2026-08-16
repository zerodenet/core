use std::fs::File;
use std::io;
use std::mem::MaybeUninit;
use std::os::fd::{AsRawFd, FromRawFd};

use tokio::io::unix::AsyncFd;

#[derive(Debug)]
pub(super) struct RouteChangeMonitor {
    socket: AsyncFd<File>,
}

impl RouteChangeMonitor {
    pub(super) fn new() -> io::Result<Self> {
        let descriptor = unsafe {
            libc::socket(
                libc::AF_NETLINK,
                libc::SOCK_RAW | libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC,
                libc::NETLINK_ROUTE,
            )
        };
        if descriptor < 0 {
            return Err(io::Error::last_os_error());
        }
        let socket = unsafe { File::from_raw_fd(descriptor) };
        let mut address = MaybeUninit::<libc::sockaddr_nl>::zeroed();
        let address = unsafe {
            let address = address.as_mut_ptr();
            (*address).nl_family = libc::AF_NETLINK as libc::sa_family_t;
            (*address).nl_groups =
                (libc::RTMGRP_LINK | libc::RTMGRP_IPV4_ROUTE | libc::RTMGRP_IPV6_ROUTE) as u32;
            address.read()
        };
        let result = unsafe {
            libc::bind(
                socket.as_raw_fd(),
                (&address as *const libc::sockaddr_nl).cast(),
                std::mem::size_of::<libc::sockaddr_nl>() as libc::socklen_t,
            )
        };
        if result != 0 {
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
                        "Linux route notification socket closed",
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
                    "Linux route notification socket closed",
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
