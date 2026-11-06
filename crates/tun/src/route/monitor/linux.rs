use super::descriptor::{Descriptor, View};
use std::io;
use std::mem::MaybeUninit;
use std::os::fd::AsRawFd;
use std::sync::Arc;

use tokio::io::unix::AsyncFd;

#[derive(Debug)]
pub(super) struct RouteChangeMonitor {
    socket: Option<AsyncFd<View>>,
    original: Arc<Descriptor>,
}

impl RouteChangeMonitor {
    pub(super) fn prepare() -> Self {
        Self {
            socket: None,
            original: Arc::default(),
        }
    }
    pub(super) fn acquire(&mut self) -> io::Result<()> {
        self.original.acquire(
            libc::AF_NETLINK,
            libc::SOCK_RAW | libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC,
            libc::NETLINK_ROUTE,
        )?;
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
                self.original.as_raw_fd(),
                (&address as *const libc::sockaddr_nl).cast(),
                std::mem::size_of::<libc::sockaddr_nl>() as libc::socklen_t,
            )
        };
        if result != 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: the persistent original retains the descriptor even when
        // reactor registration fails and drops only its unique IO view.
        self.socket = Some(unsafe { AsyncFd::register(View(self.original.clone())) }?);
        Ok(())
    }
    pub(super) fn close(&mut self) -> io::Result<()> {
        self.socket.take(); // Deregistration/view return is not native close.
        self.original.close()
    }
    pub(super) fn is_closed(&self) -> bool {
        self.original.is_closed()
    }

    pub(super) async fn changed(&mut self) -> io::Result<()> {
        let socket = self.socket.as_ref().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotConnected,
                "route monitor has no registered IO view",
            )
        })?;
        let mut buffer = [0_u8; 8192];
        loop {
            let mut ready = socket.readable().await?;
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
        let socket = self.socket.as_ref().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotConnected,
                "route monitor has no registered IO view",
            )
        })?;
        let mut buffer = [0_u8; 8192];
        loop {
            let received = unsafe {
                libc::recv(
                    socket.get_ref().as_raw_fd(),
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

impl Drop for RouteChangeMonitor {
    fn drop(&mut self) {
        let _ = self.close();
    }
}
