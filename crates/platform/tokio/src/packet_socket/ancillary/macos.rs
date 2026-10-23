//! Darwin IPv4 PKTINFO can clear a specifically bound receive address. Use a
//! synchronous, checked interface scope for that case and restore it before
//! returning to the executor. The listener has one mutable send-side owner;
//! no await or protocol work is allowed inside this scope.
use crate::{egress::bind_fd_to_index, EgressInterface};
use std::{
    io,
    net::SocketAddr,
    os::fd::{AsRawFd, RawFd},
};

pub(super) fn send(
    socket: &tokio::net::UdpSocket,
    bytes: &[u8],
    peer: SocketAddr,
    interface: &EgressInterface,
) -> io::Result<usize> {
    let scope = Scope::enter(socket.as_raw_fd(), interface.index())?;
    // This synchronous syscall never changes the local address or port.
    let result = socket2::SockRef::from(socket).send_to(bytes, &socket2::SockAddr::from(peer));
    // Restoration failure takes precedence over the send result. Never retry
    // unbound, and do not leave a temporary scope behind on a send failure.
    match scope.restore() {
        Ok(()) => result,
        Err(restore) => Err(io::Error::other(crate::PacketScopeRestoreFailure::new(
            result, restore,
        ))),
    }
}

struct Scope {
    fd: RawFd,
    original: u32,
    armed: bool,
}
impl Scope {
    fn enter(fd: RawFd, index: u32) -> io::Result<Self> {
        let mut original = 0_u32;
        let mut length = std::mem::size_of::<u32>() as libc::socklen_t;
        // SAFETY: live borrowed descriptor, correctly sized output buffer.
        if unsafe {
            libc::getsockopt(
                fd,
                libc::IPPROTO_IP,
                libc::IP_BOUND_IF,
                (&mut original as *mut u32).cast(),
                &mut length,
            )
        } != 0
        {
            return Err(io::Error::last_os_error());
        }
        if length as usize != std::mem::size_of::<u32>() {
            return Err(io::Error::other(
                "invalid IPv4 socket interface scope length",
            ));
        }
        let armed = original != index;
        if armed {
            bind_fd_to_index(fd, false, index)?;
        }
        Ok(Self {
            fd,
            original,
            armed,
        })
    }
    fn restore(mut self) -> io::Result<()> {
        if self.armed {
            bind_fd_to_index(self.fd, false, self.original)?;
            self.armed = false;
        }
        Ok(())
    }
}
impl Drop for Scope {
    fn drop(&mut self) {
        // Panic safety; the ordinary return path above checks this operation.
        if self.armed {
            let _ = bind_fd_to_index(self.fd, false, self.original);
        }
    }
}

#[cfg(test)]
#[path = "../../../tests/packet_socket/macos.rs"]
mod tests;
