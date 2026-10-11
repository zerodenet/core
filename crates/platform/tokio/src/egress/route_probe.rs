//! Reuse native probe storage, never a route/source decision. Each connect
//! consults the OS after the previous source, peer and route have been cleared.
use std::{
    io,
    net::{IpAddr, SocketAddr, UdpSocket},
};

fn wildcard(peer: SocketAddr) -> SocketAddr {
    SocketAddr::new(
        if peer.is_ipv6() {
            IpAddr::V6(std::net::Ipv6Addr::UNSPECIFIED)
        } else {
            IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED)
        },
        0,
    )
}

fn fresh_source(peer: SocketAddr) -> io::Result<IpAddr> {
    let socket = UdpSocket::bind(wildcard(peer))?;
    socket.connect(peer)?;
    socket.local_addr().map(|address| address.ip())
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[derive(Default)]
struct ProbeSockets {
    ipv4: Option<UdpSocket>,
    ipv6: Option<UdpSocket>,
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
impl ProbeSockets {
    fn source_for(&mut self, peer: SocketAddr) -> io::Result<IpAddr> {
        self.source_with_reset(peer, reset)
    }

    fn source_with_reset(
        &mut self,
        peer: SocketAddr,
        reset: impl FnOnce(&UdpSocket, bool) -> io::Result<()>,
    ) -> io::Result<IpAddr> {
        let slot = if peer.is_ipv6() {
            &mut self.ipv6
        } else {
            &mut self.ipv4
        };
        let socket = match slot.take() {
            Some(socket) => socket,
            None => {
                let socket = UdpSocket::bind(wildcard(peer))?;
                // The probe sends no payload and needs no receive backlog.
                // If an OS cannot apply this bound, use the original one-shot
                // path rather than retain a socket with unbounded defaults.
                if socket2::SockRef::from(&socket)
                    .set_recv_buffer_size(1024)
                    .is_err()
                {
                    drop(socket);
                    return fresh_source(peer);
                }
                socket
            }
        };
        // Errors drop this descriptor: a failed connect must not retain a
        // previous source, pending socket error or native route cache.
        socket.connect(peer)?;
        let source = socket.local_addr()?.ip();
        // A reset failure affects reuse only. The just-completed query remains
        // authoritative; discard the probe and open a fresh one on the next call.
        if reset(&socket, peer.is_ipv6()).is_ok() {
            *slot = Some(socket);
        }
        Ok(source)
    }
}

pub(super) fn route_source_for(peer: SocketAddr) -> io::Result<IpAddr> {
    // Dual-stack mapped connects can switch native UDP protocol state. Direct
    // canonicalizes them before this boundary; preserve the one-shot behavior
    // for other callers rather than reuse a mixed-family probe descriptor.
    if matches!(peer.ip(), IpAddr::V6(address) if address.to_ipv4_mapped().is_some()) {
        return fresh_source(peer);
    }
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    {
        // At most two descriptors per calling OS thread. No shared hot lock,
        // per-destination labels, timers or background workers. Thread exit
        // closes them; no protocol/config/policy values are retained here.
        thread_local! { static PROBES: std::cell::RefCell<ProbeSockets> = const {
            std::cell::RefCell::new(ProbeSockets { ipv4: None, ipv6: None })
        }; }
        PROBES.with(|probes| match probes.try_borrow_mut() {
            Ok(mut probes) => probes.source_for(peer),
            Err(_) => fresh_source(peer),
        })
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    fresh_source(peer)
}

#[cfg(target_os = "linux")]
fn reset(socket: &UdpSocket, _ipv6: bool) -> io::Result<()> {
    use std::os::fd::AsRawFd;
    let address = libc::sockaddr {
        sa_family: libc::AF_UNSPEC as libc::sa_family_t,
        sa_data: [0; 14],
    };
    // SAFETY: the live UDP descriptor and fully initialized sockaddr remain
    // valid for this synchronous call. Linux AF_UNSPEC disconnect clears the
    // automatically selected local address and sk_dst route cache.
    let result = unsafe {
        libc::connect(
            socket.as_raw_fd(),
            &address,
            std::mem::size_of_val(&address) as libc::socklen_t,
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(target_os = "macos")]
fn reset(socket: &UdpSocket, ipv6: bool) -> io::Result<()> {
    use std::os::fd::AsRawFd;
    // SAFETY: a live private UDP descriptor; ANY identifies its sole association.
    // disconnectx has a checked success result on both Darwin UDP families,
    // unlike connect(AF_UNSPEC), which can disconnect yet return an error.
    let result = unsafe {
        libc::disconnectx(
            socket.as_raw_fd(),
            libc::SAE_ASSOCID_ANY,
            libc::SAE_CONNID_ANY,
        )
    };
    if result != 0 {
        return Err(io::Error::last_os_error());
    }
    // Darwin disconnect clears source/peer but can retain inp_route. Setting
    // the private probe's interface scope to NONE explicitly releases that
    // cache, including a newly added more-specific route before notification.
    super::bind_fd_to_index(socket.as_raw_fd(), ipv6, 0)
}

#[cfg(test)]
#[path = "../../tests/egress/route_probe.rs"]
mod tests;
