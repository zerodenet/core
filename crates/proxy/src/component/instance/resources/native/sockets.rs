//! Listener acquisition has no await between native bind and transfer to its owner.
use super::*;
use std::{
    io,
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
};
use zero_core::Address;

pub(super) fn literal(
    component: ComponentId,
    destination: &Destination,
) -> Result<SocketAddr, ChannelError> {
    let ip = match destination.address {
        Address::Ipv4(ip) => IpAddr::V4(Ipv4Addr::from(ip)),
        Address::Ipv6(ip) => IpAddr::V6(Ipv6Addr::from(ip)),
        Address::Domain(_) => {
            return Err(error(
                component,
                Reason::Unsupported,
                "listener grants require a literal IP; DNS access is not implied",
            ))
        }
    };
    Ok(SocketAddr::new(ip, destination.port))
}
impl HostResources {
    pub(super) fn listener_phase(&self, state: &State) -> Result<(), ChannelError> {
        if self.cancel.is_cancelled() || state.closed {
            return Err(error(
                self.component,
                Reason::Revoked,
                "native resource owner was revoked",
            ));
        }
        let deadline = state.start_deadline.ok_or_else(|| {
            error(
                self.component,
                Reason::InvalidConfig,
                "native resource acquisition requires the lifecycle start phase",
            )
        })?;
        if Instant::now() >= deadline {
            return Err(error(
                self.component,
                Reason::Timeout,
                "native resource start deadline elapsed",
            ));
        }
        Ok(())
    }
    pub(super) fn bind_tcp(
        &self,
        destination: Destination,
    ) -> Result<tokio::net::TcpListener, ChannelError> {
        let mut state = self.state.lock().expect("native grants poisoned");
        self.listener_phase(&state)?;
        let address = literal(self.component, &destination)?;
        let acquired = state.tcp.get_mut(&address).ok_or_else(|| self.denied())?;
        if *acquired {
            return Err(error(
                self.component,
                Reason::Capacity,
                "declared TCP listener was already acquired",
            ));
        }
        let bind = || -> io::Result<_> {
            let socket = if address.is_ipv4() {
                tokio::net::TcpSocket::new_v4()?
            } else {
                tokio::net::TcpSocket::new_v6()?
            };
            // Match Tokio/Mio: leave SO_REUSEADDR unset on Windows.
            #[cfg(not(windows))]
            socket.set_reuseaddr(true)?;
            socket.bind(address)?;
            let backlog = if cfg!(target_os = "horizon") {
                20
            } else if cfg!(target_os = "haiku") {
                32
            } else {
                128
            };
            socket.listen(backlog)
        };
        let listener = bind().map_err(|source| {
            error(
                self.component,
                Reason::Io,
                "native TCP listener bind failed",
            )
            .with_source(source)
        })?;
        *acquired = true;
        Ok(listener)
    }
    pub(super) fn bind_datagram(
        &self,
        destination: Destination,
    ) -> Result<tokio::net::UdpSocket, ChannelError> {
        let mut state = self.state.lock().expect("native grants poisoned");
        self.listener_phase(&state)?;
        let address = literal(self.component, &destination)?;
        let acquired = state
            .datagram
            .get_mut(&address)
            .ok_or_else(|| self.denied())?;
        if *acquired {
            return Err(error(
                self.component,
                Reason::Capacity,
                "declared UDP listener was already acquired",
            ));
        }
        let bind = || -> io::Result<_> {
            let socket = std::net::UdpSocket::bind(address)?;
            socket.set_nonblocking(true)?;
            tokio::net::UdpSocket::from_std(socket)
        };
        let socket = bind().map_err(|source| {
            error(
                self.component,
                Reason::Io,
                "native UDP listener bind failed",
            )
            .with_source(source)
        })?;
        *acquired = true;
        Ok(socket)
    }
}
