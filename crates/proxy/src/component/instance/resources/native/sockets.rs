//! Listener acquisition has no await between native bind and transfer to its owner.
use super::*;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use zero_core::Address;

pub(super) struct TcpGrant {
    pub(super) native: zero_platform_tokio::ControlledTcpListenerOwner,
    pub(super) acquired: bool,
    pub(super) attempted: bool,
}

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
    ) -> Result<zero_platform_tokio::ControlledTcpListener, ChannelError> {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.listener_phase(&state)?;
        let address = literal(self.component, &destination)?;
        let owner = state.tcp.get(&address).ok_or_else(|| self.denied())?;
        // This short synchronous lock cannot cross await; the installed owner
        // remains in State before the first native create and every failure.
        let mut native = owner.try_lock().map_err(|_| self.denied())?;
        if native.acquired {
            return Err(error(
                self.component,
                Reason::Capacity,
                "declared TCP listener was already acquired",
            ));
        }
        native.attempted = true;
        let view = native.native.bind_listener(address).map_err(|mut error| {
            error.component.get_or_insert(self.component);
            error
        })?;
        native.acquired = true;
        Ok(view)
    }
    pub(super) fn bind_datagram(
        &self,
        destination: Destination,
    ) -> Result<GrantedDatagramListener, ChannelError> {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.listener_phase(&state)?;
        let address = literal(self.component, &destination)?;
        let owner = state.datagram.get(&address).ok_or_else(|| self.denied())?;
        let result = owner
            .try_lock()
            .map_err(|_| self.denied())?
            .bind(self.component, address);
        result
    }
}
mod udp;
pub(super) use udp::DatagramGrant;
