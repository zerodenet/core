use super::*;
use crate::{
    contract::component::ResourceDeclaration,
    controlled_datagram::{ControlledDatagramOwner, ControlledDatagramSocket},
    ControlledTcpListener, ControlledTcpListenerOwner,
};
use std::{
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
    num::NonZeroUsize,
};
use zero_core::{request::Destination, Address};
pub(super) struct Reservations {
    tcp: ControlledTcpListenerOwner,
    tcp_view: Option<ControlledTcpListener>,
    udp: ControlledDatagramOwner,
    udp_view: Option<ControlledDatagramSocket>,
    address: SocketAddr,
    datagram: bool,
    udp_originals: u64,
}
impl Reservations {
    pub(super) fn new(
        spec: &ProcessEndpointSpec,
        cancel: CancellationToken,
    ) -> Result<Self, ChannelError> {
        let ip = if spec.ipv6 {
            IpAddr::V6(Ipv6Addr::LOCALHOST)
        } else {
            IpAddr::V4(Ipv4Addr::LOCALHOST)
        };
        let address = SocketAddr::new(ip, 0);
        let destination = Destination {
            address: match ip {
                IpAddr::V4(ip) => Address::Ipv4(ip.octets()),
                IpAddr::V6(ip) => Address::Ipv6(ip.octets()),
            },
            port: 0,
        };
        let one = NonZeroUsize::new(1).unwrap();
        let tcp = ControlledTcpListenerOwner::new(
            &ResourceDeclaration::TcpListener {
                destination,
                accepted: None,
                max_bind_attempts: one,
            },
            one,
            cancel,
        )?;
        Ok(Self {
            tcp,
            tcp_view: None,
            udp: ControlledDatagramOwner::new(one),
            udp_view: None,
            address,
            datagram: spec.datagram,
            udp_originals: 0,
        })
    }
    pub(super) fn acquire(&mut self) -> Result<SocketAddr, ChannelError> {
        // The owner and original entries exist before bind/flags/registration.
        let view = self.tcp.bind_listener(self.address)?;
        self.tcp_view = Some(view);
        let address = self
            .tcp_view
            .as_ref()
            .unwrap()
            .local_addr()
            .map_err(channel)?;
        if !address.ip().is_loopback() || address.port() == 0 {
            return Err(channel(denied("invalid actual reservation")));
        }
        if self.datagram {
            self.udp_view = Some(
                self.udp
                    .bind_process_reservation(address)
                    .map_err(channel)?,
            );
            if self
                .udp_view
                .as_ref()
                .unwrap()
                .local_addr()
                .map_err(channel)?
                != address
            {
                return Err(channel(denied("actual UDP and TCP reservations differ")));
            }
        }
        Ok(address)
    }
}
impl ProcessEndpointHandoff {
    /// Drops only private views, then explicitly closes their exact originals.
    /// Cancellation leaves the same owners/receipts in this handoff for retry.
    pub async fn close_reservations(
        self: &Arc<Self>,
        deadline: Instant,
    ) -> Result<Arc<ProcessReservationsClosed>, ChannelError> {
        if let Some(closed) = self.lock().reservation_close.clone() {
            return Ok(closed);
        }
        let mut native = tokio::time::timeout_at(deadline, self.reservations.lock())
            .await
            .map_err(|_| {
                channel(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "reservation close gate pending",
                ))
            })?;
        // No reservation view has ever escaped this one concrete owner.
        drop(native.tcp_view.take());
        drop(native.udp_view.take());
        let tcp_result = native.tcp.wait_closed(deadline).await;
        // Always attempt both originals, retaining all typed close sources.
        let udp_result = native.udp.close_all();
        native.udp_originals = native
            .udp_originals
            .checked_add(udp_result.confirmed_closed)
            .expect("one actual UDP reservation per process endpoint");
        let mut failures = Vec::new();
        match tcp_result {
            Ok(report) if report.is_complete() => {}
            Ok(report) => failures.push(channel(IncompleteClose(report))),
            Err(error) => failures.push(error),
        }
        if !udp_result.is_complete() {
            for retained in udp_result.unconfirmed {
                failures.push(channel(SharedIo(retained.source)));
            }
            if failures.is_empty() {
                failures.push(channel(denied("original UDP reservation is still live")));
            }
        }
        if !failures.is_empty() {
            return Err(channel(CleanupFailures(failures)));
        }
        let closed = Arc::new(ProcessReservationsClosed {
            original: Arc::downgrade(self),
            tcp_originals: native.tcp.confirmed_close_count(),
            udp_originals: native.udp_originals,
        });
        self.lock().reservation_close = Some(closed.clone());
        Ok(closed)
    }
}
#[derive(Debug)]
pub(super) struct SharedIo(pub Arc<io::Error>);
impl std::fmt::Display for SharedIo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for SharedIo {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}

#[derive(Debug)]
struct IncompleteClose(crate::contract::lifecycle::CloseReport);
impl std::fmt::Display for IncompleteClose {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "actual TCP reservation retains {} original obligations",
            self.0.unfinished.len()
        )
    }
}
impl std::error::Error for IncompleteClose {}
