//! Unique packet view and a separate original logical-session controller.
use super::*;
use crate::contract::datagram::{BoxDatagramSession, DatagramLimits, DatagramRx, DatagramTx};
use std::{future::poll_fn, io, pin::Pin, sync::atomic::Ordering};
use tokio::sync::Mutex;
use zero_core::{component::AssociationId, packet::Datagram, request::Destination};
mod adopt;
pub use adopt::BorrowedDatagram;
type Tx = Pin<Box<dyn DatagramTx>>;
type Rx = Pin<Box<dyn DatagramRx>>;

pub struct DatagramLoan {
    pub io: LoanDatagramIo,
    pub owner: LoanOwner,
}
pub struct LoanDatagramIo {
    tx: Mutex<Option<Tx>>,
    rx: Mutex<Option<Rx>>,
    state: Arc<State>,
    association: AssociationId,
    endpoint: Destination,
    limits: DatagramLimits,
    observer: Option<Arc<dyn zero_traits::IoObserver>>,
    returned_by_parts: bool,
}
impl DatagramLoan {
    pub fn new_metadata_pool(
        session: BoxDatagramSession,
        actual_acquired_origin: crate::controlled_datagram::metadata_pool::MetadataPoolOrigin,
        association: AssociationId,
        endpoint: Destination,
    ) -> Self {
        Self::new(
            session,
            CarrierOrigin::from_metadata_pool(actual_acquired_origin),
            association,
            endpoint,
        )
    }

    pub fn new_metadata(
        session: BoxDatagramSession,
        original: crate::controlled_datagram::metadata_pool::MetadataPoolOrigin,
        association: AssociationId,
        endpoint: Destination,
    ) -> Self {
        Self::new(
            session,
            CarrierOrigin::from_metadata(original),
            association,
            endpoint,
        )
    }

    pub fn new(
        session: BoxDatagramSession,
        original: CarrierOrigin,
        association: AssociationId,
        endpoint: Destination,
    ) -> Self {
        let state = Arc::new(State {
            returned: AtomicBool::new(false),
            changed: Default::default(),
            cancel: CancellationToken::new(),
        });
        Self {
            io: LoanDatagramIo {
                tx: Mutex::new(Some(session.tx)),
                rx: Mutex::new(Some(session.rx)),
                state: state.clone(),
                association,
                endpoint,
                limits: session.limits,
                observer: None,
                returned_by_parts: false,
            },
            owner: LoanOwner::new(session.control, original, state),
        }
    }
}
impl LoanDatagramIo {
    pub fn set_observer(&mut self, observer: Option<Arc<dyn zero_traits::IoObserver>>) {
        self.observer = observer;
    }
    pub(super) fn targets(&self, endpoint: &Destination) -> bool {
        self.endpoint.port == endpoint.port
            && (self.endpoint.address == endpoint.address
                || literal(&self.endpoint.address)
                    .is_some_and(|ip| Some(ip) == literal(&endpoint.address)))
    }

    pub fn wrap_views(&mut self, wrap: impl FnOnce(Tx, Rx) -> (Tx, Rx)) {
        let (tx, rx) = wrap(
            self.tx.get_mut().take().expect("live packet sender"),
            self.rx.get_mut().take().expect("live packet receiver"),
        );
        *self.tx.get_mut() = Some(tx);
        *self.rx.get_mut() = Some(rx);
    }
    pub fn endpoint(&self) -> &Destination {
        &self.endpoint
    }
    pub async fn send(&self, bytes: &[u8]) -> io::Result<()> {
        let frame = Datagram {
            association: self.association,
            address: self.endpoint.clone(),
            payload: bytes.to_vec().into(),
        };
        self.limits.validate(&frame).map_err(io::Error::other)?;
        let mut tx = self.tx.lock().await;
        let sending = poll_fn(|cx| {
            tx.as_mut()
                .expect("live packet sender")
                .as_mut()
                .poll_send(cx, &frame)
        });
        let receipt = tokio::select! {biased;_=self.state.cancel.cancelled()=>return Err(io::ErrorKind::BrokenPipe.into()),result=sending=>result.map_err(io::Error::other)?};
        if receipt.accepted_bytes != bytes.len()
            || receipt.delivery != crate::contract::DeliveryState::LocallyAccepted
        {
            self.state.cancel.cancel();
            return Err(io::ErrorKind::WriteZero.into());
        }
        if let Some(observer) = &self.observer {
            observer.sent(bytes.len());
        }
        Ok(())
    }
    pub async fn receive(&self, bytes: &mut [u8]) -> io::Result<usize> {
        let mut rx = self.rx.lock().await;
        loop {
            let receiving = poll_fn(|cx| {
                rx.as_mut()
                    .expect("live packet receiver")
                    .as_mut()
                    .poll_recv(cx)
            });
            let frame = tokio::select! {biased;_=self.state.cancel.cancelled()=>return Err(io::ErrorKind::BrokenPipe.into()),result=receiving=>result.map_err(io::Error::other)?};
            let Some(frame) = frame else {
                return Err(io::ErrorKind::UnexpectedEof.into());
            };
            if let Some(observer) = &self.observer {
                observer.received(frame.payload.len());
            }
            if frame.association != self.association {
                continue;
            }
            // The existing wire form may name an IP literal as a domain.
            // Preserve native UDP peer filtering after canonicalization, rather
            // than passing unrelated source traffic into a critical codec.
            if frame.address.port != self.endpoint.port
                || (literal(&self.endpoint.address).is_some() && !self.targets(&frame.address))
            {
                continue;
            }
            if frame.payload.len() > bytes.len() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "borrowed carrier packet exceeds receive buffer",
                ));
            }
            bytes[..frame.payload.len()].copy_from_slice(&frame.payload);
            return Ok(frame.payload.len());
        }
    }
}
fn literal(address: &zero_core::Address) -> Option<std::net::IpAddr> {
    match address {
        zero_core::Address::Ipv4(ip) => Some(std::net::Ipv4Addr::from(*ip).into()),
        zero_core::Address::Ipv6(ip) => Some(std::net::Ipv6Addr::from(*ip).into()),
        zero_core::Address::Domain(host) => zero_core::address::parse_ip_literal(host),
    }
}
impl Drop for LoanDatagramIo {
    fn drop(&mut self) {
        drop(self.tx.get_mut().take());
        drop(self.rx.get_mut().take());
        if !self.returned_by_parts {
            self.state.returned.store(true, Ordering::Release);
            self.state.changed.notify_waiters();
        }
    }
}

mod session;
