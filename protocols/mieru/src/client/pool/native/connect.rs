//! One bounded pending pool record, with every candidate's original disposition.
use super::*;
use crate::client::pool::managed::CarrierLease;
use zero_platform_tokio::{
    contract::component::ControlledStreamDemandClass,
    controlled_carrier::{CarrierCandidates, CarrierGrants},
};
pub(super) struct Request<'a> {
    pub candidates: CarrierCandidates,
    pub username: &'a str,
    pub password: &'a str,
    pub options: &'a mieru_config::MieruTransportOptions,
    pub deadline: Option<Instant>,
    pub udp: bool,
}
impl Request<'_> {
    pub async fn open(
        self,
        mut lease: CarrierLease,
        grants: &CarrierGrants,
    ) -> io::Result<std::sync::Arc<ClientConnection>> {
        let state = lease.owner.clone();
        let attempt_cancel = lease.cancellation();
        let end = self
            .deadline
            .unwrap_or_else(|| Instant::now() + std::time::Duration::from_secs(15));
        let mut failure = None;
        for index in 0..self.candidates.len() {
            if Instant::now() >= end {
                return Err(io::ErrorKind::TimedOut.into());
            }
            let mut grant = grants.reserve().map_err(io::Error::other)?;
            if grants.registration().bound().is_some_and(|bound| {
                bound.demand() == ControlledStreamDemandClass::BackgroundControl
            }) {
                lease.view = Some(grant.claim_driver_view().map_err(io::Error::other)?);
            }
            let adoption = grant.adoption();
            lease.adopt(adoption.clone()).map_err(io::Error::other)?;
            lease.attempting_io();
            if self.udp {
                match self.candidates.prepare_datagram(index, grant) {
                    Ok(io) => {
                        return ClientConnection::packet_controlled(
                            io,
                            adoption,
                            self.username,
                            self.password,
                            self.options,
                            lease,
                        )
                        .await
                    }
                    Err(source) => failure = Some(io::Error::other(source)),
                }
            } else {
                let opening = async {
                    let prepared = self.candidates.prepare_stream(index, grant)?;
                    prepared.open(end).await
                };
                let ready = tokio::select! { biased;
                    _ = attempt_cancel.cancelled() => return Err(io::ErrorKind::Interrupted.into()),
                    _ = state.scope_cancel.cancelled() => return Err(io::ErrorKind::Interrupted.into()),
                    result = opening => result,
                };
                match ready {
                    Ok(ready) => {
                        let (io, _) = ready.into_parts();
                        // Wire authentication failure is not a pre-payload root
                        // candidate failure and never retries a submitted request.
                        return ClientConnection::tcp_controlled(
                            io,
                            adoption,
                            self.username,
                            self.password,
                            self.options,
                            lease,
                        )
                        .await;
                    }
                    Err(source) => failure = Some(io::Error::other(source)),
                }
            }
            drop(lease.view.take());
            // Actual original close, not error classification, permits another
            // candidate to occupy this one common pool reservation.
            let report = adoption.retire(end).await.map_err(io::Error::other)?;
            let complete = report.is_complete();
            if !complete
                || adoption
                    .closed()
                    .is_none_or(|receipt| !receipt.matches(&adoption))
            {
                return Err(failure.unwrap_or_else(|| io::ErrorKind::WouldBlock.into()));
            }
        }
        Err(failure.unwrap_or_else(|| io::ErrorKind::NotFound.into()))
    }
}

pub(super) struct BorrowRequest<'a> {
    pub udp: bool,
    pub upstream: zero_platform_tokio::contract::network::UpstreamTransport,
    pub endpoint: zero_core::request::Destination,
    pub username: &'a str,
    pub password: &'a str,
    pub options: &'a mieru_config::MieruTransportOptions,
    pub deadline: Option<Instant>,
}
impl BorrowRequest<'_> {
    pub async fn open(
        self,
        mut lease: CarrierLease,
        grants: &CarrierGrants,
    ) -> io::Result<std::sync::Arc<ClientConnection>> {
        let Self {
            udp,
            upstream,
            endpoint,
            username,
            password,
            options,
            deadline,
        } = self;
        let mut grant = grants.reserve().map_err(io::Error::other)?;
        lease.view = Some(grant.claim_driver_view().map_err(io::Error::other)?);
        let adoption = grant.adoption();
        lease.adopt(adoption.clone()).map_err(io::Error::other)?;
        lease.attempting_io();
        let state = lease.owner.clone();
        let attempt_cancel = lease.cancellation();
        let context = zero_platform_tokio::contract::network::CarrierContext {
            session: None,
            deadline,
            cancel: attempt_cancel.clone(),
        };
        if udp {
            let loan = tokio::select! {biased;
                _=attempt_cancel.cancelled()=>return Err(io::ErrorKind::Interrupted.into()),
                _=state.scope_cancel.cancelled()=>return Err(io::ErrorKind::Interrupted.into()),
                result=upstream.borrow_datagram(endpoint.clone(),context.clone())=>result.map_err(io::Error::other)?,
            };
            let io = grant
                .adopt_datagram_loan(upstream.binding(), endpoint, &context, loan)
                .map_err(io::Error::other)?;
            return ClientConnection::packet_borrowed(
                io, adoption, username, password, options, lease,
            )
            .await;
        }
        let loan = tokio::select! {biased;
            _=attempt_cancel.cancelled()=>return Err(io::ErrorKind::Interrupted.into()),
            _=state.scope_cancel.cancelled()=>return Err(io::ErrorKind::Interrupted.into()),
            result=upstream.borrow_stream(endpoint.clone(),context.clone())=>result.map_err(io::Error::other)?,
        };
        // Persist the donor's real controller in the original borrower slot before
        // Mieru wire preparation or the next await. No donor witness is attached as
        // this new driver's witness; spawn_driver still registers its actual task.
        let io = grant
            .adopt_stream_loan(upstream.binding(), endpoint, &context, loan)
            .map_err(io::Error::other)?;
        ClientConnection::tcp_controlled(io, adoption, username, password, options, lease).await
    }
}
