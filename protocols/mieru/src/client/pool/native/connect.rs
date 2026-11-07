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
    pub waiting: Option<zero_platform_tokio::contract::network::PendingCarrierObserver>,
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
        let candidates = std::sync::Arc::new(self.candidates);
        for index in 0..candidates.len() {
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
            // An opted-in caller retains the actual receiver before native
            // preparation or wire authentication can suspend. Bare callers
            // keep their old pool debt, without immediate Drop cleanup.
            if let Some(waiting) = &self.waiting {
                waiting.retain(adoption.clone()).map_err(io::Error::other)?;
            }
            if state.dynamic {
                match (super::setup::candidate::Candidate {
                    candidates: candidates.clone(),
                    index,
                    udp: self.udp,
                    username: self.username.to_owned(),
                    password: self.password.to_owned(),
                    options: self.options.clone(),
                    end,
                })
                .open(&lease, &adoption, grant)
                .await?
                {
                    super::setup::candidate::Opened::Ready(prepared) => {
                        return prepared.start(adoption, lease).await
                    }
                    super::setup::candidate::Opened::CandidateFailure(source) => {
                        failure = Some(source)
                    }
                }
            } else if self.udp {
                match candidates.prepare_datagram(index, grant) {
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
                    let prepared = candidates.prepare_stream(index, grant)?;
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
    pub provider: zero_platform_tokio::contract::network::UpstreamTransport,
    pub waiting: Option<zero_platform_tokio::contract::network::PendingCarrierObserver>,
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
            provider,
            waiting,
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
        // Business callers have no absolute connect deadline. The physical
        // background prefix still needs the same finite preparation bound as
        // this pool's existing native initialization, independently of the
        // business stream and its per-operation timers.
        let deadline =
            Some(deadline.unwrap_or_else(|| Instant::now() + std::time::Duration::from_secs(15)));
        let context = zero_platform_tokio::contract::network::CarrierContext {
            session: None,
            deadline,
            cancel: attempt_cancel.clone(),
        };
        // Keep the exact record wrapper alive across selection and borrowing.
        // Its selected ROOT holds only a weak callback; no caller is retained
        // by the persistent provider or the successful pool connection.
        if state.dynamic {
            if let Some(waiting) = &waiting {
                waiting.retain(adoption.clone()).map_err(io::Error::other)?;
            }
            let originals = lease.pending_carriers();
            let owner = grants.registration();
            let selected_binding = upstream.binding().clone();
            let username = username.to_owned();
            let password = password.to_owned();
            let options = options.clone();
            let prepare_adoption = adoption.clone();
            let stop = grant.cancellation();
            let prepared =
                super::setup::run(&lease, &adoption, stop.clone(), move |witness| async move {
                    let context = zero_platform_tokio::contract::network::CarrierContext {
                        cancel: stop,
                        ..context
                    };
                    let caller = super::setup::Caller {
                        witness,
                        owner,
                        binding: selected_binding.clone(),
                        call: context.clone(),
                    };
                    let pending = super::pending::for_setup(
                        originals,
                        selected_binding.clone(),
                        waiting,
                        caller,
                        provider.clone(),
                    );
                    let upstream = provider
                        .select_background_carrier_observed(
                            Some(selected_binding),
                            context.clone(),
                            pending.pending_carrier_observer(),
                        )
                        .await
                        .map_err(io::Error::other)?;
                    if udp {
                        let loan = upstream
                            .borrow_datagram(endpoint.clone(), context.clone())
                            .await
                            .map_err(io::Error::other)?;
                        let io = grant
                            .adopt_datagram_loan(upstream.binding(), endpoint, &context, loan)
                            .map_err(io::Error::other)?;
                        let ipv6 = io.ipv6_overhead();
                        ClientConnection::prepare_packet_driver(
                            io.into_driver(),
                            ipv6,
                            &username,
                            &password,
                            &options,
                        )
                        .map(super::setup::Prepared::Packet)
                    } else {
                        let loan = upstream
                            .borrow_stream(endpoint.clone(), context.clone())
                            .await
                            .map_err(io::Error::other)?;
                        let io = grant
                            .adopt_stream_loan(upstream.binding(), endpoint, &context, loan)
                            .map_err(io::Error::other)?;
                        ClientConnection::prepare_tcp_controlled(io, &username, &password, &options)
                            .await
                            .map(super::setup::Prepared::Tcp)
                    }
                })
                .await?;
            lease.release_installed_pending();
            return prepared.start(prepare_adoption, lease).await;
        }
        let pending = if let Some(waiting) = waiting {
            // This is the record's own receiver, separate from its bounded
            // foreign donor set. Cancellation retires receiver and donor chain.
            waiting.retain(adoption.clone()).map_err(io::Error::other)?;
            Some(super::pending::transport(
                &lease,
                upstream.binding().clone(),
                waiting,
            ))
        } else {
            None
        };
        let upstream = if let Some(pending) = &pending {
            provider
                .select_background_carrier_observed(
                    Some(upstream.binding().clone()),
                    context.clone(),
                    pending.pending_carrier_observer(),
                )
                .await
                .map_err(io::Error::other)?
        } else {
            upstream
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
            lease.release_installed_pending();
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
        lease.release_installed_pending();
        ClientConnection::tcp_controlled(io, adoption, username, password, options, lease).await
    }
}
