//! Reuse the original candidate scheduler with the same Direct grants/Scope.
use super::*;
use crate::transport::{CandidateOpenFailure, CandidateOpened, CandidateOpener, DirectCandidate};
use std::{error::Error, fmt, net::SocketAddr};
use zero_platform_tokio::{
    controlled_carrier::CarrierAdoption, EgressInterfaceControl, EgressSelection,
};
pub(super) struct Candidates {
    pub(super) root: Root,
    pub(super) request: ControlledStreamRequest,
    pub(super) resolver: Arc<dyn zero_platform_tokio::contract::dns::DnsService>,
    pub(super) dns_generation: zero_platform_tokio::contract::dns::DnsGeneration,
    pub(super) gate: tokio::sync::Mutex<()>,
}
struct Opening(Option<CarrierAdoption>);
impl Drop for Opening {
    fn drop(&mut self) {
        if let Some(original) = &self.0 {
            original.request_retirement();
        }
    }
}
impl Candidates {
    async fn attempt(
        &self,
        candidate: DirectCandidate,
        selection: &EgressSelection,
        policy: &zero_traits::DialPolicy,
    ) -> Result<CandidateOpened<StreamLoan>, ChannelError> {
        // Pending/failed candidates cannot overlap behind a smaller configured
        // native N. Waiting is bounded by this original finite candidate set.
        let _candidate = self.gate.lock().await;
        let slot = self.root.owner()?;
        if candidate.generation() != self.root.binding.network_generation
            || candidate.generation().0 != selection.generation()
            || policy != &self.root.binding.dial
            || self.request.context.cancel.is_cancelled()
            || self.resolver.generation() != self.dns_generation
        {
            return Err(invalid(
                Reason::StalePlan,
                "Direct candidate changed its original generation/policy",
            ));
        }
        let end = tokio::time::Instant::now()
            .checked_add(self.request.retirement)
            .ok_or_else(|| invalid(Reason::InvalidConfig, "Direct cleanup deadline overflow"))?;
        let end = self
            .request
            .context
            .deadline
            .map_or(end, |deadline| deadline.min(end));
        self.request.grants.reap_returned(end).await?;
        let mut grant = self.request.grants.reserve()?;
        let (view, preparation) = grant.claim_preparing_transferred_view()?;
        let adoption = grant.adoption();
        let mut opening = Opening(Some(adoption.clone()));
        if let Some(waiting) = &self.root.waiting {
            waiting.retain(adoption.clone())?;
        }
        let result = self.prepare(&slot, candidate.peer(), grant).await;
        let carrier = match result {
            Ok(carrier) => carrier,
            Err(source) => {
                drop(view);
                match adoption.retire(end).await {
                    Ok(report) if report.is_complete() => {}
                    Ok(_) => {
                        return Err(invalid(
                            Reason::Capacity,
                            "Direct failed original cleanup remains unfinished",
                        )
                        .with_source(Retained(Arc::new(source))))
                    }
                    Err(cleanup) => {
                        let cleanup = Arc::new(cleanup);
                        // Preserve the cleanup receipt and both original error
                        // objects; diagnostics never substitute for their ACK.
                        return Err(ChannelError::retained(cleanup.clone()).with_source(
                            CleanupFailure {
                                first: Arc::new(source),
                                cleanup,
                            },
                        ));
                    }
                }
                return Err(source);
            }
        };
        let local = carrier
            .endpoints()
            .local()
            .ok_or_else(|| invalid(Reason::Internal, "Direct original has no local endpoint"))?;
        let remote = carrier.endpoints().peer();
        if remote != candidate.peer()
            || !carrier.endpoints().matches(&adoption)
            || !view.matches(&adoption)
        {
            return Err(invalid(
                Reason::StalePlan,
                "Direct original differs from actual candidate",
            ));
        }
        preparation.publish_transferred()?;
        let (io, _) = carrier.into_parts();
        let loan = io.into_stream_loan(view)?;
        opening.0.take();
        Ok(CandidateOpened {
            io: loan,
            remote,
            local,
        })
    }
    fn failure(&self, source: ChannelError) -> CandidateOpenFailure {
        let original = zero_platform_tokio::controlled_carrier::original_connect_error(&source);
        let (operation, interface_bound, local, kind, os_error) = match original {
            Some(original) => (
                original.stage(),
                original.interface_bound(),
                original.local_addr(),
                original.error().kind(),
                original.error().raw_os_error(),
            ),
            None => (
                "prepare_component",
                false,
                None,
                std::io::ErrorKind::Other,
                None,
            ),
        };
        CandidateOpenFailure {
            operation,
            interface_bound,
            local,
            os_error,
            source: std::io::Error::new(kind, Retained(Arc::new(source))),
        }
    }
}
impl CandidateOpener for Candidates {
    type Socket = StreamLoan;
    fn local(&self, io: &StreamLoan, _: Option<SocketAddr>) -> Option<SocketAddr> {
        io.owner
            .original()
            .endpoints()
            .and_then(|endpoints| endpoints.local())
    }
    fn open<'a>(
        &'a self,
        candidate: DirectCandidate,
        _: &'a EgressInterfaceControl,
        selection: &'a EgressSelection,
        policy: &'a zero_traits::DialPolicy,
    ) -> BoxFuture<'a, Result<CandidateOpened<StreamLoan>, CandidateOpenFailure>> {
        Box::pin(async move {
            self.attempt(candidate, selection, policy)
                .await
                .map_err(|source| self.failure(source))
        })
    }
}
#[derive(Debug)]
pub(super) struct Retained(pub(super) Arc<ChannelError>);
impl fmt::Display for Retained {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self.0.as_ref(), f)
    }
}
impl Error for Retained {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.0.as_ref())
    }
}

#[derive(Debug)]
struct CleanupFailure {
    first: Arc<ChannelError>,
    cleanup: Arc<ChannelError>,
}
impl fmt::Display for CleanupFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}; actual original cleanup: {}",
            self.first, self.cleanup
        )
    }
}
impl Error for CleanupFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.cleanup.as_ref())
    }
}
