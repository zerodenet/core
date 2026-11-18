//! One acquisition seam under the original candidate order/history scheduler.
use super::*;
use zero_core::component::NetworkGeneration;
use zero_platform_tokio::contract::BoxFuture;

pub(crate) struct CandidateOpened<S> {
    pub(crate) io: S,
    pub(crate) remote: SocketAddr,
    pub(crate) local: SocketAddr,
}
pub(crate) struct CandidateOpenFailure {
    pub(crate) operation: &'static str,
    pub(crate) interface_bound: bool,
    pub(crate) local: Option<SocketAddr>,
    /// The actual original error object, not reconstructed from kind/raw OS code.
    pub(crate) source: io::Error,
    pub(crate) os_error: Option<i32>,
}
pub(crate) struct CandidateRefresh(DirectCandidate);
impl CandidateRefresh {
    pub(in crate::transport) fn from_failure(failure: &TcpDialFailure) -> Self {
        Self(DirectCandidate {
            peer: failure.remote,
            generation: NetworkGeneration(failure.selection.generation()),
        })
    }
    pub(crate) fn candidate(&self) -> &DirectCandidate {
        &self.0
    }
}
pub(crate) type RefreshFuture = BoxFuture<'static, io::Result<Vec<SocketAddr>>>;
pub(crate) type RefreshResult = Result<io::Result<Vec<SocketAddr>>, io::Error>;
struct RefreshPanic(std::sync::Mutex<Box<dyn std::any::Any + Send + 'static>>);
impl std::fmt::Debug for RefreshPanic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}
impl std::fmt::Display for RefreshPanic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let payload = self.0.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(message) = payload.downcast_ref::<String>() {
            return f.write_str(message);
        }
        if let Some(message) = payload.downcast_ref::<&'static str>() {
            return f.write_str(message);
        }
        f.write_str("trusted-domain refresh panicked with its retained original payload")
    }
}
impl std::error::Error for RefreshPanic {}
pub(crate) trait CandidateOpener: Sync {
    type Socket: Send;
    /// Observe one actual lookup in its caller, before any query is polled.
    /// Retries and refreshes get distinct calls; this grants no DNS authority.
    fn resolve<'a>(
        &'a self,
        _: &'a zero_dns::DnsSystem,
        work: BoxFuture<'a, Result<super::super::direct::DirectTargetResolution, zero_core::Error>>,
    ) -> BoxFuture<'a, Result<super::super::direct::DirectTargetResolution, zero_core::Error>> {
        work
    }
    fn local(&self, _: &Self::Socket, observed: Option<SocketAddr>) -> Option<SocketAddr> {
        observed
    }
    fn refresh(&self, task: RefreshFuture, _: CandidateRefresh) -> BoxFuture<'_, RefreshResult> {
        Box::pin(async move {
            use futures_util::FutureExt;
            // The real Source/Direct caller already owns this continuation and
            // its DNS provider. Poll it there; cancellation drops that original
            // wait without spawning/aborting a second private task owner.
            std::panic::AssertUnwindSafe(task)
                .catch_unwind()
                .await
                .map_err(|payload| io::Error::other(RefreshPanic(std::sync::Mutex::new(payload))))
        })
    }
    fn open<'a>(
        &'a self,
        candidate: DirectCandidate,
        egress: &'a EgressInterfaceControl,
        selection: &'a EgressSelection,
        policy: &'a zero_traits::DialPolicy,
    ) -> BoxFuture<'a, Result<CandidateOpened<Self::Socket>, CandidateOpenFailure>>;
}
#[cfg(test)]
use zero_platform_tokio::contract::{
    network::CarrierContext,
    service::{NetworkService, TcpNetworkRequest},
    CancellationToken,
};

#[cfg(test)]
pub(in crate::transport) struct LegacyCandidateOpener;
#[cfg(test)]
impl CandidateOpener for LegacyCandidateOpener {
    type Socket = TokioSocket;
    fn local(&self, io: &Self::Socket, _: Option<SocketAddr>) -> Option<SocketAddr> {
        io.local_addr().ok()
    }
    fn open<'a>(
        &'a self,
        candidate: DirectCandidate,
        egress: &'a EgressInterfaceControl,
        selection: &'a EgressSelection,
        policy: &'a zero_traits::DialPolicy,
    ) -> BoxFuture<'a, Result<CandidateOpened<Self::Socket>, CandidateOpenFailure>> {
        Box::pin(async move {
            let remote = candidate.peer();
            egress
                .open_tcp(
                    TcpNetworkRequest {
                        peer: remote,
                        dial: policy.clone(),
                        generation: NetworkGeneration(selection.generation()),
                    },
                    CarrierContext {
                        session: None,
                        deadline: Some(tokio::time::Instant::now() + CANDIDATE_TIMEOUT),
                        cancel: CancellationToken::new(),
                    },
                )
                .await
                .map(|opened| CandidateOpened {
                    io: opened.io,
                    remote: opened.remote,
                    local: opened.local,
                })
                .map_err(|failure| CandidateOpenFailure {
                    operation: failure.operation,
                    interface_bound: failure.interface_bound,
                    local: failure.local,
                    os_error: failure.source.raw_os_error(),
                    source: failure.source,
                })
        })
    }
}

#[cfg(test)]
#[path = "../tests/candidate_refresh.rs"]
mod tests;
