//! One acquisition seam under the original candidate order/history scheduler.
use super::*;
use zero_core::component::NetworkGeneration;
use zero_platform_tokio::contract::{
    network::CarrierContext,
    service::{NetworkService, TcpNetworkRequest},
    BoxFuture, CancellationToken,
};

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
pub(crate) type RefreshResult = Result<io::Result<Vec<SocketAddr>>, tokio::task::JoinError>;
pub(crate) trait CandidateOpener: Sync {
    type Socket: Send;
    fn local(&self, _: &Self::Socket, observed: Option<SocketAddr>) -> Option<SocketAddr> {
        observed
    }
    fn refresh(&self, task: RefreshFuture, _: CandidateRefresh) -> BoxFuture<'_, RefreshResult> {
        Box::pin(async move {
            let mut tasks = tokio::task::JoinSet::new();
            tasks.spawn(task);
            tasks
                .join_next()
                .await
                .expect("one trusted-domain refresh task is registered")
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
pub(in crate::transport) struct LegacyCandidateOpener;
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
