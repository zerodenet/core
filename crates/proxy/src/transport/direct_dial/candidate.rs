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
}
pub(crate) trait CandidateOpener: Sync {
    type Socket: Send;
    fn local(&self, _: &Self::Socket, observed: Option<SocketAddr>) -> Option<SocketAddr> {
        observed
    }
    fn open<'a>(
        &'a self,
        remote: SocketAddr,
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
        remote: SocketAddr,
        egress: &'a EgressInterfaceControl,
        selection: &'a EgressSelection,
        policy: &'a zero_traits::DialPolicy,
    ) -> BoxFuture<'a, Result<CandidateOpened<Self::Socket>, CandidateOpenFailure>> {
        Box::pin(async move {
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
                    source: failure.source,
                })
        })
    }
}
