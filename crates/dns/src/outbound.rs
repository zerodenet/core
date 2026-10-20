//! Host detour contract. Control traffic has no fabricated business session.
use std::{fmt, future::Future, io, net::SocketAddr, pin::Pin, sync::Arc};
use tokio::time::Instant;
use zero_platform_tokio::contract::{context::with_deadline, CancellationToken};

pub type DnsOutboundConnectFuture =
    Pin<Box<dyn Future<Output = io::Result<zero_platform_tokio::TcpRelayStream>> + Send + 'static>>;
pub type DnsOutboundDatagramFuture =
    Pin<Box<dyn Future<Output = io::Result<Vec<u8>>> + Send + 'static>>;

/// A connection carries its actual host-selected complete prefix identity.
/// The host implementation owns the typed roles, endpoints, policies and live
/// selection captures. Checking it must never resolve or advance scheduling.
pub trait DnsCarrierBinding: fmt::Debug + Send + Sync {
    fn check_current(&self) -> io::Result<()>;
    /// Completes with the original revocation cause. The existing pool driver
    /// polls this inline; implementations must not spawn detached observers.
    fn revoked(&self) -> DnsBindingRevokedFuture<'_>;
}
pub type DnsBindingRevokedFuture<'a> = Pin<Box<dyn Future<Output = io::Error> + Send + 'a>>;
pub struct DnsOutboundCarrier {
    pub stream: zero_platform_tokio::TcpRelayStream,
    /// None means the adapter cannot prove a reusable detour binding. The
    /// connection remains usable for its first exchange, but cannot be pooled.
    pub binding: Option<Arc<dyn DnsCarrierBinding>>,
}
pub type DnsOutboundCarrierFuture =
    Pin<Box<dyn Future<Output = io::Result<DnsOutboundCarrier>> + Send + 'static>>;

#[derive(Clone, Default)]
pub struct DnsConnectContext {
    /// Absolute backend attempt bound, in addition to the caller's query bound.
    pub deadline: Option<Instant>,
    /// Owned by the shared query flight, never by one coalesced caller.
    pub cancel: CancellationToken,
}

/// A host-selected QUIC bidirectional stream, without a raw endpoint,
/// connection or protocol runtime crossing the DNS boundary.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DnsQuicBidiRequest {
    pub endpoint: SocketAddr,
    pub server_name: String,
    pub alpn: Vec<Vec<u8>>,
}
pub trait DnsQuicBidiConnector: fmt::Debug + Send + Sync {
    /// The implementation retains actual native/driver ownership before any
    /// I/O. Cancellation releases this query's users, never a native close ACK.
    fn connect_bidi(
        &self,
        request: DnsQuicBidiRequest,
        context: DnsConnectContext,
    ) -> DnsOutboundCarrierFuture;
}

pub trait DnsOutboundConnector: fmt::Debug + Send + Sync {
    /// Absence preserves the staged legacy path. Once a real capability is
    /// selected its errors must not fall back to a naked UDP/QUIC acquisition.
    fn quic_bidi(&self) -> Option<&dyn DnsQuicBidiConnector> {
        None
    }
    fn connect(&self, outbound: String, endpoint: SocketAddr) -> DnsOutboundConnectFuture;
    /// Compatible adapters still receive cancellation and the real backend
    /// deadline around their inline connect future. Native hosts override this
    /// to propagate the same context through every selected Stream role.
    fn connect_with_context(
        &self,
        outbound: String,
        endpoint: SocketAddr,
        context: DnsConnectContext,
    ) -> DnsOutboundConnectFuture {
        let work = self.connect(outbound, endpoint);
        Box::pin(async move {
            if context.deadline.is_some_and(|end| Instant::now() >= end) {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "DNS detour deadline elapsed",
                ));
            }
            tokio::select! { biased;
                _=context.cancel.cancelled()=>Err(io::Error::new(io::ErrorKind::Interrupted,"DNS detour cancelled")),
                result=with_deadline(context.deadline,work)=>result.map_err(|source|io::Error::new(io::ErrorKind::TimedOut,source))?,
            }
        })
    }
    /// Called only for a genuinely new connection, after obsolete pool users
    /// and drivers have exited. Native hosts resolve and capture exactly once.
    fn connect_carrier_with_context(
        &self,
        outbound: String,
        endpoint: SocketAddr,
        context: DnsConnectContext,
    ) -> DnsOutboundCarrierFuture {
        let work = self.connect_with_context(outbound, endpoint, context);
        Box::pin(async move {
            Ok(DnsOutboundCarrier {
                stream: work.await?,
                binding: None,
            })
        })
    }
    fn exchange_datagram(
        &self,
        _outbound: String,
        _endpoint: SocketAddr,
        _query: Vec<u8>,
    ) -> DnsOutboundDatagramFuture {
        Box::pin(async {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "outbound does not provide a DNS datagram exchange",
            ))
        })
    }
}
