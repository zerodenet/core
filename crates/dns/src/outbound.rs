//! Host detour contract. Control traffic has no fabricated business session.
use std::{fmt, future::Future, io, net::SocketAddr, pin::Pin};
use tokio::time::Instant;
use zero_platform_tokio::contract::{context::with_deadline, CancellationToken};

pub type DnsOutboundConnectFuture =
    Pin<Box<dyn Future<Output = io::Result<zero_platform_tokio::TcpRelayStream>> + Send + 'static>>;
pub type DnsOutboundDatagramFuture =
    Pin<Box<dyn Future<Output = io::Result<Vec<u8>>> + Send + 'static>>;

#[derive(Clone, Default)]
pub struct DnsConnectContext {
    /// Absolute backend attempt bound, in addition to the caller's query bound.
    pub deadline: Option<Instant>,
    /// Owned by the shared query flight, never by one coalesced caller.
    pub cancel: CancellationToken,
}

pub trait DnsOutboundConnector: fmt::Debug + Send + Sync {
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
