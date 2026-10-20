use super::*;
use std::{
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use zero_dns::{
    DnsConnectContext, DnsOutboundCarrier, DnsOutboundCarrierFuture, DnsOutboundConnectFuture,
    DnsOutboundConnector, DnsQuicBidiConnector, DnsQuicBidiRequest,
};
use zero_platform_tokio::TcpRelayStream;
pub(super) struct Connector {
    pub(super) handle: OwnedEndpointHandle,
    pub(super) peer: SocketAddr,
    pub(super) opens: AtomicUsize,
    pub(super) pending: Arc<Notify>,
    pub(super) released: Arc<Notify>,
}
impl std::fmt::Debug for Connector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CanonicalQuicConnector")
    }
}
impl DnsOutboundConnector for Connector {
    fn quic_bidi(&self) -> Option<&dyn DnsQuicBidiConnector> {
        Some(self)
    }
    fn connect(&self, _: String, _: SocketAddr) -> DnsOutboundConnectFuture {
        Box::pin(async {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "fixture provides only its declared metadata capability",
            ))
        })
    }
}
impl DnsQuicBidiConnector for Connector {
    fn connect_bidi(
        &self,
        request: DnsQuicBidiRequest,
        context: DnsConnectContext,
    ) -> DnsOutboundCarrierFuture {
        assert_eq!(request.endpoint, self.peer);
        assert_eq!(request.server_name, "localhost");
        assert_eq!(request.alpn, vec![b"doq".to_vec()]);
        assert!(context.deadline.is_some());
        self.opens.fetch_add(1, Ordering::AcqRel);
        let handle = self.handle.clone();
        let pending = self.pending.clone();
        let released = self.released.clone();
        Box::pin(async move {
            let work = async {
                let connection = handle
                    .connect(request.endpoint, &request.server_name)
                    .await
                    .map_err(io::Error::other)?;
                let stream = connection
                    .into_bi_stream()
                    .await
                    .map_err(io::Error::other)?;
                Ok(DnsOutboundCarrier {
                    stream: TcpRelayStream::new(Observed {
                        stream: TcpRelayStream::new(stream),
                        pending,
                        notified: false,
                        _release: Released(released),
                    }),
                    binding: None,
                })
            };
            tokio::select! {biased;
                _=context.cancel.cancelled()=>Err(io::Error::new(io::ErrorKind::Interrupted,"actual query connector cancelled")),
                result=zero_platform_tokio::contract::context::with_deadline(context.deadline,work)=>result.map_err(|source|io::Error::new(io::ErrorKind::TimedOut,source))?,
            }
        })
    }
}
// Declared field order: the original stream (and all three leases) is dropped
// before the release notice. No Drop callback stands in for native closure.
struct Observed {
    stream: TcpRelayStream,
    pending: Arc<Notify>,
    notified: bool,
    _release: Released,
}
struct Released(Arc<Notify>);
impl Drop for Released {
    fn drop(&mut self) {
        self.0.notify_one();
    }
}
impl AsyncRead for Observed {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let result = Pin::new(&mut this.stream).poll_read(cx, bytes);
        if result.is_pending() && !this.notified {
            this.notified = true;
            this.pending.notify_one();
        }
        result
    }
}
impl AsyncWrite for Observed {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().stream).poll_write(cx, bytes)
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_flush(cx)
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_shutdown(cx)
    }
}
