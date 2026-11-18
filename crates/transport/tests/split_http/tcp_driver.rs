//! Exercise the actual Hyper dispatcher/delegates over retained native TCP.
//! This low-level test makes no host/provider close-receipt claim.
use super::*;
use http_body_util::BodyExt;
use std::task::{Context, Poll};
use std::{convert::Infallible, num::NonZeroUsize, pin::Pin};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use zero_platform_tokio::controlled_carrier::CarrierOriginalTotals;

struct ObservedTcp {
    socket: TcpStream,
    pending: tokio::sync::watch::Sender<bool>,
}
impl AsyncRead for ObservedTcp {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let result = Pin::new(&mut self.socket).poll_read(cx, buffer);
        self.pending.send_replace(result.is_pending());
        result
    }
}
impl AsyncWrite for ObservedTcp {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.socket).poll_write(cx, bytes)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.socket).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.socket).poll_shutdown(cx)
    }
}

#[tokio::test]
async fn completed_h2_dispatcher_releases_wire_delegate_before_original_join() {
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let socket = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (peer, _) = listener.accept().await.unwrap();
        let (pending, mut observed) = tokio::sync::watch::channel(false);
        let memory = Arc::new(zero_tun::packet_memory::PacketReturnTotals::default());
        let originals = CarrierOriginalTotals::new(NonZeroUsize::new(1).unwrap());
        let prepared = PreparedXhttpTcp::new(
            originals.buffer_budget(),
            memory.driver_metadata_view(),
            TcpVersion::Http2 { keepalive: 0 },
        )
        .unwrap();
        let (credit, _lease) = prepared.state.tickets.lock().unwrap().take().unwrap();
        let (ready, receive) = oneshot::channel();
        let driver = run(
            TcpRelayStream::new(ObservedTcp { socket, pending }),
            prepared.state.clone(),
            prepared.version,
            ready,
            credit,
        );
        let exchange = async {
            let mut sender = receive.await.unwrap().unwrap();
            let request = http::Request::builder()
                .uri("http://localhost/return")
                .header("host", "localhost")
                .body(crate::split_http::body::Body::empty())
                .unwrap();
            let response = sender.send_request(request).await.unwrap();
            let crate::split_http::body::ReceivedBody::Http(body) = response.into_body() else {
                panic!("the actual H2 response must retain its Hyper body");
            };
            body.collect().await.unwrap();
            observed.wait_for(|pending| *pending).await.unwrap();
            assert!(prepared.state.delegates.load(Ordering::Acquire) > 0);
            drop(sender);
            prepared.stop();
        };
        let server = async {
            hyper::server::conn::http2::Builder::new(hyper_util::rt::TokioExecutor::new())
                .serve_connection(
                    TokioIo::new(peer),
                    hyper::service::service_fn(|_| async {
                        Ok::<_, Infallible>(http::Response::new(http_body_util::Empty::<
                            bytes::Bytes,
                        >::new()))
                    }),
                )
                .await
                .unwrap();
        };
        let (result, (), ()) = tokio::join!(driver, exchange, server);
        result.unwrap();
        assert_eq!(prepared.state.delegates.load(Ordering::Acquire), 0);
        assert!(prepared.state.runtime.ledger.is_empty());
    })
    .await
    .unwrap();
}
