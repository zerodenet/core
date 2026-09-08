use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use crate::transport::{failure_origin, relay_bidirectional_metered, TransportFailureOrigin};

#[derive(Default)]
struct Endpoint {
    read: Option<Result<Vec<u8>, io::ErrorKind>>,
    write_error: Option<io::ErrorKind>,
}

impl AsyncRead for Endpoint {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.read.take() {
            Some(Ok(bytes)) => {
                buf.put_slice(&bytes);
                Poll::Ready(Ok(()))
            }
            Some(Err(kind)) => Poll::Ready(Err(kind.into())),
            None => Poll::Pending,
        }
    }
}

impl AsyncWrite for Endpoint {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        Poll::Ready(
            self.write_error
                .map_or(Ok(bytes.len()), |kind| Err(kind.into())),
        )
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[tokio::test]
async fn read_and_write_failures_keep_the_endpoint_that_actually_failed() {
    for client_failure in [true, false] {
        for read_failure in [true, false] {
            let mut failed = Endpoint::default();
            let mut peer = Endpoint::default();
            if read_failure {
                failed.read = Some(Err(io::ErrorKind::ConnectionReset));
            } else {
                failed.write_error = Some(io::ErrorKind::BrokenPipe);
                peer.read = Some(Ok(vec![1]));
            }
            let (client, upstream) = if client_failure {
                (failed, peer)
            } else {
                (peer, failed)
            };
            let error = relay_bidirectional_metered(client, upstream, |_| {}, |_| {})
                .await
                .unwrap_err();
            assert_eq!(
                failure_origin(&error),
                Some(if client_failure {
                    TransportFailureOrigin::Client
                } else {
                    TransportFailureOrigin::Upstream
                })
            );
        }
    }
}

#[tokio::test]
async fn lost_local_address_during_relay_is_not_a_node_failure() {
    let upstream = Endpoint {
        read: Some(Err(io::ErrorKind::AddrNotAvailable)),
        ..Endpoint::default()
    };
    let error = relay_bidirectional_metered(Endpoint::default(), upstream, |_| {}, |_| {})
        .await
        .unwrap_err();
    assert_eq!(
        failure_origin(&error),
        Some(TransportFailureOrigin::LocalNetwork)
    );
}
