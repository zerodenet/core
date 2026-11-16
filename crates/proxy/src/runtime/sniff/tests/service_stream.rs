//! Real fragmented input is inspected once and every consumed byte is replayed.
use super::*;
use std::{
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
struct Observed {
    io: tokio::net::TcpStream,
    pending: Option<tokio::sync::oneshot::Sender<()>>,
    reads: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}
impl AsyncRead for Observed {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        self.reads
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let result = Pin::new(&mut self.io).poll_read(cx, bytes);
        if result.is_pending() {
            if let Some(pending) = self.pending.take() {
                let _ = pending.send(());
            }
        }
        result
    }
}
impl AsyncWrite for Observed {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.io).poll_write(cx, bytes)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.io).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.io).poll_shutdown(cx)
    }
}
#[tokio::test]
async fn service_tcp_inspection_preserves_fragmented_payload_and_route_only_policy() {
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let connection = tokio::net::TcpStream::connect(listener.local_addr().unwrap());
        let (client, accepted) = tokio::join!(connection, listener.accept());
        let mut client = client.unwrap();
        let (io, _) = accepted.unwrap();
        let first = b"GET /actual HTTP/1.1\r\nHost: exam";
        let rest = b"ple.com\r\n\r\nactual-body";
        client.write_all(first).await.unwrap();
        let (pending, observed) = tokio::sync::oneshot::channel();
        let peer = tokio::spawn(async move {
            observed.await.unwrap();
            client.write_all(rest).await.unwrap();
            client.shutdown().await.unwrap();
        });
        let mut stream: zero_platform_tokio::contract::stream::BoxStream = Box::pin(Observed {
            io,
            pending: Some(pending),
            reads: Default::default(),
        });
        let original = Address::Ipv4([192, 0, 2, 7]);
        let mut session = Session::new(
            1,
            original.clone(),
            80,
            Network::Tcp,
            ProtocolType::new("vless_mux"),
        );
        let policy = SniffingPolicy::new(true, &["http".into()], &[], false, true).unwrap();
        let prefix =
            super::super::sniff_service_stream(&policy, &mut session, &mut stream, false).await;
        assert_eq!(session.target, original);
        assert_eq!(
            session.route_target,
            Some(Address::Domain("example.com".into()))
        );
        let mut replay = crate::transport::ReplayStream::new(stream, prefix);
        let mut bytes = Vec::new();
        replay.read_to_end(&mut bytes).await.unwrap();
        assert_eq!(bytes, [first.as_slice(), rest.as_slice()].concat());
        peer.await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn metadata_only_standard_stream_inspection_never_polls_actual_payload() {
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (client, accepted) = tokio::join!(
            tokio::net::TcpStream::connect(listener.local_addr().unwrap()),
            listener.accept()
        );
        let mut client = client.unwrap();
        let (io, _) = accepted.unwrap();
        let reads = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut stream: zero_platform_tokio::contract::stream::BoxStream = Box::pin(Observed {
            io,
            pending: None,
            reads: reads.clone(),
        });
        let bytes = b"GET / HTTP/1.1\r\nHost: ignored.test\r\n\r\n";
        client.write_all(bytes).await.unwrap();
        client.shutdown().await.unwrap();
        let original = Address::Ipv4([192, 0, 2, 7]);
        let mut session = Session::new(
            1,
            original.clone(),
            80,
            Network::Tcp,
            ProtocolType::new("vless_mux"),
        );
        let policy = SniffingPolicy::new(true, &["http".into()], &[], true, false).unwrap();
        assert!(
            super::super::sniff_service_stream(&policy, &mut session, &mut stream, false)
                .await
                .is_empty()
        );
        assert_eq!(reads.load(std::sync::atomic::Ordering::Relaxed), 0);
        assert_eq!(session.target, original);
        let mut untouched = Vec::new();
        stream.read_to_end(&mut untouched).await.unwrap();
        assert_eq!(untouched, bytes);
    })
    .await
    .unwrap();
}
