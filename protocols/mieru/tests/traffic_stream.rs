#![cfg(feature = "runtime")]

use mieru::{
    config::{MieruTcpFragmentConfig, MieruTrafficPatternConfig, MieruTransportOptions},
    inbound::{MieruInboundProfile, MieruInboundStream},
    traffic_pattern::TrafficPattern,
    MieruOutbound, MieruTcpStream,
};
use std::{
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, DuplexStream, ReadBuf};
use zero_platform_tokio::TcpRelayStream;

struct Counted {
    inner: DuplexStream,
    writes: Arc<AtomicUsize>,
    reads: Arc<AtomicUsize>,
}
impl AsyncRead for Counted {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}
impl AsyncWrite for Counted {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        self.writes.fetch_add(1, Ordering::Relaxed);
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

#[tokio::test]
async fn configured_public_streams_fragment_large_payloads_in_both_directions() {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let (left, right) = tokio::io::duplex(1 << 20);
        let client_writes = Arc::new(AtomicUsize::new(0));
        let server_writes = Arc::new(AtomicUsize::new(0));
        let client_reads = Arc::new(AtomicUsize::new(0));
        let server_reads = Arc::new(AtomicUsize::new(0));
        let mut client = TcpRelayStream::new(Counted {
            inner: left,
            writes: client_writes.clone(),
            reads: client_reads.clone(),
        });
        let mut server = TcpRelayStream::new(Counted {
            inner: right,
            writes: server_writes.clone(),
            reads: server_reads.clone(),
        });
        let options = MieruTransportOptions {
            traffic_pattern: Some(MieruTrafficPatternConfig {
                seed: Some(3330),
                tcp_fragment: Some(MieruTcpFragmentConfig {
                    enable: Some(true),
                    max_sleep_ms: Some(0),
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        let pattern = TrafficPattern::from_config(options.traffic_pattern.as_ref()).unwrap();
        let profile =
            MieruInboundProfile::from_config(vec![("u".into(), "p".into())]).with_options(options);
        let (outbound, inbound) = tokio::join!(
            MieruOutbound::connect_with_pattern(&mut client, "u", "p", &pattern),
            profile.accept_request(&mut server),
        );
        let mut client = MieruTcpStream::new(client, outbound.unwrap());
        let mut server = MieruInboundStream::new(server, inbound.unwrap());
        client_writes.store(0, Ordering::Relaxed);
        server_writes.store(0, Ordering::Relaxed);
        client_reads.store(0, Ordering::Relaxed);
        server_reads.store(0, Ordering::Relaxed);
        client.write_all(b"probe").await.unwrap();
        server.write_all(b"probe").await.unwrap();
        std::future::poll_fn(|cx| Pin::new(&mut client).poll_read(cx, &mut ReadBuf::new(&mut [])))
            .await
            .unwrap();
        std::future::poll_fn(|cx| Pin::new(&mut server).poll_read(cx, &mut ReadBuf::new(&mut [])))
            .await
            .unwrap();
        for calls in [&client_reads, &server_reads, &client_writes, &server_writes] {
            assert_eq!(calls.load(Ordering::Relaxed), 0);
        }
        let (left, right) = tokio::join!(client.flush(), server.flush());
        left.unwrap();
        right.unwrap();
        let mut probe = [0; 5];
        client.read_exact(&mut probe).await.unwrap();
        assert_eq!(&probe, b"probe");
        server.read_exact(&mut probe).await.unwrap();
        assert_eq!(&probe, b"probe");
        client_writes.store(0, Ordering::Relaxed);
        server_writes.store(0, Ordering::Relaxed);
        let payload: Vec<u8> = (0..100_000).map(|n| (n % 251) as u8).collect();
        let mut received = vec![0; payload.len()];
        let (sent, read) = tokio::join!(
            async {
                client.write_all(&payload).await?;
                client.flush().await
            },
            server.read_exact(&mut received),
        );
        sent.unwrap();
        read.unwrap();
        assert_eq!(received, payload);
        let (sent, read) = tokio::join!(
            async {
                server.write_all(&payload).await?;
                server.shutdown().await
            },
            client.read_exact(&mut received),
        );
        sent.unwrap();
        read.unwrap();
        assert_eq!(received, payload);
        // Four protocol DATA frames fit in the carrier without short writes.
        // More than four writes proves the configured encrypted-write fragmentation ran.
        assert!(client_writes.load(Ordering::Relaxed) > 4);
        assert!(server_writes.load(Ordering::Relaxed) > 4);
    })
    .await
    .unwrap();
}
