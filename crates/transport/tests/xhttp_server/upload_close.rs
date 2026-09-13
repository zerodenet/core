use super::*;
use crate::{
    profile::OwnedSplitHttpProfile,
    split_http::{accept_xhttp_connection, request::Profile, SplitHttpRegistry},
};
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};
struct ClosedReplyPeer(DuplexStream);
impl AsyncRead for ClosedReplyPeer {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_read(cx, buf)
    }
}
impl AsyncWrite for ClosedReplyPeer {
    fn poll_write(self: Pin<&mut Self>, _: &mut Context<'_>, _: &[u8]) -> Poll<io::Result<usize>> {
        Poll::Ready(Err(io::Error::new(
            io::ErrorKind::BrokenPipe,
            "reply peer closed",
        )))
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}
#[tokio::test]
async fn closed_packet_reply_peer_does_not_discard_pipelined_uploads() {
    let config = OwnedSplitHttpProfile {
        host: None,
        path: "/upload/".into(),
        mode: "packet-up".into(),
        options: zero_traits::SplitHttpOptions {
            sc_max_buffered_posts: 8,
            uplink_data_placement: "header".into(),
            ..Default::default()
        },
    };
    let profile = Profile::new(&config);
    let registry = SplitHttpRegistry::new();
    let session = registry.sessions.get_with_limit("session", 8).unwrap();
    let (mut client, server) = tokio::io::duplex(16 * 1024);
    let source = accept_xhttp_connection(ClosedReplyPeer(server), &config, &registry);
    let writer = tokio::spawn(async move {
        for seq in 0..64u64 {
            let request = profile
                .packet_request("session", seq, &seq.to_be_bytes())
                .unwrap();
            let mut wire = format!("POST {} HTTP/1.1\r\n", request.uri());
            for (name, value) in request.headers() {
                wire.push_str(&format!("{}: {}\r\n", name, value.to_str().unwrap()));
            }
            wire.push_str("Content-Length: 0\r\n\r\n");
            client.write_all(wire.as_bytes()).await.unwrap();
        }
        client.shutdown().await.unwrap();
        let mut unused = Vec::new();
        client.read_to_end(&mut unused).await.unwrap();
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        for seq in 0..64u64 {
            assert_eq!(
                session.next().await.unwrap().unwrap().data.as_ref(),
                &seq.to_be_bytes()
            );
        }
        writer.await.unwrap();
    })
    .await
    .expect("received uploads survive acknowledgement failure");
    drop(source);
}
#[tokio::test]
async fn stream_reply_failure_still_cancels_even_after_packet_drain() {
    let (_, server) = tokio::io::duplex(16);
    let policy = ReplyPolicy::default();
    let mut io = UploadDrain::new(ClosedReplyPeer(server), policy.clone());
    assert_eq!(
        io.write_all(b"download").await.unwrap_err().kind(),
        io::ErrorKind::BrokenPipe
    );
    policy.set_packet(true);
    io.write_all(b"packet acknowledgement").await.unwrap();
    policy.set_packet(false);
    assert_eq!(
        io.write_all(b"download").await.unwrap_err().kind(),
        io::ErrorKind::BrokenPipe
    );
    assert_eq!(
        io.flush().await.unwrap_err().kind(),
        io::ErrorKind::BrokenPipe
    );
}

struct BufferedReplyFailure;
impl AsyncWrite for BufferedReplyFailure {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        Poll::Ready(Ok(bytes.len()))
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Err(io::Error::new(
            io::ErrorKind::BrokenPipe,
            "buffered reply peer closed",
        )))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}
#[tokio::test]
async fn buffered_packet_reply_flush_failure_preserves_upload_only_policy() {
    let policy = ReplyPolicy::default();
    policy.set_packet(true);
    let mut io = UploadDrain::new(BufferedReplyFailure, policy.clone());
    io.write_all(b"acknowledgement").await.unwrap();
    io.flush().await.unwrap();
    policy.set_packet(false);
    assert_eq!(
        io.flush().await.unwrap_err().kind(),
        io::ErrorKind::BrokenPipe
    );
}

#[tokio::test]
async fn parallel_packet_connections_preserve_bounded_reordering_after_reply_failure() {
    let config = OwnedSplitHttpProfile {
        host: None,
        path: "/upload/".into(),
        mode: "packet-up".into(),
        options: zero_traits::SplitHttpOptions {
            sc_max_buffered_posts: 8,
            uplink_data_placement: "header".into(),
            ..Default::default()
        },
    };
    let profile = Profile::new(&config);
    let registry = SplitHttpRegistry::new();
    let session = registry.sessions.get_with_limit("session", 8).unwrap();
    let mut peers = Vec::new();
    let mut sources = Vec::new();
    for _ in 0..4 {
        let (peer, server) = tokio::io::duplex(16 * 1024);
        peers.push(peer);
        sources.push(accept_xhttp_connection(
            ClosedReplyPeer(server),
            &config,
            &registry,
        ));
    }
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        for base in (0..256u64).step_by(8) {
            // The peer may distribute packets among arbitrary pooled sockets.
            // Bound actual reordering by the configured reference limit, without
            // assuming a particular executor's per-connection scheduling order.
            for (lane, peer) in peers.iter_mut().enumerate() {
                for seq in (base + lane as u64..base + 8).step_by(4) {
                    let request = profile
                        .packet_request("session", seq, &seq.to_be_bytes())
                        .unwrap();
                    let mut wire = format!("POST {} HTTP/1.1\r\n", request.uri());
                    for (name, value) in request.headers() {
                        wire.push_str(&format!("{}: {}\r\n", name, value.to_str().unwrap()));
                    }
                    wire.push_str("Content-Length: 0\r\n\r\n");
                    peer.write_all(wire.as_bytes()).await.unwrap();
                }
            }
            for seq in base..base + 8 {
                assert_eq!(
                    session.next().await.unwrap().unwrap().data.as_ref(),
                    &seq.to_be_bytes()
                );
            }
        }
    })
    .await
    .unwrap();
    drop(sources);
    drop(peers);
}
