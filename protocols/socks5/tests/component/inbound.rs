use super::*;
use alloc::{boxed::Box, vec::Vec};
use std::{
    future::Future,
    io,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use zero_core::{
    component::{InboundId, RoleId, SessionId},
    request::Destination,
    Address, ProtocolType,
};

fn facts(target: Destination) -> RequestFacts {
    RequestFacts {
        session: SessionId(34),
        inbound: RoleId::Inbound(InboundId(12)),
        original_source: None,
        effective_source: None,
        local_endpoint: None,
        original_target: None,
        target: Some(target),
        target_source: None,
        subject: None,
        peer: None,
        protocol: ProtocolType::new("socks5"),
        sni: None,
        process: None,
    }
}

#[tokio::test]
async fn real_connect_reply_keeps_wire_unread_payload_half_close_and_failure_codes() {
    for (outcome, code) in [
        (ReplyOutcome::LocalChannelReady, 0),
        (ReplyOutcome::Blocked, 2),
        (ReplyOutcome::UpstreamFailed, 4),
    ] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (io, _) = listener.accept().await.unwrap();
            let mut io = zero_platform_tokio::TokioSocket::new(io);
            let parsed = crate::Socks5Inbound
                .accept_command_facts_with_auth(&mut io, &crate::NoSocks5PasswordAuth)
                .await
                .unwrap();
            assert!(parsed.auth.is_none());
            let crate::Socks5Command::Connect(target) = parsed.command else {
                panic!("actual CONNECT wire request");
            };
            assert_eq!(target, Destination::new(Address::Ipv4([127, 0, 0, 1]), 321));
            let expected = facts(target);
            let reply = ConnectReply::new(io, expected.clone()).unwrap();
            assert_eq!(reply.facts(), &expected);
            let accepted = zero_platform_tokio::contract::inbound::accept_stream(reply);
            let stream = accepted.respond(outcome).await.unwrap();
            if let Some(mut stream) = stream {
                let mut payload = Vec::new();
                stream.read_to_end(&mut payload).await.unwrap();
                assert_eq!(payload, b"pipelined payload");
                // The client's write EOF leaves the opposite direction usable.
                stream.write_all(b"response after EOF").await.unwrap();
                stream.shutdown().await.unwrap();
            } else {
                assert_ne!(outcome, ReplyOutcome::LocalChannelReady);
            }
        });
        let mut client = tokio::net::TcpStream::connect(address).await.unwrap();
        client.write_all(&[5, 1, 0]).await.unwrap();
        let mut method = [0; 2];
        client.read_exact(&mut method).await.unwrap();
        assert_eq!(method, [5, 0]);
        client
            .write_all(b"\x05\x01\x00\x01\x7f\x00\x00\x01\x01\x41pipelined payload")
            .await
            .unwrap();
        client.shutdown().await.unwrap();
        let mut reply = [0; 10];
        client.read_exact(&mut reply).await.unwrap();
        assert_eq!(reply, [5, code, 0, 1, 0, 0, 0, 0, 0, 0]);
        let mut tail = Vec::new();
        client.read_to_end(&mut tail).await.unwrap();
        assert_eq!(
            tail,
            if code == 0 {
                b"response after EOF".as_slice()
            } else {
                &[]
            }
        );
        server.await.unwrap();
    }
}

struct PendingWrite {
    inner: tokio::io::DuplexStream,
    pending: Option<tokio::sync::oneshot::Sender<()>>,
}
impl AsyncRead for PendingWrite {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}
impl AsyncWrite for PendingWrite {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        let result = Pin::new(&mut self.inner).poll_write(cx, bytes);
        if result.is_pending() {
            if let Some(pending) = self.pending.take() {
                let _ = pending.send(());
            }
        }
        result
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

#[tokio::test]
async fn cancelled_actual_pending_reply_does_not_replay_or_hand_over_partial_response() {
    let (io, mut peer) = tokio::io::duplex(1);
    let (sender, mut pending) = tokio::sync::oneshot::channel();
    let reply = ConnectReply::new(
        PendingWrite {
            inner: io,
            pending: Some(sender),
        },
        facts(Destination::new(Address::Ipv4([127, 0, 0, 1]), 321)),
    )
    .unwrap();
    let mut response = Box::pin(reply.respond(ReplyOutcome::LocalChannelReady));
    std::future::poll_fn(|cx| {
        assert!(response.as_mut().poll(cx).is_pending());
        Pin::new(&mut pending).poll(cx).map(Result::unwrap)
    })
    .await;
    drop(response);
    let mut bytes = Vec::new();
    peer.read_to_end(&mut bytes).await.unwrap();
    assert_eq!(bytes, [5]);
}

#[derive(Debug)]
struct OriginalFailure(Arc<()>);
impl std::fmt::Display for OriginalFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("original reply carrier error")
    }
}
impl std::error::Error for OriginalFailure {}
struct BrokenCarrier(Option<io::Error>);
impl AsyncRead for BrokenCarrier {
    fn poll_read(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        _: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Poll::Pending
    }
}
impl AsyncWrite for BrokenCarrier {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        _: &[u8],
    ) -> Poll<io::Result<usize>> {
        Poll::Ready(Err(self.0.take().unwrap()))
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[tokio::test]
async fn response_failure_preserves_the_original_typed_carrier_source_and_identity() {
    let original = Arc::new(());
    let reply = ConnectReply::new(
        BrokenCarrier(Some(io::Error::new(
            io::ErrorKind::BrokenPipe,
            OriginalFailure(original.clone()),
        ))),
        facts(Destination::new(Address::Ipv4([127, 0, 0, 1]), 321)),
    )
    .unwrap();
    let error = match reply.respond(ReplyOutcome::LocalChannelReady).await {
        Err(error) => error,
        Ok(_) => panic!("broken carrier cannot hand over a stream"),
    };
    assert_eq!(error.session, Some(SessionId(34)));
    assert_eq!(error.delivery, DeliveryState::Unknown);
    use std::error::Error;
    let wire = error
        .source()
        .unwrap()
        .downcast_ref::<super::super::error::HandshakeError>()
        .unwrap();
    let source = wire.source().unwrap().downcast_ref::<io::Error>().unwrap();
    assert_eq!(source.kind(), io::ErrorKind::BrokenPipe);
    assert!(Arc::ptr_eq(
        &source
            .get_ref()
            .unwrap()
            .downcast_ref::<OriginalFailure>()
            .unwrap()
            .0,
        &original
    ));
}
