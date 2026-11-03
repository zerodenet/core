use super::*;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use zero_core::component::{ComponentId, ResourceGeneration, RoleInstanceId};
use zero_platform_tokio::{
    contract::{
        context::{AcceptContext, HandshakeContext, SessionIdentitySource},
        inbound::{
            AcceptedRequest, MessageAction, MessageContext, MessageTransfer, RequestMultiplexer,
            StreamActivity,
        },
        CancellationToken,
    },
    TcpRelayStream,
};
struct Identities(AtomicU64);
impl SessionIdentitySource for Identities {
    fn allocate(&self) -> Result<SessionId, ChannelError> {
        Ok(SessionId(self.0.fetch_add(1, Ordering::SeqCst)))
    }
}
fn handshake(ids: Arc<Identities>) -> HandshakeContext {
    HandshakeContext {
        accepted: None,
        connection: None,
        inbound: RoleInstanceId {
            component: ComponentId(12),
            role: RoleId::Inbound(InboundId(12)),
            generation: ResourceGeneration(1),
        },
        identities: ids,
        deadline: tokio::time::Instant::now() + std::time::Duration::from_secs(5),
        cancel: CancellationToken::new(),
    }
}
fn accept() -> AcceptContext {
    AcceptContext {
        deadline: None,
        cancel: CancellationToken::new(),
    }
}
#[derive(Debug, Default)]
struct Counter {
    read: AtomicUsize,
    written: AtomicUsize,
}
impl zero_traits::IoObserver for Counter {
    fn received(&self, n: usize) {
        self.read.fetch_add(n, Ordering::SeqCst);
    }
    fn sent(&self, n: usize) {
        self.written.fetch_add(n, Ordering::SeqCst);
    }
    fn error(&self) {}
    fn dropped(&self) {}
}
fn message_context(a: Arc<Counter>, b: Arc<Counter>) -> MessageContext {
    MessageContext::new(None, None, StreamActivity::new(), a, b)
}

#[tokio::test]
async fn source_accepts_without_reading_headers_and_cancellation_keeps_original_continuation() {
    let (io, mut peer) = tokio::io::duplex(4096);
    let mut source = Requests::new(TcpRelayStream::new(io));
    assert_eq!(source.max_in_flight().get(), 1);
    let pending = source.next(accept()).await.unwrap().unwrap();
    let ids = Arc::new(Identities(AtomicU64::new(31)));
    let mut completing = Box::pin(pending.complete(handshake(ids.clone())));
    std::future::poll_fn(|cx| {
        assert!(completing.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    assert_eq!(ids.0.load(Ordering::SeqCst), 31);
    peer.write_all(b"GET http://origin.test:80/one HTTP/1.1\r\nHost: origin.test\r\n\r\n")
        .await
        .unwrap();
    let AcceptedRequest::Message(first) = completing.await.unwrap() else {
        panic!("message");
    };
    assert_eq!(first.facts().session, SessionId(31));
    // Actually poll the wait for the previous framed exchange, then cancel
    // that one await. The same receiver and stream must remain in the source.
    let mut waiting = source.next(accept());
    std::future::poll_fn(|cx| {
        assert!(waiting.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(waiting);
    let (upstream, mut origin) = tokio::io::duplex(4096);
    let request_len =
        b"GET /one HTTP/1.1\r\nHost: origin.test:80\r\nConnection: close\r\n\r\n".len();
    let response = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok";
    let server = tokio::spawn(async move {
        let mut head = vec![0; request_len];
        origin.read_exact(&mut head).await.unwrap();
        assert_eq!(
            head,
            b"GET /one HTTP/1.1\r\nHost: origin.test:80\r\nConnection: close\r\n\r\n"
        );
        origin.write_all(response).await.unwrap();
    });
    let inbound = Arc::new(Counter::default());
    let outbound = Arc::new(Counter::default());
    let effective_target = first.facts().target.clone().unwrap();
    assert!(matches!(
        first
            .respond(MessageAction::Transfer {
                upstream: TcpRelayStream::new(upstream),
                effective_target,
                context: message_context(inbound.clone(), outbound.clone())
            })
            .await
            .unwrap(),
        MessageTransfer::Complete
    ));
    let mut received = vec![0; response.len()];
    peer.read_exact(&mut received).await.unwrap();
    assert_eq!(received, response);
    server.await.unwrap();
    assert_eq!(inbound.read.load(Ordering::SeqCst), request_len);
    assert_eq!(outbound.written.load(Ordering::SeqCst), request_len);
    assert_eq!(inbound.written.load(Ordering::SeqCst), response.len());
    assert_eq!(outbound.read.load(Ordering::SeqCst), response.len());
    // Unbounded keep-alive waiting stays outside the finite header budget.
    // Cancelling the actual pending lookahead preserves the original stream.
    let mut idle = source.next(AcceptContext {
        deadline: None,
        cancel: CancellationToken::new(),
    });
    std::future::poll_fn(|cx| {
        assert!(idle.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(idle);
    assert_eq!(ids.0.load(Ordering::SeqCst), 32);
    // An explicit caller deadline is authoritative and also retains custody.
    let result = source
        .next(AcceptContext {
            deadline: Some(tokio::time::Instant::now() - std::time::Duration::from_secs(1)),
            cancel: CancellationToken::new(),
        })
        .await;
    assert!(matches!(result, Err(error) if error.reason == Reason::Timeout));
    peer.write_all(b"CONNECT other.test:443 HTTP/1.1\r\nHost: other.test\r\n\r\npayload")
        .await
        .unwrap();
    let AcceptedRequest::Stream(next) = source
        .next(accept())
        .await
        .unwrap()
        .unwrap()
        .complete(handshake(ids.clone()))
        .await
        .unwrap()
    else {
        panic!("connect");
    };
    assert_eq!(next.facts().session, SessionId(32));
    let mut stream = next
        .respond(ReplyOutcome::LocalChannelReady)
        .await
        .unwrap()
        .unwrap();
    let mut payload = [0; 7];
    stream.read_exact(&mut payload).await.unwrap();
    assert_eq!(&payload, b"payload");
    assert!(source.next(accept()).await.unwrap().is_none());
    // Exhausting the source does not close the CONNECT-owned byte view.
    stream.write_all(b"live").await.unwrap();
    stream.shutdown().await.unwrap();
    let mut tail = Vec::new();
    peer.read_to_end(&mut tail).await.unwrap();
    assert_eq!(tail, b"HTTP/1.1 200 Connection Established\r\n\r\nlive");
    assert_eq!(ids.0.load(Ordering::SeqCst), 33);
}

#[tokio::test]
async fn message_upgrade_hands_back_actual_streams_and_preserves_unread_bidirectional_payload() {
    let (io, mut peer) = tokio::io::duplex(4096);
    let mut source = Requests::new(TcpRelayStream::new(io));
    peer.write_all(b"GET http://origin.test/socket HTTP/1.1\r\nHost: origin.test\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\nclient frame").await.unwrap();
    let AcceptedRequest::Message(request) = source
        .next(accept())
        .await
        .unwrap()
        .unwrap()
        .complete(handshake(Arc::new(Identities(AtomicU64::new(1)))))
        .await
        .unwrap()
    else {
        panic!("message");
    };
    let (upstream, mut origin) = tokio::io::duplex(4096);
    let target = request.facts().target.clone().unwrap();
    let server = tokio::spawn(async move {
        let mut bytes = Vec::new();
        while !bytes.ends_with(b"\r\n\r\n") {
            bytes.push(origin.read_u8().await.unwrap());
        }
        origin.write_all(b"HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\nserver frame").await.unwrap();
        origin
    });
    let MessageTransfer::Upgraded {
        mut inbound,
        mut upstream,
    } = request
        .respond(MessageAction::Transfer {
            upstream: TcpRelayStream::new(upstream),
            effective_target: target,
            context: message_context(Arc::default(), Arc::default()),
        })
        .await
        .unwrap()
    else {
        panic!("upgrade");
    };
    assert!(source.next(accept()).await.unwrap().is_none());
    let mut client_frame = [0; 12];
    inbound.read_exact(&mut client_frame).await.unwrap();
    assert_eq!(&client_frame, b"client frame");
    let mut server_frame = [0; 12];
    upstream.read_exact(&mut server_frame).await.unwrap();
    assert_eq!(&server_frame, b"server frame");
    let mut origin = server.await.unwrap();
    upstream.write_all(b"still owned").await.unwrap();
    let mut bytes = [0; 11];
    origin.read_exact(&mut bytes).await.unwrap();
    assert_eq!(&bytes, b"still owned");
}

#[tokio::test]
async fn forward_failure_keeps_actual_carrier_cause_session_and_uncertain_delivery() {
    use std::error::Error;
    let (io, mut peer) = tokio::io::duplex(1024);
    let mut source = Requests::new(TcpRelayStream::new(io));
    peer.write_all(b"GET http://origin.test/ HTTP/1.1\r\nHost: origin.test\r\n\r\n")
        .await
        .unwrap();
    let AcceptedRequest::Message(request) = source
        .next(accept())
        .await
        .unwrap()
        .unwrap()
        .complete(handshake(Arc::new(Identities(AtomicU64::new(43)))))
        .await
        .unwrap()
    else {
        panic!("message");
    };
    let target = request.facts().target.clone().unwrap();
    let original = Arc::new(());
    let result = request
        .respond(MessageAction::Transfer {
            upstream: TcpRelayStream::new(BrokenCarrier(Some(io::Error::new(
                io::ErrorKind::BrokenPipe,
                OriginalFailure(original.clone()),
            )))),
            effective_target: target,
            context: message_context(Arc::default(), Arc::default()),
        })
        .await;
    let error = match result {
        Err(error) => error,
        Ok(_) => panic!("actual failed write"),
    };
    assert_eq!(error.session, Some(SessionId(43)));
    assert_eq!(error.delivery, DeliveryState::Unknown);
    let carrier = error
        .source()
        .unwrap()
        .source()
        .unwrap()
        .downcast_ref::<io::Error>()
        .unwrap();
    assert_eq!(carrier.kind(), io::ErrorKind::BrokenPipe);
    assert!(Arc::ptr_eq(
        &carrier
            .get_ref()
            .unwrap()
            .downcast_ref::<OriginalFailure>()
            .unwrap()
            .0,
        &original
    ));
    assert!(source.next(accept()).await.unwrap().is_none());
}
