use super::*;
#[path = "stream/fixtures.rs"]
mod fixtures;
#[path = "stream/guards.rs"]
mod guards;
use fixtures::*;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zero_core::request::Destination;
use zero_platform_tokio::contract::{
    context::ConnectFact,
    stream::{ConnectRequest, PeerConfirmation},
};

#[tokio::test]
async fn actual_encrypted_socks_response_is_complete_before_confirmed_and_flow_stays_zero() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (partial, seen_partial) = tokio::sync::oneshot::channel();
    let release = Arc::new(tokio::sync::Notify::new());
    let gate = release.clone();
    let (finish, finished) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let connection = accept(listener).await;
        let mut stream = next_request(&connection).await;
        // The complete BND domain and port must be consumed before confirmation.
        stream.write_all(&[5, 0, 0, 3, 3, b'a']).await.unwrap();
        stream.flush().await.unwrap();
        partial.send(()).unwrap();
        gate.notified().await;
        stream.write_all(&[b'b', b'c', 1, 187]).await.unwrap();
        stream.flush().await.unwrap();
        let mut payload = [0; 4];
        stream.read_exact(&mut payload).await.unwrap();
        assert_eq!(&payload, b"ping");
        stream.write_all(b"pong").await.unwrap();
        stream.flush().await.unwrap();
        let mut end = Vec::new();
        stream.read_to_end(&mut end).await.unwrap();
        assert!(end.is_empty());
        finished.await.unwrap();
    });
    let mut host = Harness::new(address, 1).await;
    let observer = Arc::new(Observations::default());
    let role = host.role(0);
    let context = host.context(observer.clone());
    let mut opening = Box::pin(role.connect(
        ConnectRequest {
            destination: destination(),
        },
        context,
    ));
    tokio::select! {
        _=opening.as_mut()=>panic!("CONNECT cannot complete before the full response"),
        result=seen_partial=>result.unwrap(),
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(40), opening.as_mut())
            .await
            .is_err()
    );
    assert!(!observer
        .facts
        .lock()
        .unwrap()
        .iter()
        .any(|(_, fact)| *fact == ConnectFact::PeerConfirmed));
    release.notify_one();
    let mut connected = opening.await.unwrap();
    assert_eq!(connected.peer_confirmation, PeerConfirmation::Confirmed);
    assert!(
        host.opener
            .traffic
            .sent
            .load(std::sync::atomic::Ordering::Relaxed)
            > 21
    );
    assert!(
        host.opener
            .traffic
            .received
            .load(std::sync::atomic::Ordering::Relaxed)
            > 10
    );
    assert_eq!(observer.traffic.lock().unwrap().as_slice(), &[(None, 0, 0)]);
    assert_eq!(
        observer.facts.lock().unwrap().as_slice(),
        &[
            (None, ConnectFact::CarrierReady),
            (None, ConnectFact::RequestSubmitted),
            (None, ConnectFact::PeerConfirmed)
        ]
    );
    connected.io.write_all(b"ping").await.unwrap();
    let mut payload = [0; 4];
    connected.io.read_exact(&mut payload).await.unwrap();
    assert_eq!(&payload, b"pong");
    connected.io.shutdown().await.unwrap();
    drop(connected);
    host.pool.clear();
    host.close().await;
    finish.send(()).unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn genuine_socks_rejection_retains_original_protocol_error_without_confirmation() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (finish, finished) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let connection = accept(listener).await;
        let mut stream = next_request(&connection).await;
        stream
            .write_all(&[5, 2, 0, 1, 0, 0, 0, 0, 0, 0])
            .await
            .unwrap();
        stream.flush().await.unwrap();
        drop(stream);
        finished.await.unwrap();
    });
    let mut host = Harness::new(address, 1).await;
    let observer = Arc::new(Observations::default());
    let error = host
        .role(0)
        .connect(
            ConnectRequest {
                destination: destination(),
            },
            host.context(observer.clone()),
        )
        .await
        .err()
        .unwrap();
    assert_eq!(error.stage, Stage::Handshake);
    assert_eq!(error.reason, Reason::Protocol);
    assert_eq!(error.delivery, DeliveryState::Unknown);
    assert_eq!(error.session, None);
    assert!(original_protocol(&error).is_some_and(|error| matches!(
        error,
        zero_core::Error::Protocol("mieru socks5: connect rejected")
    )));
    assert_eq!(
        observer.facts.lock().unwrap().last(),
        Some(&(None, ConnectFact::PeerRejected))
    );
    assert!(!observer
        .facts
        .lock()
        .unwrap()
        .iter()
        .any(|(_, fact)| *fact == ConnectFact::PeerConfirmed));
    assert_eq!(observer.traffic.lock().unwrap().as_slice(), &[(None, 0, 0)]);
    host.pool.clear();
    host.close().await;
    finish.send(()).unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn cancelling_a_pending_real_target_response_preserves_another_canonical_consumer() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (submitted, seen_request) = tokio::sync::oneshot::channel();
    let (finish, finished) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let connection = accept(listener).await;
        let mut first = next_request(&connection).await;
        submitted.send(()).unwrap();
        let mut end = Vec::new();
        first.read_to_end(&mut end).await.unwrap();
        assert!(end.is_empty());
        drop(first);
        let mut second = next_request(&connection).await;
        second
            .write_all(&[5, 0, 0, 1, 0, 0, 0, 0, 0, 0])
            .await
            .unwrap();
        second.flush().await.unwrap();
        let mut end = Vec::new();
        second.read_to_end(&mut end).await.unwrap();
        assert!(end.is_empty());
        finished.await.unwrap();
    });
    let mut host = Harness::new(address, 2).await;
    let observer = Arc::new(Observations::default());
    let first = host.role(0);
    let context = host.context(observer.clone());
    let cancel = context.cancel.clone();
    let pending = tokio::spawn(async move {
        first
            .connect(
                ConnectRequest {
                    destination: destination(),
                },
                context,
            )
            .await
    });
    seen_request.await.unwrap();
    cancel.cancel();
    let error = pending.await.unwrap().err().unwrap();
    assert_eq!(error.reason, Reason::Cancelled);
    assert_eq!(error.stage, Stage::Handshake);
    assert_eq!(error.delivery, DeliveryState::Unknown);
    assert!(!observer
        .facts
        .lock()
        .unwrap()
        .iter()
        .any(|(_, fact)| *fact == ConnectFact::PeerConfirmed));
    let mut second = host
        .role(1)
        .connect(
            ConnectRequest {
                destination: destination(),
            },
            host.context(Arc::new(Observations::default())),
        )
        .await
        .unwrap();
    assert_eq!(second.peer_confirmation, PeerConfirmation::Confirmed);
    assert_eq!(
        host.opener.opens.load(std::sync::atomic::Ordering::SeqCst),
        1
    );
    second.io.shutdown().await.unwrap();
    drop(second);
    host.pool.clear();
    host.close().await;
    finish.send(()).unwrap();
    server.await.unwrap();
}

fn original_protocol<'a>(
    error: &'a (dyn std::error::Error + 'static),
) -> Option<&'a zero_core::Error> {
    let mut current = Some(error);
    while let Some(error) = current {
        if let Some(protocol) = error.downcast_ref::<zero_core::Error>() {
            return Some(protocol);
        }
        current = error.source();
    }
    None
}
