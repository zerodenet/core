use super::fixtures::*;
use std::{
    error::Error,
    io,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use zero_platform_tokio::contract::{
    component::ComponentFactory,
    context::ConnectFact,
    stream::{ConnectRequest, PeerConfirmation},
    CloseReason, DeliveryState, Reason, Stage,
};

#[tokio::test]
async fn authenticated_service_uses_selected_prefix_and_preserves_pre_read_and_half_close() {
    let (mut instance, mut scope) = instance(true).await;
    let (client, mut peer) = tokio::io::duplex(1024);
    let opener = Opener::new(Box::pin(client));
    let facts = Arc::new(Facts::default());
    let server = tokio::spawn(async move {
        let mut greeting = [0; 3];
        peer.read_exact(&mut greeting).await.unwrap();
        assert_eq!(greeting, [5, 1, 2]);
        peer.write_all(&[5, 2]).await.unwrap();
        let mut credentials = [0; 13];
        peer.read_exact(&mut credentials).await.unwrap();
        assert_eq!(&credentials, b"\x01\x04user\x06secret");
        peer.write_all(&[1, 0]).await.unwrap();
        let mut request = [0; 10];
        peer.read_exact(&mut request).await.unwrap();
        assert_eq!(request, [5, 1, 0, 1, 203, 0, 113, 9, 1, 187]);
        peer.write_all(&[5, 0, 0, 1, 127, 0, 0, 1, 0, 0])
            .await
            .unwrap();
        peer.write_all(b"pre-read").await.unwrap();
        let mut payload = Vec::new();
        peer.read_to_end(&mut payload).await.unwrap();
        assert_eq!(payload, b"request");
        peer.write_all(b"tail").await.unwrap();
        peer.shutdown().await.unwrap();
    });
    let ctx = context(opener.clone(), facts.clone());
    let expected = ctx.upstream.binding().clone();
    let mut connected = instance
        .roles
        .stream
        .as_ref()
        .unwrap()
        .connect(
            ConnectRequest {
                destination: destination(),
            },
            ctx,
        )
        .await
        .unwrap();
    assert_eq!(connected.peer_confirmation, PeerConfirmation::Confirmed);
    connected.io.write_all(b"request").await.unwrap();
    connected.io.shutdown().await.unwrap();
    let mut response = Vec::new();
    connected.io.read_to_end(&mut response).await.unwrap();
    assert_eq!(response, b"pre-readtail");
    server.await.unwrap();
    assert_eq!(
        *opener.calls.lock().unwrap(),
        vec![(expected, endpoint(), None)]
    );
    assert_eq!(
        *facts.0.lock().unwrap(),
        vec![
            (None, ConnectFact::CarrierReady),
            (None, ConnectFact::RequestSubmitted),
            (None, ConnectFact::PeerConfirmed)
        ]
    );
    instance.lifecycle.revoke(CloseReason::Shutdown);
    assert!(instance
        .lifecycle
        .shutdown(deadline())
        .await
        .unwrap()
        .is_complete());
    assert!(scope.shutdown(deadline()).await.is_complete());
}

#[tokio::test]
async fn handshake_failure_retains_original_os_error_without_false_confirmation() {
    let (instance, _scope) = instance(false).await;
    let facts = Arc::new(Facts::default());
    let error = match instance
        .roles
        .stream
        .as_ref()
        .unwrap()
        .connect(
            ConnectRequest {
                destination: destination(),
            },
            context(Opener::new(Box::pin(FailedIo)), facts.clone()),
        )
        .await
    {
        Ok(_) => panic!("failed carrier must fail"),
        Err(error) => error,
    };
    assert_eq!(error.stage, Stage::Handshake);
    assert_eq!(error.reason, Reason::Io);
    assert_eq!(error.component.unwrap().0, 7);
    assert!(error.session.is_none());
    assert_eq!(
        error
            .source()
            .unwrap()
            .source()
            .unwrap()
            .downcast_ref::<io::Error>()
            .unwrap()
            .raw_os_error(),
        Some(104)
    );
    assert_eq!(
        *facts.0.lock().unwrap(),
        vec![(None, ConnectFact::CarrierReady)]
    );
}

#[tokio::test]
async fn cancellation_during_handshake_closes_only_the_selected_stream_and_does_not_replay() {
    let (instance, _scope) = instance(false).await;
    let (client, mut peer) = tokio::io::duplex(256);
    let opener = Opener::new(Box::pin(client));
    let ctx = context(opener.clone(), Arc::new(Facts::default()));
    let cancel = ctx.cancel.clone();
    let role = instance.roles.stream.as_ref().unwrap();
    let future = role.connect(
        ConnectRequest {
            destination: destination(),
        },
        ctx,
    );
    tokio::pin!(future);
    let error = tokio::select! {
        result=&mut future=>match result {Err(e)=>e,Ok(_)=>panic!("unexpected success")},
        _=async { let mut greeting=[0;3];peer.read_exact(&mut greeting).await.unwrap();cancel.cancel(); }=>match future.await {Err(e)=>e,Ok(_)=>panic!("unexpected success")},
    };
    assert_eq!(error.stage, Stage::Handshake);
    assert_eq!(error.reason, Reason::Cancelled);
    assert_eq!(error.delivery, DeliveryState::Unknown);
    assert_eq!(opener.calls.lock().unwrap().len(), 1);
    assert_eq!(peer.read(&mut [0; 1]).await.unwrap(), 0);
}

#[test]
fn options_validate_once_and_keep_credentials_out_of_debug_and_static_description() {
    let a = socks5::component::Options::new(
        endpoint(),
        Some("private-user"),
        Some("private-secret"),
        Default::default(),
    )
    .unwrap();
    let b = socks5::component::Options::new(
        endpoint(),
        Some("private-user"),
        Some("different-secret"),
        Default::default(),
    )
    .unwrap();
    let spec = socks5::component::Factory.describe(&a);
    assert_eq!(spec, socks5::component::Factory.describe(&a));
    assert_ne!(
        spec.static_identity,
        socks5::component::Factory.describe(&b).static_identity
    );
    let debug = format!("{a:?} {spec:?}");
    assert!(!debug.contains("private-user"));
    assert!(!debug.contains("private-secret"));
    assert!(
        socks5::component::Options::new(endpoint(), Some("user"), None, Default::default())
            .is_err()
    );
    assert!(socks5::component::Options::new(
        endpoint(),
        Some(""),
        Some("secret"),
        Default::default()
    )
    .is_err());
}
struct FailedIo;
impl AsyncRead for FailedIo {
    fn poll_read(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        _: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Poll::Ready(Err(io::Error::from_raw_os_error(104)))
    }
}
impl AsyncWrite for FailedIo {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        Poll::Ready(Ok(bytes.len()))
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}
