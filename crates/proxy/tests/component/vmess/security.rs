use super::fixtures::*;
use std::sync::Arc;
use tokio::{
    io::AsyncReadExt,
    net::TcpListener,
    sync::Notify,
    time::{timeout, Duration},
};
use zero_platform_tokio::contract::{stream::ConnectRequest, DeliveryState, Reason};

#[tokio::test]
async fn actual_native_vmess_tls_role_preserves_secure_verification_and_submits_no_proxy_request_on_failure(
) {
    timeout(Duration::from_secs(5), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let acceptor = acceptor_with_alpn(false);
        let server = tokio::spawn(async move {
            let (io, _) = listener.accept().await.unwrap();
            assert!(acceptor.accept(io).await.is_err());
        });
        let observations = Arc::new(Observations::default());
        let mut tls = tls_profile();
        tls.insecure = false;
        let (mut instance, mut scope) = instance(address, "aes-128-gcm", Some(&tls), None, None);
        instance
            .lifecycle
            .start(super::super::fixtures::deadline())
            .await
            .unwrap();
        let result = instance
            .roles
            .stream
            .as_ref()
            .unwrap()
            .connect(
                ConnectRequest {
                    destination: destination(),
                },
                context(address, observations.clone()),
            )
            .await;
        let error = match result {
            Ok(_) => panic!("self-signed TLS must fail secure verification"),
            Err(error) => error,
        };
        assert_eq!(error.reason, Reason::Io);
        assert_eq!(error.delivery, DeliveryState::NotSubmitted);
        assert_eq!(
            error.retry,
            zero_platform_tokio::contract::RetryClass::BeforeSubmission
        );
        assert!(std::error::Error::source(&error).is_some());
        assert!(observations.facts.lock().unwrap().is_empty());
        assert!(observations.protocol.lock().unwrap().is_none());
        server.await.unwrap();
        instance
            .lifecycle
            .shutdown(super::super::fixtures::deadline())
            .await
            .unwrap();
        assert!(scope
            .shutdown(super::super::fixtures::deadline())
            .await
            .is_complete());
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn cancellation_at_an_actual_pending_tls_read_drops_the_real_carrier_without_replay() {
    timeout(Duration::from_secs(5), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let entered = Arc::new(Notify::new());
        let signal = entered.clone();
        let server = tokio::spawn(async move {
            let (mut io, _) = listener.accept().await.unwrap();
            let mut header = [0; 5];
            io.read_exact(&mut header).await.unwrap();
            assert_eq!(header[0], 22);
            let mut hello = vec![0; u16::from_be_bytes([header[3], header[4]]) as usize];
            io.read_exact(&mut hello).await.unwrap();
            signal.notify_one();
            let mut remainder = Vec::new();
            io.read_to_end(&mut remainder).await.unwrap();
        });
        let observations = Arc::new(Observations::default());
        let tls = tls_profile();
        let (mut instance, mut scope) = instance(address, "aes-128-gcm", Some(&tls), None, None);
        instance
            .lifecycle
            .start(super::super::fixtures::deadline())
            .await
            .unwrap();
        let role = instance.roles.stream.clone().unwrap();
        let context = context(address, observations.clone());
        let cancel = context.cancel.clone();
        let request = tokio::spawn(async move {
            role.connect(
                ConnectRequest {
                    destination: destination(),
                },
                context,
            )
            .await
        });
        entered.notified().await;
        assert!(
            !request.is_finished(),
            "no server hello was sent; TLS read must still be pending"
        );
        cancel.cancel();
        let error = match request.await.unwrap() {
            Ok(_) => panic!("cancelled open must fail"),
            Err(error) => error,
        };
        assert_eq!(error.reason, Reason::Cancelled);
        assert_eq!(error.delivery, DeliveryState::Unknown);
        server.await.unwrap();
        assert!(observations.facts.lock().unwrap().is_empty());
        instance
            .lifecycle
            .shutdown(super::super::fixtures::deadline())
            .await
            .unwrap();
        assert!(scope
            .shutdown(super::super::fixtures::deadline())
            .await
            .is_complete());
    })
    .await
    .unwrap();
}
