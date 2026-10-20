use super::fixtures::*;
use std::{
    io,
    sync::{atomic::Ordering, Arc},
};
use zero_platform_tokio::contract::{stream::ConnectRequest, Reason, Stage};

#[tokio::test]
async fn native_file_ca_failures_preserve_read_and_pem_sources_and_never_admit_streams() {
    for failure in ["missing", "empty", "malformed", "oversized"] {
        let directory = Directory::new();
        let path = directory.0.join("identity.pem");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        match failure {
            "empty" => std::fs::write(&path, b"").unwrap(),
            "malformed" => std::fs::write(
                &path,
                b"-----BEGIN CERTIFICATE-----\ninvalid!\n-----END CERTIFICATE-----\n",
            )
            .unwrap(),
            "oversized" => std::fs::write(&path, b"two").unwrap(),
            _ => {}
        }
        let bound = if failure == "oversized" { 1 } else { 4096 };
        let (mut instance, mut scope, grants) = build(
            options(address, &tls_profile(), None, None, &directory.0, bound),
            &path,
            bound,
        );
        let access = grants.activate(deadline()).unwrap();
        let error = instance.lifecycle.start(deadline()).await.unwrap_err();
        drop(access);
        assert_eq!(error.stage, Stage::Prepare);
        assert_eq!(
            error.reason,
            match failure {
                "missing" => Reason::Io,
                "oversized" => Reason::Capacity,
                _ => Reason::InvalidConfig,
            }
        );
        assert_eq!(
            io_kind(&error),
            Some(match failure {
                "missing" => io::ErrorKind::NotFound,
                "oversized" => io::ErrorKind::InvalidData,
                _ => io::ErrorKind::InvalidInput,
            })
        );
        let observations = Arc::new(Observations::default());
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
            Ok(_) => panic!("failed CA start admitted {failure}"),
            Err(error) => error,
        };
        assert_eq!(error.reason, Reason::Revoked);
        assert_eq!(observations.dials.load(Ordering::Relaxed), 0);
        assert!(observations.facts.lock().unwrap().is_empty());
        instance.lifecycle.shutdown(deadline()).await.unwrap();
        assert!(scope.shutdown(deadline()).await.is_complete());
        let report = grants.shutdown_io(deadline()).await.unwrap();
        assert!(report.is_complete());
        assert_eq!(report.released_resources.len(), 1);
    }
}

#[tokio::test]
async fn declared_ca_still_requires_the_real_start_grant_phase() {
    let directory = Directory::new();
    let path = directory.0.join("identity.pem");
    let (_, pem) = tls_server(false);
    std::fs::write(&path, &pem).unwrap();
    let address = "127.0.0.1:443".parse().unwrap();
    let (mut instance, mut scope, grants) = build(
        options(address, &tls_profile(), None, None, &directory.0, pem.len()),
        &path,
        pem.len(),
    );
    assert_eq!(
        instance
            .lifecycle
            .start(deadline())
            .await
            .unwrap_err()
            .reason,
        Reason::InvalidConfig
    );
    instance.lifecycle.shutdown(deadline()).await.unwrap();
    assert!(scope.shutdown(deadline()).await.is_complete());
    let report = grants.shutdown_io(deadline()).await.unwrap();
    assert!(report.is_complete());
    assert!(report.released_resources.is_empty());
}

#[tokio::test]
async fn prepared_file_roots_still_reject_an_untrusted_actual_tls_peer_without_vmess_submission() {
    use tokio::time::{timeout, Duration};
    use zero_platform_tokio::contract::DeliveryState;
    timeout(Duration::from_secs(3), async {
        let directory = Directory::new();
        let path = directory.0.join("identity.pem");
        let (acceptor, _) = tls_server(false);
        let (_, different_authority) = tls_server(false);
        std::fs::write(&path, &different_authority).unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let peer = tokio::spawn(async move {
            let (io, _) = listener.accept().await.unwrap();
            assert!(acceptor.accept(io).await.is_err());
        });
        let (mut instance, mut scope, grants) = build(
            options(
                address,
                &tls_profile(),
                None,
                None,
                &directory.0,
                different_authority.len(),
            ),
            &path,
            different_authority.len(),
        );
        let access = grants.activate(deadline()).unwrap();
        instance.lifecycle.start(deadline()).await.unwrap();
        drop(access);
        std::fs::remove_file(&path).unwrap();
        let observations = Arc::new(Observations::default());
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
            Ok(_) => panic!("untrusted TLS peer admitted"),
            Err(error) => error,
        };
        assert_eq!(error.stage, Stage::Handshake);
        assert_eq!(error.reason, Reason::Io);
        assert_eq!(error.delivery, DeliveryState::NotSubmitted);
        assert!(std::error::Error::source(&error).is_some());
        assert!(observations.facts.lock().unwrap().is_empty());
        assert!(observations.protocol.lock().unwrap().is_none());
        peer.await.unwrap();
        instance.lifecycle.shutdown(deadline()).await.unwrap();
        assert!(scope.shutdown(deadline()).await.is_complete());
        assert!(grants.shutdown_io(deadline()).await.unwrap().is_complete());
    })
    .await
    .unwrap();
}
