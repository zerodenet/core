use super::*;
use std::time::Duration;
use tokio::time::Instant;
use zero_platform_tokio::contract::scope::{TaskClass, TaskScope};

pub(super) fn endpoint(
    drivers: &QuinnDriverOwner,
    native: &MetadataSocketOwner,
) -> quinn::Endpoint {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let key = rustls::pki_types::PrivateKeyDer::Pkcs8(cert.signing_key.serialize_der().into());
    let config = quinn::ServerConfig::with_single_cert(vec![cert.cert.der().clone()], key).unwrap();
    let mut roots = rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let mut endpoint = quinn::Endpoint::new_with_abstract_socket(
        Default::default(),
        Some(config),
        native.socket(),
        drivers.runtime(),
    )
    .unwrap();
    endpoint.set_default_client_config(
        quinn::ClientConfig::with_root_certificates(Arc::new(roots)).unwrap(),
    );
    endpoint
}
#[tokio::test]
async fn actual_quinn_handshake_drivers_have_real_classes_joins_and_original_typed_close_causes() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let mut scope = TaskScope::new(tokio::runtime::Handle::current(), bound(3));
        let (drivers, mut receiver) = QuinnDriverOwner::new(scope.registrar(), bound(3));
        let mut native = socket();
        let endpoint = endpoint(&drivers, &native);
        let (client, server) = tokio::join!(
            endpoint
                .connect(endpoint.local_addr().unwrap(), "localhost")
                .unwrap(),
            async { endpoint.accept().await.unwrap().await },
        );
        let client = client.unwrap();
        let server = server.unwrap();
        let ((), received) = tokio::join!(
            async {
                let mut stream = client.open_uni().await.unwrap();
                stream.write_all(b"actual owned driver").await.unwrap();
                stream.finish().unwrap();
            },
            async {
                server
                    .accept_uni()
                    .await
                    .unwrap()
                    .read_to_end(1024)
                    .await
                    .unwrap()
            },
        );
        assert_eq!(received, b"actual owned driver");
        let retained_view = native.socket();
        client.close(11u32.into(), b"actual owning caller");
        assert!(matches!(
            server.closed().await,
            quinn::ConnectionError::ApplicationClosed(_)
        ));
        drop(client);
        drop(server);
        endpoint.wait_idle().await;
        drop(endpoint);
        let tasks = drivers.tasks();
        assert_eq!(tasks.len(), 3);
        assert_eq!(
            drivers.task_status().unjoined.len(),
            3,
            "body Ready is not host join consumption"
        );
        let deadline_failure = receiver
            .receive(tasks[0], Instant::now())
            .await
            .unwrap_err();
        assert_eq!(
            deadline_failure.reason,
            zero_platform_tokio::contract::Reason::Timeout
        );
        assert_eq!(drivers.task_status().unreceived.len(), 3);
        let mut critical = 0;
        let mut auxiliary = 0;
        for _ in 0..3 {
            let exit = scope.next_exit().await.unwrap();
            exit.result.unwrap();
            match exit.class {
                TaskClass::Critical => critical += 1,
                TaskClass::Auxiliary => auxiliary += 1,
            }
        }
        assert_eq!((critical, auxiliary), (1, 2));
        assert!(drivers.task_status().unjoined.is_empty());
        assert_eq!(
            drivers.task_status().unreceived.len(),
            3,
            "join observation did not accept any application outcome"
        );
        let mut local = 0;
        let mut peer = 0;
        for task in tasks {
            let delivery = receiver
                .receive(task, Instant::now() + Duration::from_secs(1))
                .await
                .unwrap();
            assert_eq!(delivery.joined.id(), task);
            delivery.joined.result().unwrap();
            let body = delivery.body.unwrap();
            assert!(body.io_error.is_none());
            match body.close_reason.as_ref() {
                Some(quinn::ConnectionError::LocallyClosed) => local += 1,
                Some(quinn::ConnectionError::ApplicationClosed(error)) => {
                    assert_eq!(error.error_code, 11u32.into());
                    assert_eq!(error.reason.as_ref(), b"actual owning caller");
                    peer += 1;
                }
                None => assert_eq!(body.origin, quinn::DriverOrigin::Endpoint),
                other => panic!("unexpected original close reason: {other:?}"),
            }
        }
        assert_eq!((local, peer), (1, 1));
        drivers.reclaim_received();
        assert_eq!(drivers.task_status().retained, 0);
        assert_eq!(drivers.task_status().reclaimed, 3);
        // Task joins and caller delivery alone leave the original FD open.
        assert!(retained_view.local_addr().is_ok());
        assert_eq!(native.close_io().main.confirmed_closed, 1);
        assert_eq!(
            retained_view.local_addr().unwrap_err().kind(),
            io::ErrorKind::NotConnected
        );
        assert_eq!(native.close_io().main.confirmed_closed, 0);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn rejected_actual_endpoint_driver_preserves_scope_rejection_without_invented_task_or_close()
{
    let mut scope = TaskScope::suspended(tokio::runtime::Handle::current(), Some(bound(1)));
    let (drivers, _receiver) = QuinnDriverOwner::new(scope.registrar(), bound(1));
    let mut native = socket();
    let endpoint = endpoint(&drivers, &native);
    let status = drivers.task_status();
    assert_eq!(status.pending_registration, 0);
    assert_eq!(status.retained, 0);
    let failure = status.rejection.unwrap();
    assert_eq!(failure.origin, Some(quinn::DriverOrigin::Endpoint));
    assert_eq!(
        failure.reason,
        DriverRegistrationReason::Scope(
            zero_platform_tokio::contract::scope::SpawnError::NotStarted
        )
    );
    assert!(drivers.tasks().is_empty());
    assert!(native.socket().local_addr().is_ok());
    drop(endpoint);
    scope.enable_registration().unwrap();
    let report = scope
        .shutdown(Instant::now() + Duration::from_secs(1))
        .await;
    assert!(
        report.exited_tasks.is_empty(),
        "an unaccepted future did not become TaskExit"
    );
    assert_eq!(native.close_io().main.confirmed_closed, 1);
}
