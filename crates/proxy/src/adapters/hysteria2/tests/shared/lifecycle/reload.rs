//! Public configuration ACKs exercise the same actual HY2 owners and crypto.
use super::*;
use crate::ProxyHandle;
use zero_config::RuntimeConfig;
use zero_core::{
    component::AssociationId,
    request::{DatagramTargets, Destination},
};
use zero_platform_tokio::{
    contract::datagram::DatagramRequest,
    controlled_carrier::{loan::DatagramLoan, CarrierOrigin, CarrierOriginOwner},
    controlled_datagram::metadata_pool::MetadataPoolOrigin,
};

pub(super) fn config(server: &Server, ca: Option<&std::path::Path>) -> RuntimeConfig {
    let mut protocol = serde_json::json!({
        "type":"hysteria2", "server":"127.0.0.1", "server_name":"localhost",
        "port":server.port, "password":"p", "insecure":ca.is_none()
    });
    if let Some(path) = ca {
        protocol["ca_cert_path"] = path.to_str().unwrap().into();
    }
    RuntimeConfig::parse(
        &serde_json::json!({
            "outbounds":[{"tag":"hy","protocol":protocol}],
            "route":{"final":{"type":"route","outbound":"hy"}}
        })
        .to_string(),
    )
    .unwrap()
}

pub(super) async fn running(config: RuntimeConfig) -> (Proxy, crate::RunningProxy, ProxyHandle) {
    let proxy = Proxy::new(config.clone()).unwrap();
    let running = proxy.spawn();
    let handle = ProxyHandle::new(
        zero_engine::EngineHandle::new(proxy.engine().clone()),
        proxy.clone(),
    );
    // The public ACK waits for actual startup and establishes a committed pass.
    handle
        .apply_runtime_config_and_wait(config, Duration::from_secs(5))
        .await
        .unwrap();
    (proxy, running, handle)
}

pub(super) async fn loan(proxy: &Proxy, association: u64) -> DatagramLoan {
    let services = proxy.tcp_runtime_services().execution();
    let slot = services
        .components
        .as_ref()
        .unwrap()
        .configured("hy")
        .unwrap();
    let role = slot.datagram(end()).await.unwrap();
    let loan = role
        .open_carrier(
            DatagramRequest {
                association: AssociationId(association),
                targets: DatagramTargets::Fixed(Destination::new(
                    Address::Ipv4([203, 0, 113, 2]),
                    53,
                )),
            },
            OpenContext {
                identity: RequestIdentity::Service(Arc::new(
                    zero_core::request::ServiceRequestFacts {
                        origin: zero_core::request::ServiceOrigin::Runtime,
                        purpose: zero_core::request::ServicePurpose::BackgroundControl,
                    },
                )),
                deadline: Some(end()),
                cancel: CancellationToken::new(),
                observer: None,
                upstream: UpstreamTransport::new(
                    UpstreamBinding {
                        prefix: vec![],
                        dial: slot.dial.clone(),
                        network_generation: zero_core::component::NetworkGeneration(
                            proxy.egress_interface.generation(),
                        ),
                    },
                    Arc::new(Denied),
                ),
            },
        )
        .await
        .unwrap();
    loan.io.send(b"active UDP").await.unwrap();
    let mut response = [0; 32];
    let count = loan.io.receive(&mut response).await.unwrap();
    assert_eq!(&response[..count], b"active UDP");
    loan
}

pub(super) fn original(root: &CarrierOrigin) -> MetadataPoolOrigin {
    match root.owner() {
        CarrierOriginOwner::Metadata(owner) => owner.current_original().unwrap(),
        _ => panic!("the actual HY2 metadata owner is required"),
    }
}

fn admission(proxy: &Proxy) -> zero_runtime::component::ServiceAdmission {
    proxy
        .tcp_runtime_services()
        .execution()
        .components
        .as_ref()
        .unwrap()
        .configured("hy")
        .unwrap()
        .service_admission()
        .unwrap()
}

async fn return_views(
    stream: zero_platform_tokio::TcpRelayStream,
    held: DatagramLoan,
    prior: &MetadataPoolOrigin,
    applying: &tokio::task::JoinHandle<Result<(), String>>,
) {
    assert!(
        !applying.is_finished(),
        "active original users block the final ACK"
    );
    assert!(prior.closed().is_none());
    drop(stream);
    assert!(
        !applying.is_finished(),
        "the actual UDP view still holds custody"
    );
    let DatagramLoan { io, mut owner } = held;
    let incomplete = owner.close(tokio::time::Instant::now()).await.unwrap();
    assert!(!incomplete.is_complete());
    assert!(owner.closed().is_none());
    drop(io);
    assert!(
        prior.closed().is_none(),
        "view return is not the original controller report"
    );
    let report = owner.close(end()).await.unwrap();
    assert!(report.is_complete());
    assert!(report.closed_io.is_empty() && report.exited_tasks.is_empty());
    assert!(owner.closed().is_some());
    assert!(
        prior.closed().is_none(),
        "the transferred controller must also return"
    );
    drop(owner);
}

fn closed(root: &CarrierOrigin, prior: &MetadataPoolOrigin) {
    let receipt = prior.closed().expect("the original raw controller closed");
    assert!(receipt.matches(prior) && receipt.had_native_acquisition());
    let receipt = root.closed().expect("all original pool slots closed");
    assert!(receipt.matches(root) && receipt.had_native_acquisition());
}

#[tokio::test]
async fn hysteria2_public_reload_waits_for_active_tcp_udp_views_and_original_controls_before_ack() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let previous = Server::new().await;
        let next = Server::new().await;
        let (proxy, running, handle) = running(config(&previous, None)).await;
        let held_tcp = connect(&proxy, "hy").await;
        let held_udp = loan(&proxy, 910).await;
        assert_eq!(previous.authenticated.load(Ordering::SeqCst), 1);
        let root = held_udp.owner.original().clone();
        let prior = original(&root);
        let revision = proxy.engine().config_revision();
        let original_admission = admission(&proxy);
        let candidate = config(&next, None);
        let applying = tokio::spawn(async move {
            handle
                .apply_runtime_config_and_wait(candidate, Duration::from_secs(5))
                .await
        });
        let revoked = original_admission.revoked().await;
        assert_eq!(
            revoked.reason,
            zero_platform_tokio::contract::Reason::StalePlan
        );
        eprintln!("actual prior HY2 role admission retired; original users/control retained");
        assert_eq!(
            proxy.engine().config_revision(),
            revision,
            "no ACK/revision while the old actual owner remains open"
        );
        assert_eq!(next.authenticated.load(Ordering::SeqCst), 0);
        return_views(held_tcp, held_udp, &prior, &applying).await;
        applying.await.unwrap().unwrap();
        closed(&root, &prior);
        eprintln!("actual old HY2 native/pool close receipts match before successor traffic");
        assert_eq!(proxy.engine().config_revision(), revision + 1);
        previous.wait_closed(1).await;
        let successor_tcp = connect(&proxy, "hy").await;
        let successor_udp = loan(&proxy, 911).await;
        assert_eq!(
            next.authenticated.load(Ordering::SeqCst),
            1,
            "new TCP and UDP share the one actual successor authentication"
        );
        assert_ne!(successor_udp.owner.original().instance(), root.instance());
        assert_eq!(previous.authenticated.load(Ordering::SeqCst), 1);
        drop(successor_tcp);
        let DatagramLoan { io, mut owner } = successor_udp;
        drop(io);
        assert!(owner.close(end()).await.unwrap().is_complete());
        drop(owner);
        running.shutdown().await.unwrap();
        previous.close().await;
        next.close().await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn hysteria2_public_same_path_bad_ca_reload_restores_last_good_prepared_crypto_after_cleanup()
{
    tokio::time::timeout(Duration::from_secs(20), async {
        let server = Server::new().await;
        let ca = server._material.path().join("client-ca.pem");
        std::fs::copy(server._material.path().join("cert.pem"), &ca).unwrap();
        let config = config(&server, Some(&ca));
        let (proxy, running, handle) = running(config.clone()).await;
        let held_tcp = connect(&proxy, "hy").await;
        let held_udp = loan(&proxy, 920).await;
        assert_eq!(server.authenticated.load(Ordering::SeqCst), 1);
        let root = held_udp.owner.original().clone();
        let prior = original(&root);
        let revision = proxy.engine().config_revision();
        let original_admission = admission(&proxy);
        std::fs::write(&ca, b"invalid replacement CA material").unwrap();
        let candidate = config.clone();
        let applying = tokio::spawn(async move {
            handle
                .apply_runtime_config_and_wait(candidate, Duration::from_secs(5))
                .await
        });
        let revoked = original_admission.revoked().await;
        assert_eq!(
            revoked.reason,
            zero_platform_tokio::contract::Reason::StalePlan
        );
        eprintln!(
            "actual prior CA-backed HY2 role admission retired; original users/control retained"
        );
        assert_eq!(server.authenticated.load(Ordering::SeqCst), 1);
        return_views(held_tcp, held_udp, &prior, &applying).await;
        let failure = applying
            .await
            .unwrap()
            .expect_err("a new actual preparation pass must read and reject the changed CA bytes");
        assert!(
            failure.contains("Prepare") && failure.contains("InvalidConfig"),
            "the original material failure classification must remain visible: {failure}"
        );
        assert!(
            !failure.contains("unconfirmed") && !failure.contains("restore failed"),
            "failed-candidate cleanup and last-good recovery must finish before ACK: {failure}"
        );
        closed(&root, &prior);
        eprintln!("actual old HY2 native/pool close receipts match before successor traffic");
        assert_eq!(
            proxy.engine().config_revision(),
            revision,
            "the failed candidate never commits"
        );
        assert_eq!(proxy.engine().config().as_ref(), &config);
        assert_eq!(
            std::fs::read(&ca).unwrap(),
            b"invalid replacement CA material",
            "recovery cannot silently repair the external file"
        );
        // These real certificate-verified new connections can succeed only
        // through the old recipe's prepared crypto, after failed-candidate
        // cleanup. The old native endpoint is already physically closed.
        server.wait_closed(1).await;
        let restored_tcp = connect(&proxy, "hy").await;
        let restored_udp = loan(&proxy, 921).await;
        assert_eq!(server.authenticated.load(Ordering::SeqCst), 2);
        assert_ne!(restored_udp.owner.original().instance(), root.instance());
        drop(restored_tcp);
        let DatagramLoan { io, mut owner } = restored_udp;
        drop(io);
        assert!(owner.close(end()).await.unwrap().is_complete());
        drop(owner);
        running.shutdown().await.unwrap();
        server.close().await;
    })
    .await
    .unwrap();
}
