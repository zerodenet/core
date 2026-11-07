//! Ordinary Proxy owns the CA File, cached H2 original and logical borrowers.
use crate::{Proxy, ProxyHandle};
use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zero_config::RuntimeConfig;
use zero_core::{
    component::NetworkGeneration,
    request::{Destination, ServiceOrigin, ServicePurpose, ServiceRequestFacts},
    Address,
};
use zero_platform_tokio::{
    contract::{
        context::{OpenContext, RequestIdentity},
        network::{CarrierContext, UpstreamBinding, UpstreamOpener, UpstreamTransport},
        stream::{BoxStream, ConnectRequest},
        BoxFuture, CancellationToken, ChannelError,
    },
    controlled_carrier::loan::StreamLoan,
};
mod peer;
use peer::{Directory, Peer};
fn deadline() -> tokio::time::Instant {
    tokio::time::Instant::now() + Duration::from_secs(5)
}
struct NoBare;
impl UpstreamOpener for NoBare {
    fn open_stream(
        &self,
        _: &UpstreamBinding,
        _: Destination,
        _: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxStream, ChannelError>> {
        Box::pin(async { panic!("ordinary gRPC must use its actual installed native provider") })
    }
    fn open_datagram(
        &self,
        _: &UpstreamBinding,
        _: zero_platform_tokio::contract::datagram::DatagramRequest,
        _: CarrierContext,
    ) -> BoxFuture<
        '_,
        Result<zero_platform_tokio::contract::datagram::BoxDatagramSession, ChannelError>,
    > {
        Box::pin(async { panic!("ordinary gRPC must not switch to bare Datagram IO") })
    }
}
async fn loan(proxy: &Proxy) -> StreamLoan {
    let services = proxy.tcp_runtime_services();
    let slot = services
        .components
        .as_ref()
        .unwrap()
        .configured("grpc")
        .expect("actual ordinary gRPC role");
    slot.stream(deadline())
        .await
        .unwrap()
        .connect_carrier(
            ConnectRequest {
                destination: Destination::new(Address::Domain("real.example".into()), 443),
            },
            OpenContext {
                identity: RequestIdentity::Service(
                    Arc::new(ServiceRequestFacts {
                        origin: ServiceOrigin::Runtime,
                        purpose: ServicePurpose::BackgroundControl,
                    })
                    .into(),
                ),
                deadline: Some(deadline()),
                cancel: CancellationToken::new(),
                observer: None,
                upstream: UpstreamTransport::new(
                    UpstreamBinding {
                        prefix: vec![],
                        dial: slot.dial.clone(),
                        network_generation: NetworkGeneration(
                            services.upstream().network_generation(),
                        ),
                    },
                    Arc::new(NoBare),
                ),
            },
        )
        .await
        .unwrap()
        .io
}
async fn echo(loan: &mut StreamLoan) {
    loan.io.write_all(b"tls").await.unwrap();
    loan.io.flush().await.unwrap();
    let mut bytes = [0; 3];
    loan.io.read_exact(&mut bytes).await.unwrap();
    assert_eq!(bytes, *b"tls");
}
async fn returned(loan: StreamLoan) {
    let StreamLoan { io, mut owner } = loan;
    drop(io);
    assert!(owner.close(deadline()).await.unwrap().is_complete());
    assert!(owner.closed().unwrap().matches(&owner));
}
fn config(peer: &Peer, path: &std::path::Path, backend: &str, multi: bool) -> RuntimeConfig {
    RuntimeConfig::parse(&serde_json::json!({"runtime":{"file_read_max_bytes":4096,"grpc_pool_max_carriers":1,"grpc_pool_max_streams":2,"grpc_total_max_carriers":1,"grpc_total_max_streams":2},"outbounds":[{"tag":"grpc","protocol":{"type":"vless","server":"127.0.0.1","port":peer.address.port(),"id":"11111111-2222-3333-4444-555555555555","tls":{"server_name":"localhost","ca_cert_path":path,"options":{"backend":backend,"disable_system_roots":true}},"grpc":{"service_names":["actual.ca/Tun"],"multi_mode":multi}}}],"route":{"final":{"type":"direct"}}}).to_string()).unwrap()
}
#[tokio::test]
async fn ordinary_each_tls_backend_uses_the_actual_file_snapshot_across_cached_logical_borrowers() {
    tokio::time::timeout(Duration::from_secs(30), async {
        for backend in ["rustls", "openssl"] {
            for multi in [false, true] {
                let directory = Directory::new();
                let path = directory.0.join("ca.pem");
                let peer = Peer::new().await;
                let config = config(&peer, &path, backend, multi);
                let proxy = Proxy::new(config.clone()).unwrap();
                assert!(
                    !path.exists(),
                    "cold descriptors cannot read/create the CA File"
                );
                std::fs::write(&path, &peer.pem).unwrap();
                let running = proxy.spawn();
                let handle = ProxyHandle::new(
                    zero_engine::EngineHandle::new(proxy.engine().clone()),
                    proxy.clone(),
                );
                handle
                    .apply_runtime_config_and_wait(config.clone(), Duration::from_secs(5))
                    .await
                    .unwrap();
                assert_eq!(
                    peer.accepts.load(Ordering::SeqCst),
                    0,
                    "Ready allocates no carrier"
                );
                std::fs::remove_file(&path).unwrap();
                let mut first = loan(&proxy).await;
                echo(&mut first).await;
                let original = first.owner.original().clone();
                let services=proxy.tcp_runtime_services();let plan=services.components.as_ref().unwrap();
                let pool=plan.component(original.instance().component).unwrap();
                assert!(matches!(&pool.recipe.spec().resources[..],[zero_platform_tokio::contract::component::ResourceDeclaration::File{path:actual,max_bytes},zero_platform_tokio::contract::component::ResourceDeclaration::ControlledCarrierPool{..}] if actual==&path && *max_bytes==4096));
                let native=plan.original_carrier_owner(original.instance(),1).unwrap();assert!(original.owner().same_owner(&native));assert!(plan.original_carrier_owner(original.instance(),0).is_none());
                let driver = original.driver().unwrap().clone();
                assert_eq!(original.owner().limit().get(), 1);
                assert!(original.closed().is_none());
                assert!(!driver.is_joined());
                let mut second = loan(&proxy).await;
                echo(&mut second).await;
                assert!(second.owner.original().same_original(&original));
                assert_eq!(peer.accepts.load(Ordering::SeqCst), 1);
                returned(first).await;
                echo(&mut second).await;
                returned(second).await;
                running.shutdown().await.unwrap();
                let joined = driver.wait_registration_released().await;
                assert!(driver.matches_joined(&joined) && joined.registration_released());
                assert!(original.closed().unwrap().matches(&original));
                assert!(original.owner().closed().unwrap().matches(original.owner()));
                peer.close().await;
            }
        }
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn bad_ca_new_recipe_fails_before_new_carrier_and_the_exact_old_prepared_recipe_recovers() {
    tokio::time::timeout(Duration::from_secs(25), async {
        for backend in ["rustls", "openssl"] {
            let directory = Directory::new();
            let path = directory.0.join("ca.pem");
            let peer = Peer::new().await;
            std::fs::write(&path, &peer.pem).unwrap();
            let config = config(&peer, &path, backend, true);
            let proxy = Proxy::new(config.clone()).unwrap();
            let running = proxy.spawn();
            let handle = ProxyHandle::new(
                zero_engine::EngineHandle::new(proxy.engine().clone()),
                proxy.clone(),
            );
            handle
                .apply_runtime_config_and_wait(config.clone(), Duration::from_secs(5))
                .await
                .unwrap();
            let mut first = loan(&proxy).await;
            echo(&mut first).await;
            let prior = first.owner.original().clone();
            returned(first).await;
            std::fs::write(&path, b"not a CA certificate").unwrap();
            let mut candidate = config.clone();
            candidate.runtime.diagnostic_max_operations = Some(7);
            let revision = proxy.config_revision();
            assert!(handle
                .apply_runtime_config_and_wait(candidate, Duration::from_secs(5))
                .await
                .is_err());
            assert_eq!(proxy.config_revision(), revision);
            assert_eq!(proxy.engine().runtime_snapshot().config().as_ref(), &config);
            assert_eq!(
                peer.accepts.load(Ordering::SeqCst),
                1,
                "invalid File material must fail before a replacement native connect"
            );
            assert!(prior.closed().unwrap().matches(&prior));
            let driver = prior.driver().unwrap();
            let joined = driver.wait_registration_released().await;
            assert!(driver.matches_joined(&joined) && joined.registration_released());
            let mut fresh = loan(&proxy).await;
            echo(&mut fresh).await;
            let current = fresh.owner.original().clone();
            assert!(!current.same_original(&prior));
            assert_eq!(
                current.instance().component,
                prior.instance().component,
                "recovery retains the exact original owning pool component"
            );
            assert_ne!(current.instance(), prior.instance());
            returned(fresh).await;
            running.shutdown().await.unwrap();
            assert!(current.closed().unwrap().matches(&current));
            peer.close().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn ordinary_new_candidate_with_the_same_ca_path_and_profile_reads_the_rotated_actual_file() {
    tokio::time::timeout(Duration::from_secs(25), async {
        for backend in ["rustls", "openssl"] {
            let directory = Directory::new();
            let path = directory.0.join("ca.pem");
            let peer = Peer::new().await;
            std::fs::write(&path, &peer.pem).unwrap();
            let config = config(&peer, &path, backend, true);
            let proxy = Proxy::new(config.clone()).unwrap();
            let running = proxy.spawn();
            let handle = ProxyHandle::new(
                zero_engine::EngineHandle::new(proxy.engine().clone()),
                proxy.clone(),
            );
            handle
                .apply_runtime_config_and_wait(config.clone(), Duration::from_secs(5))
                .await
                .unwrap();
            let mut first = loan(&proxy).await;
            echo(&mut first).await;
            let prior = first.owner.original().clone();
            std::fs::write(&path, &peer.replacement_pem).unwrap();
            peer.rotate_authority();
            echo(&mut first).await;
            assert_eq!(peer.accepts.load(Ordering::SeqCst), 1);
            assert!(
                prior.closed().is_none(),
                "disk/new peer material cannot close the already active original TLS"
            );
            returned(first).await;
            let mut candidate = config;
            candidate.runtime.diagnostic_max_operations = Some(7);
            handle
                .apply_runtime_config_and_wait(candidate, Duration::from_secs(5))
                .await
                .unwrap();
            let driver = prior.driver().unwrap();
            let joined = driver.wait_registration_released().await;
            assert!(driver.matches_joined(&joined) && joined.registration_released());
            assert!(prior.closed().unwrap().matches(&prior));
            let mut fresh = loan(&proxy).await;
            echo(&mut fresh).await;
            let current = fresh.owner.original().clone();
            assert_eq!(current.instance().component, prior.instance().component);
            assert_ne!(current.instance(), prior.instance());
            assert_eq!(peer.accepts.load(Ordering::SeqCst), 2);
            returned(fresh).await;
            running.shutdown().await.unwrap();
            assert!(current.closed().unwrap().matches(&current));
            assert!(current.owner().closed().unwrap().matches(current.owner()));
            peer.close().await;
        }
    })
    .await
    .unwrap();
}

mod network;

mod ech;

#[cfg(all(feature = "socks5", feature = "raw-ip-runtime"))]
mod business;
