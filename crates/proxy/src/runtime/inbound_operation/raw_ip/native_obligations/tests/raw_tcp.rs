//! The real raw TCP accept -> ordinary serve -> selected Direct wire pipeline.
use super::*;
use crate::runtime::{
    inbound_owners::InboundOwnerRegistry,
    route_runtime::{InboundRouteRuntimeFactory, SharedIngressRuntimeServices},
    Proxy,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpSocket},
    sync::mpsc,
};
use zero_core::Address;
use zero_stack::{ClientTcpStack, UserNetworkStack};
use zero_traits::TcpStack;
mod fixture;
use fixture::Workers;

async fn completed(proxy: &Proxy, port: u16) -> zero_engine::CompletedSessionRecord {
    timeout(Duration::from_secs(3), async {
        loop {
            if let Some(record) = proxy
                .engine()
                .completed_sessions()
                .into_iter()
                .find(|record| record.port == port)
            {
                return record;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn actual_raw_tcp_accept_serve_refusal_does_not_stop_the_next_admitted_wire_session() {
    if std::env::var_os("ZERO_TEST_LOG").is_some() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("zero_proxy=trace,zero_stack=trace")
            .try_init();
    }
    timeout(Duration::from_secs(8), async {
        let proxy = Proxy::new(
            zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap(),
        )
        .unwrap();
        proxy.protocols.drive_component_supervision(async {
        // Match real startup: cold Proxy construction is not current host
        // admission for either the refused flow or its healthy successor.
        let config = proxy.engine().runtime_snapshot().config().clone();
        proxy
            .protocols
            .start_components(config.clone(), Instant::now() + Duration::from_secs(3))
            .await
            .unwrap();
        proxy
            .protocols
            .commit_components(&config, || Ok(()))
            .unwrap();
        let registry = Arc::new(InboundOwnerRegistry::default());
        let (stop, _) = watch::channel(false);
        let installed = registry
            .install("raw-wire", &proxy.protocols, 2, stop)
            .unwrap();
        let group = installed
            .child_tasks(NonZeroUsize::new(2).unwrap())
            .unwrap();
        let native = NativeObligations::new(1, 0, 0);
        installed.register(native.resource()).unwrap();
        let owner = installed.owner();
        let host = installed.tasks();
        let factory = InboundRouteRuntimeFactory::new(
            SharedIngressRuntimeServices::new(proxy.tcp_runtime_services()),
            "raw-wire".into(),
        );
        // Two real flow workers on each stack, two control workers, two packet
        // pumps: this explicit fixture limit is not a production queue default.
        let workers = Arc::new(Workers::new(2 * 2 + 2 + 2));
        let (client_packets, mut from_client) = mpsc::channel(64);
        let (server_packets, mut from_server) = mpsc::channel::<Vec<u8>>(64);
        let (server, _udp) = UserNetworkStack::new_with_packet_output_and_task_owner(
            server_packets.into(),
            1420,
            workers.clone(),
        )
        .into_parts();
        let client_ip = "10.77.0.2".parse().unwrap();
        let client = Arc::new(
            ClientTcpStack::new_observed_with_task_owner(
                vec![client_ip],
                client_packets,
                1480,
                workers.clone(),
            )
            .unwrap(),
        );
        assert!(workers.spawn({
            let server = server.clone();
            async move {
                while let Some(packet) = from_client.recv().await {
                    server.feed(&packet.packet).await;
                }
            }
        }));
        assert!(workers.spawn({
            let client = client.clone();
            async move {
                while let Some(packet) = from_server.recv().await {
                    client.feed(&packet).await;
                }
            }
        }));
        let (stop_parent, parent_stopped) = oneshot::channel();
        let for_accept = native.clone();
        let accept_host = host.clone();
        owner
            .spawn_driver(async move {
                tokio::select! {
                    result = crate::runtime::inbound_operation::raw_ip::tcp::accept(
                        server, factory, group, accept_host, for_accept,
                    ) => result,
                    _ = parent_stopped => Ok(()),
                }
            })
            .unwrap();
        let monitor = {
            let owner = owner.clone();
            tokio::spawn(async move { owner.observe_driver().await })
        };
        // macOS drops SYN on a bound non-listening socket. Release this
        // endpoint so this fixture observes a real native refusal instead.
        let refusing = TcpSocket::new_v4().unwrap();
        refusing.bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let refused = refusing.local_addr().unwrap();
        drop(refusing);
        let mut first = client.connect(client_ip, refused).await.unwrap();
        let mut bytes = [0; 64];
        let closed = timeout(Duration::from_secs(3), first.read(&mut bytes))
            .await
            .unwrap_or_else(|error| {
                eprintln!("raw TCP active={:?} completed={:?} monitor_done={}", proxy.engine().active_sessions(), proxy.engine().completed_sessions(), monitor.is_finished());
                let state = native.0.lock().unwrap();
                for (id, record) in &state.records {
                    eprintln!("raw TCP record={id} entered={} retiring={} gateways={} uncovered={} caller_joined={} business={:?}", record.entered, record.retiring, record.gateways, record.uncovered_gateway, record.caller.as_ref().is_some_and(|caller| caller.observed_join().is_some()), record.business_failure);
                }
                panic!("raw TCP refusal response: {error}");
            });
        assert!(
            !matches!(closed, Ok(size) if size != 0),
            "failed establishment cannot yield successful response bytes"
        );
        let failed = completed(&proxy, refused.port()).await;
        assert_ne!(
            failed.id, 0,
            "the real Engine minted the first business identity"
        );
        assert_eq!(failed.inbound_tag.as_deref(), Some("raw-wire"));
        assert_eq!(failed.target, Address::Ipv4([127, 0, 0, 1]));
        assert!(failed.failure.is_some());
        let (registration, source) = timeout(Duration::from_secs(2), async {
            loop {
                let received = {
                    let state = native.0.lock().unwrap();
                    state.records.iter().find_map(|(id, record)| {
                        if !record.entered || !record.retiring {
                            return None;
                        }
                        Some((
                            NativeRegistration {
                                owner: Arc::downgrade(&native.0),
                                id: *id,
                            },
                            record.business_failure.clone()?,
                        ))
                    })
                };
                if let Some(received) = received {
                    break received;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        if refused_source(source.as_ref()).is_none() {
            eprintln!("raw actual original business failure={source:#?}");
            eprintln!(
                "raw actual completed records={:#?}",
                proxy.engine().completed_sessions()
            );
            trace_source(source.as_ref());
        }
        let refused_os = refused_source(source.as_ref())
            .expect("actual upstream connect was refused, not host admission");
        assert!(
            refused_os.raw_os_error().is_some(),
            "retain the real native OS source"
        );
        let joined = registration
            .wait_caller_joined(Instant::now() + Duration::from_secs(2))
            .await
            .unwrap();
        let original_exit = registration.caller_witness().unwrap().try_joined().unwrap();
        assert_eq!(joined.task(), original_exit.id());
        assert_eq!(joined.class(), original_exit.class());
        assert!(original_exit.result().is_ok());
        assert!(joined.failure().is_none());
        assert!(!host.cancellation().is_cancelled());
        assert!(native.0.lock().unwrap().first_source.is_none());

        assert!(Arc::ptr_eq(
            &source,
            native
                .0
                .lock()
                .unwrap()
                .records
                .get(&registration.id)
                .unwrap()
                .business_failure
                .as_ref()
                .unwrap()
        ));

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let echo = tokio::spawn(async move {
            let (mut io, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            io.read_to_end(&mut request).await.unwrap();
            assert_eq!(request, b"next raw flow");
            io.write_all(b"real reverse after FIN").await.unwrap();
            io.shutdown().await.unwrap();
        });
        let mut next = client.connect(client_ip, address).await.unwrap();
        next.write_all(b"next raw flow").await.unwrap();
        next.shutdown().await.unwrap();
        let mut reverse = Vec::new();
        timeout(Duration::from_secs(3), next.read_to_end(&mut reverse))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(reverse, b"real reverse after FIN");
        echo.await.unwrap();
        let healthy = completed(&proxy, address.port()).await;
        assert_ne!(healthy.id, failed.id);
        assert_ne!(healthy.id, 0);
        assert_eq!(healthy.inbound_tag.as_deref(), Some("raw-wire"));
        assert_eq!(healthy.target, Address::Ipv4([127, 0, 0, 1]));
        assert!(healthy.failure.is_none());
        assert!(
            !native.0.lock().unwrap().records.contains_key(&registration.id),
            "the same one-slot quota was reclaimed from the original native/caller receipts"
        );
        assert_eq!(healthy.outcome, zero_engine::SessionOutcome::DirectRelayed);
        assert!(!host.cancellation().is_cancelled());
        assert!(
            !monitor.is_finished(),
            "ordinary flow failures kept the actual listener alive"
        );
        stop_parent.send(()).unwrap();
        monitor.await.unwrap().unwrap();
        registry
            .confirm_shutdown("raw-wire", Instant::now() + Duration::from_secs(2))
            .await
            .unwrap();
        assert!(native.0.lock().unwrap().records.is_empty());
        assert!(
            owner.terminal_for_test().is_some(),
            "original native receipts and real caller joins complete the host"
        );
        drop((first, next));
        client.revoke();
        workers.close().await;
        proxy
            .protocols
            .shutdown_components(Instant::now() + Duration::from_secs(2))
            .await
            .unwrap();
        }).await;
    })
    .await
    .unwrap();
}

// Preserve both boxed std::io payloads and typed Error::source links. A host
// StalePlan rejection must never masquerade as this real native refusal.
fn refused_source<'a>(error: &'a (dyn std::error::Error + 'static)) -> Option<&'a std::io::Error> {
    let mut current = Some(error);
    for _ in 0..64 {
        let error = current?;
        if let Some(io) = error.downcast_ref::<std::io::Error>() {
            if io.kind() == std::io::ErrorKind::ConnectionRefused {
                return Some(io);
            }
            if let Some(inner) = io.get_ref() {
                current = Some(inner);
                continue;
            }
        }
        current = error.source();
    }
    None
}

// Actual retained typed payloads only; never derive a retry/close decision here.
fn trace_source(error: &(dyn std::error::Error + 'static)) {
    let mut current = Some(error);
    for index in 0..64 {
        let Some(error) = current else { return };
        eprintln!("raw source[{index}] display={error}, debug={error:?}");
        if let Some(io) = error.downcast_ref::<std::io::Error>() {
            eprintln!(
                "raw source[{index}] io_kind={:?}, raw_os_error={:?}",
                io.kind(),
                io.raw_os_error()
            );
            if let Some(inner) = io.get_ref() {
                current = Some(inner);
                continue;
            }
        }
        if let Some(io) = error.downcast_ref::<Arc<std::io::Error>>() {
            current = Some(io.as_ref());
            continue;
        }
        if let Some(channel) = error.downcast_ref::<Arc<ChannelError>>() {
            current = Some(channel.as_ref());
            continue;
        }
        current = error.source();
    }
    eprintln!("raw source chain exceeded bounded diagnostic traversal");
}
