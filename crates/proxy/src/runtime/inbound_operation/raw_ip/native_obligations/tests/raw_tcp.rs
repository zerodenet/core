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
    timeout(Duration::from_secs(8), async {
        let proxy = Proxy::new(
            zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap(),
        )
        .unwrap();
        let registry = Arc::new(InboundOwnerRegistry::default());
        let (stop, _) = watch::channel(false);
        let installed = registry
            .install("raw-wire", &proxy.protocols, 2, stop)
            .unwrap();
        let group = installed
            .child_tasks(NonZeroUsize::new(2).unwrap())
            .unwrap();
        let native = NativeObligations::new(2, 0, 0);
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
                    trace_packet("client->server", &packet.packet);
                    server.feed(&packet.packet).await;
                }
            }
        }));
        assert!(workers.spawn({
            let client = client.clone();
            async move {
                while let Some(packet) = from_server.recv().await {
                    trace_packet("server->client", &packet);
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
        // Exact port remains reserved by a real non-listening native socket.
        let refusing = TcpSocket::new_v4().unwrap();
        refusing.bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let refused = refusing.local_addr().unwrap();
        let mut first = client.connect(client_ip, refused).await.unwrap();
        let mut bytes = [0; 64];
        let closed = timeout(Duration::from_secs(3), first.read(&mut bytes))
            .await
            .unwrap();
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
        let reading = timeout(Duration::from_secs(3), next.read_to_end(&mut reverse))
            .await
            .unwrap();
        if let Err(error) = &reading {
            eprintln!("raw next-flow read error={error:?}, reverse={reverse:?}, echo_finished={}, host_cancelled={}", echo.is_finished(), host.cancellation().is_cancelled());
            eprintln!("actual completed records={:#?}", proxy.engine().completed_sessions());
            let state = native.0.lock().unwrap();
            for (id, record) in &state.records {
                eprintln!("native record={id:?} entered={} retiring={} business_failure={:?}", record.entered, record.retiring, record.business_failure);
            }
        }
        reading.unwrap();
        assert_eq!(reverse, b"real reverse after FIN");
        echo.await.unwrap();
        let healthy = completed(&proxy, address.port()).await;
        assert_ne!(healthy.id, failed.id);
        assert_ne!(healthy.id, 0);
        assert_eq!(healthy.inbound_tag.as_deref(), Some("raw-wire"));
        assert_eq!(healthy.target, Address::Ipv4([127, 0, 0, 1]));
        assert!(healthy.failure.is_none());
        assert_eq!(healthy.outcome, zero_engine::SessionOutcome::DirectRelayed);
        assert!(!host.cancellation().is_cancelled());
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
        stop_parent.send(()).unwrap();
        monitor.await.unwrap().unwrap();
        assert!(registry
            .confirm_shutdown("raw-wire", Instant::now() + Duration::from_secs(2))
            .await
            .is_err());
        assert_eq!(native.0.lock().unwrap().records.len(), 2);
        assert!(
            owner.terminal_for_test().is_none(),
            "ordinary flow handling never proves original native closure"
        );
        drop((first, next));
        client.revoke();
        workers.close().await;
    })
    .await
    .unwrap();
}

// Header-only diagnostics: actual packet direction/sequence and FIN/RST; no payload.
fn trace_packet(direction: &str, packet: &[u8]) {
    if packet.len() < 40 || packet[0] >> 4 != 4 || packet[9] != 6 {
        return;
    }
    let start = usize::from(packet[0] & 15) * 4;
    if packet.len() < start + 20 {
        return;
    }
    let tcp = &packet[start..];
    let port = |at| u16::from_be_bytes([tcp[at], tcp[at + 1]]);
    let seq = |at| u32::from_be_bytes([tcp[at], tcp[at + 1], tcp[at + 2], tcp[at + 3]]);
    eprintln!(
        "raw packet {direction} {}->{} flags={:#04x} seq={} ack={} bytes={}",
        port(0),
        port(2),
        tcp[13],
        seq(4),
        seq(8),
        packet.len()
    );
}
