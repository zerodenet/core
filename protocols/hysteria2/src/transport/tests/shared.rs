use super::super::{
    establish_hysteria2_udp_flow_connection, open_hysteria2_udp_packet_path_build,
    test_fixtures::{endpoint, profile},
    Hysteria2TransportLeaf,
};
use super::*;
use crate::udp::Hysteria2UdpChannel;
use std::{
    collections::BTreeSet,
    sync::atomic::{AtomicUsize, Ordering},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    time::timeout,
};
use zero_core::{Address, InboundClientResponse};

struct Server {
    port: u16,
    authenticated: Arc<AtomicUsize>,
    connections: Arc<Mutex<Vec<quinn::Connection>>>,
    sessions: Arc<Mutex<BTreeSet<(usize, u32)>>>,
    task: tokio::task::JoinHandle<()>,
}
impl Server {
    fn start() -> Self {
        let endpoint = endpoint();
        let port = endpoint.local_addr().unwrap().port();
        let authenticated = Arc::new(AtomicUsize::new(0));
        let count = authenticated.clone();
        let connections = Arc::new(Mutex::new(Vec::new()));
        let observed = connections.clone();
        let sessions = Arc::new(Mutex::new(BTreeSet::new()));
        let ids = sessions.clone();
        let task = tokio::spawn(async move {
            let mut clients = tokio::task::JoinSet::new();
            while let Some(incoming) = endpoint.accept().await {
                let observed = observed.clone();
                let count = count.clone();
                let ids = ids.clone();
                clients.spawn(async move {
                    let Ok(raw) = incoming.await else { return };
                    let Ok(connection) = profile().accept_authenticated_connection(raw.clone()).await else { return };
                    let number = count.fetch_add(1, Ordering::Relaxed);
                    observed.lock().unwrap().push(raw.clone());
                    let mut streams = tokio::task::JoinSet::new();
                    tokio::select! {
                        _ = async {
                            while let Ok(data) = raw.read_datagram().await {
                                let packet = crate::udp::parse_udp_datagram(&data).unwrap();
                                ids.lock().unwrap().insert((number, packet.session_id()));
                                if raw.send_datagram(data).is_err() { break; }
                            }
                        } => {},
                        _ = async {
                            while let Ok(Some((_, mut stream))) = connection.accept_next_tcp_stream().await {
                                let response = connection.response_protocol();
                                streams.spawn(async move {
                                    response.send_ok(&mut stream).await.unwrap();
                                    let mut bytes = [0; 4];
                                    stream.read_exact(&mut bytes).await.unwrap();
                                    stream.write_all(&bytes).await.unwrap();
                                    stream.shutdown().await.unwrap();
                                });
                            }
                        } => {},
                    }
                });
            }
        });
        Self {
            port,
            authenticated,
            connections,
            sessions,
            task,
        }
    }
    fn leaf(&self, pool: &Hysteria2ConnectionPool) -> Hysteria2TransportLeaf {
        Hysteria2TransportLeaf::new("hy", "127.0.0.1", self.port, "test-password", None)
            .with_insecure(true)
            .with_pool(pool.clone())
    }
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
fn sockets() -> OutboundDatagramSocketFactory {
    OutboundDatagramSocketFactory::new(Default::default())
}
async fn channel(leaf: &Hysteria2TransportLeaf) -> Hysteria2UdpChannel {
    open_hysteria2_udp_packet_path_build(leaf.packet_path_carrier_build(), &sockets())
        .await
        .unwrap()
}
async fn datagram(channel: &Hysteria2UdpChannel, marker: u8) {
    let payload = vec![marker; 1600];
    let target = Address::Domain(format!("{marker}.example.com"));
    channel.send_to(&target, 53, &payload).await.unwrap();
    let (got_target, port, got) = channel.receive().await.unwrap();
    assert_eq!((got_target, port, got), (target, 53, payload));
}

#[tokio::test]
async fn tcp_managed_udp_and_packet_paths_share_one_authentication_without_cross_delivery() {
    timeout(Duration::from_secs(15), async {
        let server = Server::start();
        let pool = Hysteria2ConnectionPool::default();
        let leaf = server.leaf(&pool);
        let mut jobs = tokio::task::JoinSet::new();
        for marker in 1..=16 {
            let leaf = leaf.clone();
            jobs.spawn(async move {
                let channel = channel(&leaf).await;
                datagram(&channel, marker).await;
                datagram(&channel, marker + 32).await;
            });
        }
        let target = Address::Domain("managed.example.com".into());
        let resume = leaf.flow_resume();
        let flow = establish_hysteria2_udp_flow_connection(
            "127.0.0.1",
            server.port,
            &target,
            53,
            b"initial",
            resume.clone(),
            &sockets(),
        )
        .await
        .unwrap();
        let mut responses = flow.subscribe_responses();
        flow.send(&target, 53, b"managed").await.unwrap();
        loop {
            let (got, port, bytes) = responses.recv().await.unwrap();
            assert_eq!((&got, port), (&target, 53));
            if bytes == b"managed" {
                break;
            }
        }
        tokio::join!(super::tests::echo(&leaf), super::tests::echo(&leaf));
        while let Some(result) = jobs.join_next().await {
            result.unwrap();
        }
        assert_eq!(server.authenticated.load(Ordering::Relaxed), 1);
        assert_eq!(server.sessions.lock().unwrap().len(), 17);
        drop(flow);
        // One dropped UDP session must not close sibling users of the connection.
        datagram(&channel(&leaf).await, 99).await;
        assert_eq!(server.authenticated.load(Ordering::Relaxed), 1);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn disconnect_rebuilds_once_and_reload_retires_without_closing_active_udp() {
    timeout(Duration::from_secs(15), async {
        let server = Server::start();
        let pool = Hysteria2ConnectionPool::default();
        let leaf = server.leaf(&pool);
        let old = channel(&leaf).await;
        datagram(&old, 1).await;
        let cached = pool.0.lock().unwrap().values().next().unwrap().clone();
        assert!(!cached.is_idle());
        server.connections.lock().unwrap()[0].close(0u32.into(), b"disconnect");
        assert!(old.receive().await.is_err());
        let (a, b, ()) = tokio::join!(channel(&leaf), channel(&leaf), super::tests::echo(&leaf));
        datagram(&a, 2).await;
        datagram(&b, 3).await;
        assert_eq!(server.authenticated.load(Ordering::Relaxed), 2);
        pool.clear();
        let fresh = channel(&leaf).await;
        datagram(&fresh, 4).await;
        datagram(&a, 5).await;
        assert_eq!(server.authenticated.load(Ordering::Relaxed), 3);
        drop(a);
        drop(b);
        drop(fresh);
        drop(old);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn shared_connection_identity_preserves_tls_credentials_settings_and_outbound_isolation() {
    timeout(Duration::from_secs(15), async {
        let server = Server::start();
        let pool = Hysteria2ConnectionPool::default();
        let leaf = server.leaf(&pool);
        let first = channel(&leaf).await;
        datagram(&first, 1).await;
        let mut wrong = leaf.clone();
        wrong.password = "wrong-password".into();
        assert!(open_hysteria2_udp_packet_path_build(
            wrong.packet_path_carrier_build(),
            &sockets()
        )
        .await
        .is_err());
        for variant in [
            leaf.clone().with_server_name(Some("localhost")),
            leaf.clone().with_settings(crate::settings::Settings {
                upload: 1_000_000,
                ..Default::default()
            }),
        ] {
            datagram(&channel(&variant).await, 2).await;
        }
        let mut different_tag = leaf.clone();
        different_tag.tag = "other".into();
        datagram(&channel(&different_tag).await, 3).await;
        assert_eq!(server.authenticated.load(Ordering::Relaxed), 4);
        let mut strict = leaf.clone();
        strict.insecure = false;
        assert!(open_hysteria2_udp_packet_path_build(
            strict.packet_path_carrier_build(),
            &sockets()
        )
        .await
        .is_err());
        datagram(&first, 4).await;
        assert_eq!(server.authenticated.load(Ordering::Relaxed), 4);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn active_udp_survives_pool_idle_sweep_and_unused_connection_is_reclaimed() {
    timeout(Duration::from_secs(15), async {
        let server = Server::start();
        let pool = Hysteria2ConnectionPool::default();
        let mut settings = crate::settings::Settings::default();
        settings.quic.max_idle_timeout_secs = 4;
        settings.quic.keep_alive_interval_secs = 2;
        let leaf = server.leaf(&pool).with_settings(settings);
        let active = channel(&leaf).await;
        datagram(&active, 1).await;
        tokio::time::sleep(Duration::from_secs(5)).await;
        datagram(&channel(&leaf).await, 2).await;
        assert_eq!(server.authenticated.load(Ordering::Relaxed), 1);
        drop(active);
        while !pool.0.lock().unwrap().is_empty() {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn egress_generation_change_creates_a_new_connection_and_preserves_old_udp() {
    timeout(Duration::from_secs(10), async {
        let server = Server::start();
        let pool = Hysteria2ConnectionPool::default();
        let leaf = server.leaf(&pool);
        let egress = Default::default();
        let sockets = OutboundDatagramSocketFactory::new(Clone::clone(&egress));
        let old = open_hysteria2_udp_packet_path_build(leaf.packet_path_carrier_build(), &sockets)
            .await
            .unwrap();
        datagram(&old, 1).await;
        egress.invalidate_network();
        let new = open_hysteria2_udp_packet_path_build(leaf.packet_path_carrier_build(), &sockets)
            .await
            .unwrap();
        datagram(&new, 2).await;
        datagram(&old, 3).await;
        assert_eq!(server.authenticated.load(Ordering::Relaxed), 2);
    })
    .await
    .unwrap();
}

#[path = "lifetime.rs"]
mod lifetime;
