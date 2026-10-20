use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tokio::net::TcpListener;
use tokio::sync::Notify;

use crate::backends::doh::DohDnsResolver;
use zero_platform_tokio::EgressInterfaceControl;

mod binding;
mod owner;

#[derive(Clone, Copy)]
enum FirstConnection {
    Healthy,
    Stalled,
    Rejected,
}

struct Server {
    addr: std::net::SocketAddr,
    accepted: Arc<AtomicUsize>,
    first_request: Arc<Notify>,
    task: tokio::task::JoinHandle<()>,
}

impl Server {
    async fn start(mode: FirstConnection) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let accepted = Arc::new(AtomicUsize::new(0));
        let first_request = Arc::new(Notify::new());
        let count = accepted.clone();
        let seen = first_request.clone();
        let task = tokio::spawn(async move {
            let mut tasks = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    next = listener.accept() => {
                        let (stream, _) = next.unwrap();
                        let index = count.fetch_add(1, Ordering::SeqCst);
                        let seen = seen.clone();
                        tasks.spawn(async move {
                            let Ok(mut connection) = h2::server::handshake(stream).await else { return };
                            while let Some(Ok((request, mut respond))) = connection.accept().await {
                                let mut body = request.into_body();
                                let mut query = Vec::new();
                                while let Some(chunk) = body.data().await {
                                    let Ok(chunk) = chunk else { return };
                                    query.extend_from_slice(&chunk);
                                    body.flow_control().release_capacity(chunk.len()).unwrap();
                                }
                                seen.notify_one();
                                if matches!(mode, FirstConnection::Stalled) && index == 0 {
                                    // Keep this TCP/H2 session open but stop driving replies.
                                    std::future::pending::<()>().await;
                                }
                                let response = crate::message::build_address_response(
                                    &query, &[zero_traits::IpAddress::V4([203, 0, 113, 53])], 30,
                                );
                                let status = if index == 0 && matches!(mode, FirstConnection::Rejected) {
                                    503
                                } else { 200 };
                                let headers = http::Response::builder().status(status).body(()).unwrap();
                                let mut send = respond.send_response(headers, false).unwrap();
                                send.send_data(Bytes::from(response), true).unwrap();
                            }
                        });
                    }
                    _ = tasks.join_next(), if !tasks.is_empty() => {}
                }
            }
        });
        Self {
            addr,
            accepted,
            first_request,
            task,
        }
    }

    fn resolver(&self, egress: EgressInterfaceControl) -> Arc<DohDnsResolver> {
        Arc::new(
            DohDnsResolver::new_cleartext(
                "127.0.0.1".into(),
                self.addr.port(),
                "/dns-query".into(),
                vec![],
                egress,
            )
            .unwrap(),
        )
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn exchange(resolver: &DohDnsResolver) -> std::io::Result<Vec<u8>> {
    let query = crate::message::build_query("pool.example", crate::message::TYPE_A).unwrap();
    tokio::time::timeout(
        Duration::from_secs(2),
        resolver.exchange(&query, None, None),
    )
    .await
    .expect("bounded local DoH exchange")
}

#[tokio::test]
async fn network_change_with_same_interface_replaces_doh_connection() {
    let server = Server::start(FirstConnection::Healthy).await;
    let egress = EgressInterfaceControl::default();
    let resolver = server.resolver(egress.clone());
    exchange(&resolver).await.unwrap();
    exchange(&resolver).await.unwrap();
    assert_eq!(
        server.accepted.load(Ordering::SeqCst),
        1,
        "healthy connection is reused"
    );
    egress.invalidate_network();
    exchange(&resolver).await.unwrap();
    assert_eq!(
        server.accepted.load(Ordering::SeqCst),
        2,
        "network changes must retire connections even if the interface name is unchanged"
    );
}

#[tokio::test]
async fn outer_deadline_cancellation_retires_stalled_doh_connection() {
    let server = Server::start(FirstConnection::Stalled).await;
    let resolver = server.resolver(EgressInterfaceControl::default());
    let first = {
        let resolver = resolver.clone();
        tokio::spawn(async move { exchange(&resolver).await })
    };
    tokio::time::timeout(Duration::from_secs(2), server.first_request.notified())
        .await
        .unwrap();
    // A backend deadline or caller cancellation drops the entire exchange future.
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    exchange(&resolver)
        .await
        .expect("next query opens a fresh H2 connection");
    assert_eq!(server.accepted.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn retiring_old_users_before_replacement_preserves_the_new_generation_connection() {
    let server = Server::start(FirstConnection::Stalled).await;
    let egress = EgressInterfaceControl::default();
    let resolver = server.resolver(egress.clone());
    let first = {
        let resolver = resolver.clone();
        tokio::spawn(async move { exchange(&resolver).await })
    };
    tokio::time::timeout(Duration::from_secs(2), server.first_request.notified())
        .await
        .unwrap();
    egress.invalidate_network();
    exchange(&resolver).await.unwrap();
    let old = tokio::time::timeout(Duration::from_secs(2), first)
        .await
        .unwrap()
        .unwrap();
    let old = old.expect_err("affected old query must exit before its pool slot is replaced");
    let mut source: &(dyn std::error::Error + 'static) = &old;
    loop {
        if source.downcast_ref::<h2::Error>().is_some() {
            break;
        }
        source = source
            .source()
            .expect("pool cancellation must retain the actual H2 failure source");
    }

    exchange(&resolver).await.unwrap();
    assert_eq!(
        server.accepted.load(Ordering::SeqCst),
        2,
        "retiring an old slot must not evict the replacement connection"
    );
}

#[tokio::test]
async fn failed_http_exchange_is_replaced_without_changing_dns_policy() {
    let server = Server::start(FirstConnection::Rejected).await;
    let resolver = server.resolver(EgressInterfaceControl::default());
    let error = exchange(&resolver)
        .await
        .expect_err("first HTTP session returns 503");
    assert!(error.to_string().contains("503"));
    exchange(&resolver).await.unwrap();
    assert_eq!(server.accepted.load(Ordering::SeqCst), 2);
}

#[derive(Debug)]
struct PausedDetour {
    entered: Arc<Notify>,
    release: Arc<Notify>,
}

impl crate::DnsOutboundConnector for PausedDetour {
    fn connect(
        &self,
        _outbound: String,
        endpoint: std::net::SocketAddr,
    ) -> crate::DnsOutboundConnectFuture {
        let entered = self.entered.clone();
        let release = self.release.clone();
        Box::pin(async move {
            let stream = tokio::net::TcpStream::connect(endpoint).await?;
            entered.notify_one();
            release.notified().await;
            Ok(zero_platform_tokio::TcpRelayStream::new(stream))
        })
    }
}

#[tokio::test]
async fn connection_prepared_before_network_change_is_not_published_or_queried() {
    let server = Server::start(FirstConnection::Healthy).await;
    let egress = EgressInterfaceControl::default();
    let resolver = server.resolver(egress.clone());
    let detour = Arc::new(PausedDetour {
        entered: Arc::new(Notify::new()),
        release: Arc::new(Notify::new()),
    });
    let pending = {
        let resolver = resolver.clone();
        let detour = detour.clone();
        tokio::spawn(async move {
            let query =
                crate::message::build_query("pool.example", crate::message::TYPE_A).unwrap();
            resolver
                .exchange(&query, Some("selected-outbound"), Some(detour.as_ref()))
                .await
        })
    };
    tokio::time::timeout(Duration::from_secs(2), detour.entered.notified())
        .await
        .unwrap();
    egress.invalidate_network();
    detour.release.notify_one();
    let error = tokio::time::timeout(Duration::from_secs(2), pending)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::Interrupted);
    assert!(
        resolver.clients.lock().await.is_empty(),
        "stale prepared connection must not enter the pool"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(20), server.first_request.notified())
            .await
            .is_err(),
        "a DNS request must not be sent through the stale prepared connection"
    );
}

#[derive(Debug)]
struct LoopbackDetour;

impl crate::DnsOutboundConnector for LoopbackDetour {
    fn connect(
        &self,
        _outbound: String,
        endpoint: std::net::SocketAddr,
    ) -> crate::DnsOutboundConnectFuture {
        Box::pin(async move {
            tokio::net::TcpStream::connect(endpoint)
                .await
                .map(zero_platform_tokio::TcpRelayStream::new)
        })
    }
}

#[tokio::test]
async fn detoured_doh_reconnects_after_egress_change_without_rewriting_the_detour() {
    let server = Server::start(FirstConnection::Healthy).await;
    let egress = EgressInterfaceControl::default();
    let resolver = server.resolver(egress.clone());
    let query = crate::message::build_query("pool.example", crate::message::TYPE_A).unwrap();
    for round in 0..2 {
        resolver
            .exchange(&query, Some("selected-outbound"), Some(&LoopbackDetour))
            .await
            .unwrap();
        if round == 0 {
            egress.invalidate_network();
        }
    }
    assert_eq!(server.accepted.load(Ordering::SeqCst), 2);
}
