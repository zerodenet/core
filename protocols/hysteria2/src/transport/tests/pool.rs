use super::super::{
    test_fixtures::{endpoint, profile},
    Hysteria2TransportLeaf,
};
use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    time::timeout,
};
use zero_core::InboundClientResponse;

pub(super) async fn echo(leaf: &Hysteria2TransportLeaf) {
    let sockets = OutboundDatagramSocketFactory::new(Default::default());
    let session = Session::new(
        1,
        zero_core::Address::Domain("example.com".into()),
        80,
        zero_core::Network::Tcp,
        zero_core::ProtocolType::new("hysteria2"),
    );
    let mut stream = leaf.open_tcp_stream(&session, &sockets).await.unwrap();
    stream.write_all(b"ping").await.unwrap();
    let mut bytes = [0; 4];
    stream.read_exact(&mut bytes).await.unwrap();
    assert_eq!(&bytes, b"ping");
}
#[tokio::test]
async fn tcp_pool_single_flight_reuses_recovers_and_retires_on_reload() {
    timeout(Duration::from_secs(15), async {
        let endpoint = endpoint();
        let port = endpoint.local_addr().unwrap().port();
        let accepts = Arc::new(AtomicUsize::new(0));
        let count = accepts.clone();
        let connections = Arc::new(Mutex::new(Vec::new()));
        let observed = connections.clone();
        let server = tokio::spawn(async move {
            let mut clients = tokio::task::JoinSet::new();
            while let Some(incoming) = endpoint.accept().await {
                let connection = incoming.await.unwrap();
                count.fetch_add(1, Ordering::Relaxed);
                observed.lock().unwrap().push(connection.clone());
                clients.spawn(async move {
                    let connection = profile()
                        .accept_authenticated_connection(connection)
                        .await
                        .unwrap();
                    let mut streams = tokio::task::JoinSet::new();
                    while let Ok(Some((_, mut stream))) = connection.accept_next_tcp_stream().await
                    {
                        let response = connection.response_protocol();
                        streams.spawn(async move {
                            response.send_ok(&mut stream).await.unwrap();
                            let mut bytes = [0; 4];
                            stream.read_exact(&mut bytes).await.unwrap();
                            stream.write_all(&bytes).await.unwrap();
                            stream.shutdown().await.unwrap();
                        });
                    }
                });
            }
        });
        let pool = Hysteria2ConnectionPool::default();
        let leaf = Hysteria2TransportLeaf::new("hy", "127.0.0.1", port, "test-password", None)
            .with_insecure(true)
            .with_pool(pool.clone());
        tokio::join!(echo(&leaf), echo(&leaf), echo(&leaf));
        echo(&leaf).await;
        assert_eq!(accepts.load(Ordering::Relaxed), 1);
        let entry = pool.0.lock().unwrap().values().next().unwrap().clone();
        let cached = entry.connection.lock().await.as_ref().unwrap().clone();
        connections.lock().unwrap()[0].close(0u32.into(), b"test disconnect");
        cached.connection().closed().await;
        echo(&leaf).await;
        assert_eq!(accepts.load(Ordering::Relaxed), 2);
        pool.clear();
        echo(&leaf).await;
        assert_eq!(accepts.load(Ordering::Relaxed), 3);
        let changed = leaf.clone().with_settings(crate::settings::Settings {
            upload: 1_000_000,
            ..Default::default()
        });
        echo(&changed).await;
        assert_eq!(accepts.load(Ordering::Relaxed), 4);
        pool.clear();
        server.abort();
    })
    .await
    .unwrap();
}
