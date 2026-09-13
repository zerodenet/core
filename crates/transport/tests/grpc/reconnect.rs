use super::*;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};

#[tokio::test]
async fn concurrent_routes_reconnect_once_after_carrier_eof() {
    timeout(Duration::from_secs(10), async {
        let count = Arc::new(AtomicUsize::new(0));
        let incoming = Arc::new(Mutex::new(Vec::<Arc<GrpcIncoming>>::new()));
        let profile = OwnedGrpcProfile {
            service_names: vec!["service".into()],
            multi_mode: true,
            ..Default::default()
        };
        let pool = GrpcPool::default();
        let open = || {
            count.fetch_add(1, Ordering::Relaxed);
            let (client, server) = tokio::io::duplex(8192);
            let connection = Arc::new(accept_grpc_connection(server, &profile).unwrap());
            incoming.lock().unwrap().push(connection.clone());
            tokio::spawn(async move {
                let mut routes = tokio::task::JoinSet::new();
                while let Some(mut stream) = connection.accept().await {
                    routes.spawn(async move {
                        let mut bytes = [0; 257];
                        while let Ok(length) = AsyncReadExt::read(&mut stream, &mut bytes).await {
                            if length == 0
                                || AsyncWriteExt::write_all(&mut stream, &bytes[..length])
                                    .await
                                    .is_err()
                                || AsyncWriteExt::flush(&mut stream).await.is_err()
                            {
                                break;
                            }
                        }
                    });
                }
                routes.abort_all();
                while routes.join_next().await.is_some() {}
            });
            async { Ok::<_, RuntimeError>(client) }
        };
        let mut first = pool.open(&profile, "localhost", open).await.unwrap();
        exchange(&mut first, b"before disconnect").await;
        incoming.lock().unwrap()[0].close();
        let mut byte = [0];
        assert!(
            !matches!(AsyncReadExt::read(&mut first, &mut byte).await, Ok(length) if length > 0)
        );

        // Every caller observes the same dead carrier. Only one replacement
        // should be opened; all admitted logical streams must carry data.
        let mut requests = Vec::new();
        for _ in 0..8 {
            requests.push(pool.open(&profile, "localhost", open));
        }
        let streams = futures_util::future::join_all(requests).await;
        assert_eq!(count.load(Ordering::Relaxed), 2);
        for (id, stream) in streams.into_iter().enumerate() {
            let mut stream = stream.unwrap();
            exchange(&mut stream, &vec![id as u8; 4097]).await;
        }
        for connection in incoming.lock().unwrap().drain(..) {
            connection.close();
        }
    })
    .await
    .expect("gRPC reconnect or recovered route stalled");
}

async fn exchange(stream: &mut GrpcStream, payload: &[u8]) {
    AsyncWriteExt::write_all(stream, payload).await.unwrap();
    AsyncWriteExt::flush(stream).await.unwrap();
    let mut received = vec![0; payload.len()];
    stream.read_exact(&mut received).await.unwrap();
    assert_eq!(received, payload);
}
