use super::fixtures::*;
use mieru::client::{ClientPool, ClientPoolPolicy};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn concurrent_opens_share_one_carrier_and_clear_drains_existing_sessions() {
    tokio::time::timeout(std::time::Duration::from_secs(15), async {
        let pool = Arc::new(ClientPool::default());
        let count = Arc::new(AtomicUsize::new(0));
        let mut tasks = tokio::task::JoinSet::new();
        for byte in 0..12u8 {
            let pool = pool.clone();
            let count = count.clone();
            tasks.spawn(async move {
                let mut stream = pool
                    .open(key("u", "p"), || connect(count.clone(), "u", "p"))
                    .await
                    .unwrap();
                stream.write_all(&[byte; 512]).await.unwrap();
                stream.flush().await.unwrap();
                let mut result = [0; 512];
                stream.read_exact(&mut result).await.unwrap();
                assert_eq!(result, [byte; 512]);
                stream
            });
        }
        let mut streams = Vec::new();
        while let Some(result) = tasks.join_next().await {
            streams.push(result.unwrap());
        }
        assert_eq!(count.load(Ordering::SeqCst), 1);
        pool.clear();
        for stream in &mut streams {
            stream.write_all(b"old").await.unwrap();
            let mut result = [0; 3];
            stream.read_exact(&mut result).await.unwrap();
            assert_eq!(&result, b"old");
        }
        let mut fresh = pool
            .open(key("u", "p"), || connect(count.clone(), "u", "p"))
            .await
            .unwrap();
        assert_eq!(count.load(Ordering::SeqCst), 2);
        fresh.write_all(b"new").await.unwrap();
        let mut result = [0; 3];
        fresh.read_exact(&mut result).await.unwrap();
        assert_eq!(&result, b"new");
        let _separate = pool
            .open(key("v", "q"), || connect(count.clone(), "v", "q"))
            .await
            .unwrap();
        assert_eq!(count.load(Ordering::SeqCst), 3);
        let _egress = pool
            .open(key("u", "p").with_generation(1), || {
                connect(count.clone(), "u", "p")
            })
            .await
            .unwrap();
        assert_eq!(count.load(Ordering::SeqCst), 4);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn failed_open_on_one_carrier_does_not_block_a_healthy_carrier() {
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        let pool = Arc::new(
            ClientPool::with_policy(ClientPoolPolicy {
                max_connections_per_identity: 2,
                scale_out_load_percent: 1,
                max_connection_age: std::time::Duration::from_secs(60),
            })
            .unwrap(),
        );
        let (first, first_server_blocked, first_server) = establish_with_server_write_gate().await;
        let mut first_streams = Vec::with_capacity(3);
        first_streams.push(
            pool.open(key("u", "p"), || {
                let first = first.clone();
                async move { Ok(first) }
            })
            .await
            .unwrap(),
        );
        for _ in 1..3 {
            first_streams.push(
                pool.open(key("u", "p"), || async {
                    Err(std::io::Error::other("unexpected carrier dial"))
                })
                .await
                .unwrap(),
            );
        }

        let healthy_dials = Arc::new(AtomicUsize::new(0));
        let second_stream = pool
            .open(key("u", "p"), || connect(healthy_dials.clone(), "u", "p"))
            .await
            .unwrap();
        assert_eq!(healthy_dials.load(Ordering::SeqCst), 1);

        for _ in 0..2 {
            let mut released = first_streams.pop().unwrap();
            released.shutdown().await.unwrap();
            drop(released);
        }
        for _ in 0..32 {
            tokio::task::yield_now().await;
        }
        first_server_blocked.store(true, Ordering::Release);

        let stalled = {
            let pool = pool.clone();
            let healthy_dials = healthy_dials.clone();
            tokio::spawn(async move {
                pool.open(key("u", "p"), || connect(healthy_dials.clone(), "u", "p"))
                    .await
            })
        };
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(!stalled.is_finished(), "first carrier OPEN did not stall");

        let mut healthy = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            pool.open(key("u", "p"), || connect(healthy_dials.clone(), "u", "p")),
        )
        .await
        .expect("healthy carrier was blocked behind another carrier OPEN")
        .unwrap();
        healthy.write_all(b"healthy").await.unwrap();
        let mut result = [0; 7];
        healthy.read_exact(&mut result).await.unwrap();
        assert_eq!(&result, b"healthy");

        first_server.abort();
        let mut recovered = tokio::time::timeout(std::time::Duration::from_secs(2), stalled)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        recovered.write_all(b"retry").await.unwrap();
        let mut retry = [0; 5];
        recovered.read_exact(&mut retry).await.unwrap();
        assert_eq!(&retry, b"retry");
        assert_eq!(healthy_dials.load(Ordering::SeqCst), 1);

        drop(second_stream);
        drop(first_streams);
    })
    .await
    .unwrap();
}
