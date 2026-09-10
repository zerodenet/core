use super::super::fixtures::{echo, profile};
use super::fixtures::*;
use mieru::client::{ClientConnection, ClientPool};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zero_platform_tokio::TcpRelayStream;

#[tokio::test]
async fn disconnected_connection_is_replaced_without_replaying_application_data() {
    let pool = ClientPool::default();
    let count = Arc::new(AtomicUsize::new(0));
    let (client, server) = tokio::io::duplex(65536);
    let task = tokio::spawn(async move {
        echo(
            profile()
                .accept_multiplexer(TcpRelayStream::new(server))
                .await
                .unwrap(),
        )
        .await;
    });
    let connection = ClientConnection::tcp(TcpRelayStream::new(client), "u", "p")
        .await
        .unwrap();
    let mut stream = pool
        .open(key("u", "p"), || async { Ok(connection.clone()) })
        .await
        .unwrap();
    task.abort();
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while !connection.is_closed() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(stream.write_all(b"do not replay").await.is_err());
    let _fresh = pool
        .open(key("u", "p"), || connect(count.clone(), "u", "p"))
        .await
        .unwrap();
    assert_eq!(count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn clear_detaches_an_inflight_dial_without_closing_its_returned_stream() {
    let pool = Arc::new(ClientPool::default());
    let old_count = Arc::new(AtomicUsize::new(0));
    let (old_entered, mut old_entries) = tokio::sync::mpsc::unbounded_channel();
    let releases = Arc::new([tokio::sync::Notify::new(), tokio::sync::Notify::new()]);

    let old = {
        let pool = pool.clone();
        let count = old_count.clone();
        let entered = old_entered.clone();
        let releases = releases.clone();
        tokio::spawn(async move {
            pool.open(key("u", "p"), || {
                let count = count.clone();
                let entered = entered.clone();
                let releases = releases.clone();
                async move {
                    let dial = count.fetch_add(1, Ordering::SeqCst);
                    entered.send(dial).unwrap();
                    releases[dial].notified().await;
                    establish("u", "p").await
                }
            })
            .await
        })
    };
    assert_eq!(old_entries.recv().await, Some(0));

    let old_waiter = {
        let pool = pool.clone();
        let count = old_count.clone();
        let entered = old_entered.clone();
        let releases = releases.clone();
        tokio::spawn(async move {
            pool.open(key("u", "p"), || {
                let count = count.clone();
                let entered = entered.clone();
                let releases = releases.clone();
                async move {
                    let dial = count.fetch_add(1, Ordering::SeqCst);
                    entered.send(dial).unwrap();
                    releases[dial].notified().await;
                    establish("u", "p").await
                }
            })
            .await
        })
    };
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(50), old_entries.recv())
            .await
            .is_err()
    );
    pool.clear();
    assert_eq!(
        tokio::time::timeout(std::time::Duration::from_secs(2), old_entries.recv())
            .await
            .unwrap(),
        Some(1)
    );

    let fresh_count = Arc::new(AtomicUsize::new(0));
    let mut fresh = pool
        .open(key("u", "p"), || connect(fresh_count.clone(), "u", "p"))
        .await
        .unwrap();
    assert_eq!(fresh_count.load(Ordering::SeqCst), 1);

    releases[1].notify_one();
    releases[0].notify_one();
    let mut old = old.await.unwrap().unwrap();
    let mut old_waiter = old_waiter.await.unwrap().unwrap();
    assert_eq!(old_count.load(Ordering::SeqCst), 2);

    old.write_all(b"old").await.unwrap();
    let mut result = [0; 3];
    old.read_exact(&mut result).await.unwrap();
    assert_eq!(&result, b"old");
    fresh.write_all(b"new").await.unwrap();
    fresh.read_exact(&mut result).await.unwrap();
    assert_eq!(&result, b"new");
    old_waiter.write_all(b"waiter").await.unwrap();
    let mut waiter_result = [0; 6];
    old_waiter.read_exact(&mut waiter_result).await.unwrap();
    assert_eq!(&waiter_result, b"waiter");

    let _reused = pool
        .open(key("u", "p"), || connect(fresh_count.clone(), "u", "p"))
        .await
        .unwrap();
    assert_eq!(fresh_count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn cancelled_dial_wakes_an_existing_waiter() {
    let pool = Arc::new(ClientPool::default());
    let (leader_entered, mut leader_entries) = tokio::sync::mpsc::unbounded_channel();
    let leader = {
        let pool = pool.clone();
        tokio::spawn(async move {
            pool.open(key("u", "p"), || {
                let leader_entered = leader_entered.clone();
                async move {
                    leader_entered.send(()).unwrap();
                    std::future::pending::<std::io::Result<Arc<ClientConnection>>>().await
                }
            })
            .await
        })
    };
    leader_entries.recv().await.unwrap();

    let count = Arc::new(AtomicUsize::new(0));
    let (waiter_dialed, mut waiter_dials) = tokio::sync::mpsc::unbounded_channel();
    let waiter = {
        let pool = pool.clone();
        let count = count.clone();
        tokio::spawn(async move {
            pool.open(key("u", "p"), || {
                let count = count.clone();
                let waiter_dialed = waiter_dialed.clone();
                async move {
                    waiter_dialed.send(()).unwrap();
                    connect(count, "u", "p").await
                }
            })
            .await
        })
    };
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(50), waiter_dials.recv())
            .await
            .is_err()
    );

    leader.abort();
    let _ = leader.await;
    tokio::time::timeout(std::time::Duration::from_secs(2), waiter)
        .await
        .expect("existing waiter was not woken after dial cancellation")
        .unwrap()
        .unwrap();
    assert_eq!(waiter_dials.recv().await, Some(()));
    assert_eq!(count.load(Ordering::SeqCst), 1);
}
