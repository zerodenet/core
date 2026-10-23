use super::*;
use crate::runtime::Proxy;
use std::future::Future;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::oneshot,
    time::{timeout, Duration},
};

#[tokio::test]
async fn same_private_join_cell_retains_original_exit_without_retaining_the_peer_graph() {
    let proxy = Proxy::new(
        zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap(),
    )
    .unwrap();
    let (stop, mut stopping) = watch::channel(false);
    let installed = proxy
        .inbound_owners
        .install_listener("join-view", &proxy.protocols, 1, stop)
        .unwrap();
    let owner = installed.owner();
    let peers = installed.peer_tasks();
    let access = installed.tasks();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut remote = TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let (mut io, _) = listener.accept().await.unwrap();
    let (sent, received) = oneshot::channel();
    let pending = Arc::new(Notify::new());
    owner
        .spawn_driver({
            let pending = pending.clone();
            async move {
                assert!(peers.spawn_bound(&access, |binding| async move {
                    let task = binding.wait_bound().await.map_err(std::io::Error::other)?;
                    let view = binding.join_observer();
                    assert!(view.belongs_to(&binding));
                    assert!(view
                        .join_witness()
                        .unwrap()
                        .same_registration(&binding.join_witness().unwrap()));
                    sent.send((view, Arc::downgrade(&binding), task)).unwrap();
                    let read = io.read_u8();
                    tokio::pin!(read);
                    let _ = std::future::poll_fn(|cx| {
                        let result = read.as_mut().poll(cx);
                        if result.is_pending() {
                            pending.notify_one();
                        }
                        result
                    })
                    .await?;
                    Err(EngineError::NoInbounds)
                })?);
                let _ = stopping.wait_for(|stopped| *stopped).await;
                Ok(())
            }
        })
        .unwrap();
    owner.seal_listener_installation();
    let (view, binding, task) = received.await.unwrap();
    timeout(Duration::from_secs(2), pending.notified())
        .await
        .unwrap();
    assert!(view.observed_join().is_none());
    assert!(!view.join_witness().unwrap().is_joined());
    assert!(view.belongs_to(&binding.upgrade().unwrap()));
    remote.write_u8(1).await.unwrap();
    assert!(timeout(Duration::from_secs(2), owner.observe_listener())
        .await
        .unwrap()
        .is_err());
    assert!(
        binding.upgrade().is_none(),
        "read-only join observer cannot retain Binding/group resources"
    );
    let joined = view
        .wait_joined(Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    assert_eq!(joined.task(), task);
    assert_eq!(joined.class(), TaskClass::Critical);
    assert!(Arc::ptr_eq(&joined, &view.observed_join().unwrap()));
    let original = joined.failure().unwrap();
    let receipt = proxy
        .inbound_owners
        .confirm_shutdown("join-view", Instant::now() + Duration::from_secs(2))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(receipt.terminal().joined_tasks(), 2);
    assert_eq!(
        receipt.terminal().closed_io_receipts(),
        0,
        "join observation does not invent native receipts"
    );
    assert!(Arc::ptr_eq(
        &original,
        &receipt.terminal().failure().unwrap()
    ));
    assert!(proxy
        .inbound_owners
        .release_closed_reported(receipt, |_| Ok(()))
        .is_err());
    drop(installed);
    drop(owner);
    drop(proxy);
    assert!(
        Arc::ptr_eq(&joined, &view.observed_join().unwrap()),
        "small original receipt survives only in its actual observer"
    );
}
