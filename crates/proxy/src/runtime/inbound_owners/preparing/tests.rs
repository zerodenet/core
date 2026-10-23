use super::*;
use crate::runtime::Proxy;
use std::{future::Future, task::Poll};
use tokio::{
    io::AsyncReadExt,
    net::{TcpListener, TcpStream},
    sync::Notify,
    time::{timeout, Duration},
};
#[tokio::test]
async fn phase_stop_targets_only_real_unready_candidates_and_fences_late_installation() {
    let proxy = Proxy::new(
        zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap(),
    )
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (stop, mut stopping) = watch::channel(false);
    let old = proxy
        .inbound_owners
        .install("last-good", &proxy.protocols, 0, stop)
        .unwrap()
        .owner();
    let ready = old.readiness();
    old.spawn_driver(async move {
        ready.poll_loop(async {
            tokio::select! {
                _ = async { let _ = stopping.wait_for(|stop| *stop).await; } => Ok(()),
                accepted = listener.accept() => { accepted?; panic!("actual last-good listener remains idle") },
            }
        }).await
    }).unwrap();
    let old_observer = {
        let old = old.clone();
        tokio::spawn(async move { old.observe_driver().await })
    };
    timeout(Duration::from_secs(2), old.wait_ready())
        .await
        .unwrap()
        .unwrap();
    let phase = PreparingInbounds::default();
    let (stop, _) = watch::channel(false);
    let candidate = proxy
        .inbound_owners
        .install("candidate", &proxy.protocols, 0, stop)
        .unwrap()
        .owner();
    phase.register(&candidate).unwrap();
    let gate = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (client, accepted) = tokio::join!(
        TcpStream::connect(gate.local_addr().unwrap()),
        gate.accept()
    );
    let _client = client.unwrap();
    let (mut io, _) = accepted.unwrap();
    let pending = Arc::new(Notify::new());
    let entered = pending.clone();
    let cancel = candidate.task_access().cancellation();
    candidate
        .spawn_driver(async move {
            let read = io.read_u8();
            tokio::pin!(read);
            tokio::select! {
                _ = cancel.cancelled() => Ok(()),
                result = std::future::poll_fn(|cx| match read.as_mut().poll(cx) {
                    Poll::Pending => { entered.notify_one(); Poll::Pending },
                    Poll::Ready(result) => Poll::Ready(result),
                }) => { result?; panic!("actual activation gate remains pending") },
            }
        })
        .unwrap();
    let candidate_observer = {
        let owner = candidate.clone();
        tokio::spawn(async move { owner.observe_driver().await })
    };
    timeout(Duration::from_secs(2), pending.notified())
        .await
        .unwrap();
    phase.begin_shutdown();
    assert!(preparation_stopped(
        &candidate.wait_ready().await.unwrap_err()
    ));
    assert!(!old.task_access().cancellation().is_cancelled());
    old.wait_ready().await.unwrap();
    assert!(TcpListener::bind(address).await.is_err());
    candidate_observer.await.unwrap().unwrap();
    proxy
        .inbound_owners
        .shutdown("candidate", Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    let (stop, _) = watch::channel(false);
    let late = proxy
        .inbound_owners
        .install("late", &proxy.protocols, 0, stop)
        .unwrap()
        .owner();
    assert!(preparation_stopped(&phase.register(&late).unwrap_err()));
    assert!(preparation_stopped(
        &late
            .spawn_driver(async { panic!("closed phase must never poll late activation") })
            .unwrap_err()
    ));
    proxy
        .inbound_owners
        .shutdown("late", Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    old.task_access().request_stop();
    old_observer.await.unwrap().unwrap();
    proxy
        .inbound_owners
        .shutdown("last-good", Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    let _closed = TcpListener::bind(address).await.unwrap();
}
