//! Actual canonical native holders close in the original registered driver.
use super::*;
mod start;
use crate::runtime::inbound_owners::bind::tests::{lease, proxy};
use std::error::Error;
use tokio::{
    net::TcpListener,
    sync::Notify,
    time::{timeout, Duration},
};

async fn rebound(address: std::net::SocketAddr) -> TcpListener {
    timeout(Duration::from_secs(2), async {
        loop {
            match TcpListener::bind(address).await {
                Ok(listener) => break listener,
                Err(_) => tokio::task::yield_now().await,
            }
        }
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn requested_driver_exit_closes_original_listener_without_fabricating_its_join() {
    let proxy = proxy();
    let config = proxy.engine().config();
    let mut lease = lease(&proxy, &config, 0);
    lease.bind_tcp().unwrap();
    let shutdown = lease.shutdown_sender();
    let (operation, mut listener, installed) = lease.finish().unwrap().promote().unwrap();
    drop(operation);
    let address = listener.local_addr().unwrap();
    let owner = installed.owner();
    let entered = Arc::new(Notify::new());
    let pending = entered.clone();
    let mut stopping = shutdown.subscribe();
    owner.spawn_driver(async move {
        let accepting = listener.accept();
        tokio::pin!(accepting);
        tokio::select! {
            _ = stopping.wait_for(|stop| *stop) => Ok(()),
            result = std::future::poll_fn(|cx| {
                let result = accepting.as_mut().poll(cx);
                if result.is_pending() { pending.notify_one(); }
                result
            }) => Err(result.err().unwrap_or_else(|| std::io::Error::other("unexpected accept")).into()),
        }
    }).unwrap();
    owner.seal_listener_installation();
    entered.notified().await; // Actual native accept was polled Pending.
    assert!(TcpListener::bind(address).await.is_err());
    shutdown.send_replace(true);
    let replacement = rebound(address).await;
    assert_eq!(replacement.local_addr().unwrap(), address);
    assert!(!owner.driver_finished.load(Ordering::Acquire));
    assert!(!owner.scope_closed.load(Ordering::Acquire));
    assert!(owner.terminal().is_none());
    {
        let report = owner.report.lock().unwrap();
        assert_eq!(report.closed_io.len(), 1);
        assert!(
            report.exited_tasks.is_empty(),
            "native closure is not actual Scope consumption"
        );
    }
    let receipt = proxy
        .inbound_owners
        .confirm_shutdown("first", Instant::now() + Duration::from_secs(2))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(receipt.terminal().joined_tasks(), 1);
    assert_eq!(receipt.terminal().closed_io_receipts(), 1);
    assert!(receipt.terminal().failure().is_none());
    proxy
        .inbound_owners
        .release_closed_reported(receipt, |_| Ok(()))
        .unwrap();
}

#[tokio::test]
async fn work_panic_closes_same_original_then_resumes_in_the_same_registered_task() {
    let proxy = proxy();
    let config = proxy.engine().config();
    let mut lease = lease(&proxy, &config, 0);
    lease.bind_tcp().unwrap();
    let (operation, mut listener, installed) = lease.finish().unwrap().promote().unwrap();
    drop(operation);
    let address = listener.local_addr().unwrap();
    let owner = installed.owner();
    let entered = Arc::new(Notify::new());
    let pending = entered.clone();
    let native_id = Arc::new(Mutex::new(None));
    let actual_id = native_id.clone();
    let (panic, mut panicking) = tokio::sync::oneshot::channel::<()>();
    owner.spawn_driver(async move {
        *actual_id.lock().unwrap() = Some(tokio::task::id());
        let accepting = listener.accept();
        tokio::pin!(accepting);
        tokio::select! {
            _ = &mut panicking => panic!("original driver work panic before explicit native close"),
            result = std::future::poll_fn(|cx| {
                let result = accepting.as_mut().poll(cx);
                if result.is_pending() { pending.notify_one(); }
                result
            }) => Err(result.err().unwrap_or_else(|| std::io::Error::other("unexpected accept")).into()),
        }
    }).unwrap();
    owner.seal_listener_installation();
    entered.notified().await;
    panic.send(()).unwrap();
    let replacement = rebound(address).await;
    assert!(!owner.driver_finished.load(Ordering::Acquire));
    assert!(owner.terminal().is_none());
    assert_eq!(owner.report.lock().unwrap().closed_io.len(), 1);
    assert!(owner.observe_driver().await.is_err());
    {
        let failures = owner.task_failures.lock().unwrap();
        let fault = &failures[owner.driver.get().unwrap()];
        assert_eq!(fault.class, TaskClass::Critical);
        let original = fault
            .source
            .source()
            .unwrap()
            .downcast_ref::<tokio::task::JoinError>()
            .unwrap();
        assert!(original.is_panic());
        assert_eq!(original.id(), native_id.lock().unwrap().unwrap());
    }
    let receipt = proxy
        .inbound_owners
        .confirm_shutdown("first", Instant::now() + Duration::from_secs(2))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(receipt.terminal().joined_tasks(), 1);
    assert_eq!(receipt.terminal().closed_io_receipts(), 1);
    assert_eq!(receipt.terminal().task_failures().len(), 1);
    proxy
        .inbound_owners
        .release_closed_reported(receipt, |_| Ok(()))
        .unwrap();
    assert_eq!(replacement.local_addr().unwrap(), address);
}
