use super::*;
use std::{
    future::{poll_fn, Future},
    task::Poll,
    time::Duration,
};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};

#[tokio::test]
async fn cancelled_ipc_shutdown_waiter_retains_actual_subscriber_and_native_originals() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("control.sock");
    let proxy = zero_proxy::Proxy::new(
        zero_config::RuntimeConfig::parse(r#"{"route":{"rules":[],"final":{"type":"direct"}}}"#)
            .unwrap(),
    )
    .unwrap();
    let engine = zero_engine::EngineHandle::new(proxy.engine().clone());
    let server = spawn_ipc_server(ProxyHandle::new(engine, proxy), &path)
        .await
        .unwrap();
    let client = tokio::net::UnixStream::connect(&path).await.unwrap();
    let mut client = BufReader::new(client);
    client
        .get_mut()
        .write_all(b"{\"type\":\"subscribe\",\"events\":[\"test.never\"]}\n")
        .await
        .unwrap();
    let mut ack = String::new();
    client.read_line(&mut ack).await.unwrap();
    assert!(
        serde_json::from_str::<serde_json::Value>(&ack).unwrap()["ok"]
            .as_bool()
            .unwrap()
    );
    let mut stopping = Box::pin(server.shutdown());
    poll_fn(|cx| {
        assert!(stopping.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(stopping);
    tokio::time::timeout(Duration::from_secs(2), server.shutdown())
        .await
        .unwrap()
        .unwrap();
    assert!(!path.exists());
    assert!(server.originals.active.lock().unwrap().is_empty());
    assert!(server.originals.activating.lock().unwrap().is_none());
    let closed = server
        .listener
        .close(super::super::owner::close_deadline())
        .await
        .unwrap();
    assert!(closed.had_native_acquisition());
    assert!(server.listener.matches_closed(&closed));
    assert_eq!(client.read(&mut [0]).await.unwrap(), 0);
}
#[tokio::test]
async fn actual_ipc_root_budget_waits_before_accept_and_reuses_capacity_only_after_join_and_native_close(
) {
    tokio::time::timeout(Duration::from_secs(5), async {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("budget.sock");
        let proxy = zero_proxy::Proxy::new(
            zero_config::RuntimeConfig::parse(
                r#"{"route":{"rules":[],"final":{"type":"direct"}}}"#,
            )
            .unwrap(),
        )
        .unwrap();
        let engine = zero_engine::EngineHandle::new(proxy.engine().clone());
        let budget = zero_runtime::configuration::ControlBudget::new(
            zero_runtime::configuration::ControlBudgetLimits {
                connections: 1,
                subscriptions: 1,
                queued_event_bytes: 1024,
                pipe_workspace_bytes: 2 * 1024 * 1024,
            },
        );
        let held = budget
            .reserve(zero_runtime::configuration::ControlCharge::Connection)
            .unwrap();
        let server = prepare_ipc_server_with_budget(&path, budget.clone());
        server.start(ProxyHandle::new(engine, proxy)).await.unwrap();
        let mut first = BufReader::new(tokio::net::UnixStream::connect(&path).await.unwrap());
        first
            .get_mut()
            .write_all(b"{\"type\":\"subscribe\",\"events\":[\"test.never\"]}\n")
            .await
            .unwrap();
        let mut ack = String::new();
        assert!(
            tokio::time::timeout(Duration::from_millis(50), first.read_line(&mut ack))
                .await
                .is_err()
        );
        assert!(server.originals.active.lock().unwrap().is_empty());
        assert!(server.originals.activating.lock().unwrap().is_none());
        assert_eq!(budget.usage().connections, 1);
        drop(held);
        first.read_line(&mut ack).await.unwrap();
        assert!(
            serde_json::from_str::<serde_json::Value>(&ack).unwrap()["ok"]
                .as_bool()
                .unwrap()
        );
        assert_eq!(budget.usage().connections, 1);
        assert_eq!(budget.usage().subscriptions, 1);
        let mut second = BufReader::new(tokio::net::UnixStream::connect(&path).await.unwrap());
        second
            .get_mut()
            .write_all(b"{\"type\":\"subscribe\",\"events\":[\"test.never\"]}\n")
            .await
            .unwrap();
        let mut next = String::new();
        assert!(
            tokio::time::timeout(Duration::from_millis(50), second.read_line(&mut next))
                .await
                .is_err()
        );
        assert_eq!(server.originals.active.lock().unwrap().len(), 1);
        drop(first);
        second.read_line(&mut next).await.unwrap();
        assert!(
            serde_json::from_str::<serde_json::Value>(&next).unwrap()["ok"]
                .as_bool()
                .unwrap()
        );
        assert_eq!(budget.usage().connections, 1);
        assert_eq!(budget.usage().subscriptions, 1);
        server.shutdown().await.unwrap();
        assert_eq!(
            budget.usage(),
            zero_runtime::configuration::ControlBudgetUsage::default()
        );
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn consumed_server_fault_still_returns_its_source_after_matching_native_retirement() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("fault.sock");
        let proxy = zero_proxy::Proxy::new(
            zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap(),
        )
        .unwrap();
        let handle = ProxyHandle::new(
            zero_engine::EngineHandle::new(proxy.engine().clone()),
            proxy,
        );
        let server = prepare_ipc_server(&path);
        server.start(handle.clone()).await.unwrap();
        server.budget.close_admission();
        let pending_client = tokio::net::UnixStream::connect(&path).await.unwrap();
        let fault = server.wait_terminated().await.unwrap_err();
        assert!(!server.retirement_complete());
        assert_eq!(fault.kind(), io::ErrorKind::BrokenPipe);
        let closed_fault = server.shutdown().await.unwrap_err();
        assert!(server.retirement_complete());
        assert!(server.originals.active.lock().unwrap().is_empty());
        assert!(server.originals.activating.lock().unwrap().is_none());
        let closed = server
            .listener
            .close(super::super::owner::close_deadline())
            .await
            .unwrap();
        assert!(closed.had_native_acquisition() && server.listener.matches_closed(&closed));
        server.budget.confirm_released().unwrap();
        let initial = fault
            .get_ref()
            .unwrap()
            .downcast_ref::<Arc<io::Error>>()
            .unwrap();
        let retained = closed_fault
            .get_ref()
            .unwrap()
            .downcast_ref::<Arc<io::Error>>()
            .unwrap();
        assert!(
            Arc::ptr_eq(initial, retained),
            "retirement never fabricates a successful server exit"
        );
        assert!(!path.exists());
        drop(pending_client);
        let replacement = spawn_ipc_server(handle, &path).await.unwrap();
        let mut client = BufReader::new(tokio::net::UnixStream::connect(&path).await.unwrap());
        client
            .get_mut()
            .write_all(b"{\"type\":\"ping\",\"id\":1}\n")
            .await
            .unwrap();
        let mut line = String::new();
        client.read_line(&mut line).await.unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&line).unwrap()["ok"],
            true
        );
        replacement.shutdown().await.unwrap();
        assert!(replacement.retirement_complete());
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn invalid_utf8_closes_only_its_actual_client_and_keeps_the_same_server_available() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("frame.sock");
        let proxy = zero_proxy::Proxy::new(
            zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap(),
        )
        .unwrap();
        let engine = zero_engine::EngineHandle::new(proxy.engine().clone());
        let server = spawn_ipc_server(ProxyHandle::new(engine, proxy), &path)
            .await
            .unwrap();
        let mut invalid = tokio::net::UnixStream::connect(&path).await.unwrap();
        let original = loop {
            let actual = server
                .originals
                .active
                .lock()
                .unwrap()
                .values()
                .next()
                .cloned();
            if let Some(original) = actual {
                break original;
            }
            tokio::task::yield_now().await;
        };
        invalid.write_all(&[0xff, b'\n']).await.unwrap();
        assert_eq!(invalid.read(&mut [0]).await.unwrap(), 0);
        loop {
            if original.charge.lock().unwrap().is_none() {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(original
            .control
            .lock()
            .await
            .wait_closed(super::super::owner::close_deadline())
            .await
            .unwrap()
            .is_complete());
        let mut client = BufReader::new(tokio::net::UnixStream::connect(&path).await.unwrap());
        client
            .get_mut()
            .write_all(b"{\"type\":\"ping\",\"id\":\"same-owner\"}\n")
            .await
            .unwrap();
        let mut response = String::new();
        client.read_line(&mut response).await.unwrap();
        let response: serde_json::Value = serde_json::from_str(&response).unwrap();
        assert_eq!(response["ok"], true);
        assert_eq!(response["id"], "same-owner");
        server.shutdown().await.unwrap();
        assert!(server.retirement_complete());
        let closed = server
            .listener
            .close(super::super::owner::close_deadline())
            .await
            .unwrap();
        assert!(server.listener.matches_closed(&closed) && closed.had_native_acquisition());
    })
    .await
    .unwrap();
}
