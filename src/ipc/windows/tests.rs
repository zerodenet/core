//! Real Windows IPC peers; cross compilation alone is not execution evidence.
use super::*;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::windows::named_pipe::{ClientOptions, NamedPipeClient},
};
use zero_runtime::configuration::{ControlBudget, ControlBudgetLimits};
mod recovery;
fn budget() -> Arc<ControlBudget> {
    ControlBudget::new(ControlBudgetLimits {
        connections: 2,
        subscriptions: 1,
        queued_event_bytes: 1024,
        pipe_workspace_bytes: 2 * 1024 * 1024,
    })
}
fn handles() -> (zero_engine::EngineHandle, ProxyHandle) {
    let proxy = zero_proxy::Proxy::new(
        zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap(),
    )
    .unwrap();
    let engine = zero_engine::EngineHandle::new(proxy.engine().clone());
    (engine.clone(), ProxyHandle::new(engine, proxy))
}
fn path() -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    PathBuf::from(format!(
        r"\\.\pipe\zero-control-original-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::AcqRel)
    ))
}
async fn frame(client: &mut BufReader<NamedPipeClient>) -> serde_json::Value {
    let mut line = String::new();
    assert!(client.read_line(&mut line).await.unwrap() > 0);
    serde_json::from_str(&line).unwrap()
}
async fn ping(path: &std::path::Path) -> BufReader<NamedPipeClient> {
    let mut client = BufReader::new(ClientOptions::new().open(path).unwrap());
    client
        .get_mut()
        .write_all(b"{\"type\":\"ping\",\"id\":\"actual-pipe\"}\n")
        .await
        .unwrap();
    let response = frame(&mut client).await;
    assert_eq!(response["id"], "actual-pipe");
    assert_eq!(response["ok"], true);
    client
}
async fn actual_pending(owner: &IpcServerHandle) -> Vec<Arc<owner::Entry>> {
    loop {
        let originals = owner.originals.originals_for_test();
        if originals.len() == 2
            && originals.iter().any(|entry| entry.original.pending_io().0)
            && originals.iter().any(|entry| entry.original.pending_io().1)
        {
            return originals;
        }
        tokio::task::yield_now().await;
    }
}
fn originals_closed(originals: &[Arc<owner::Entry>]) {
    for entry in originals {
        let closed = entry
            .original
            .closed()
            .expect("same actual pipe native close receipt");
        assert!(entry.original.matches_closed(&closed) && closed.had_native_acquisition());
        assert!(entry.original.view_returned() && entry.original.std_worker_joined());
        let worker = entry
            .original
            .worker()
            .expect("same actual Scope join guard");
        assert!(worker.is_joined());
        assert!(worker.try_joined().unwrap().registration_released());
        worker.try_joined().unwrap().result().unwrap();
    }
}
#[tokio::test]
async fn pending_named_pipe_subscription_joins_workers_before_returning_native_charges() {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let (engine, handle) = handles();
        let budget = budget();
        let path = path();
        let owner = prepare_ipc_server_with_budget(&path, budget.clone());
        owner.start(handle).await.unwrap();
        let mut client = ping(&path).await;
        client
            .get_mut()
            .write_all(b"{\"type\":\"subscribe\",\"events\":[\"test.actual.pipe\"]}\n")
            .await
            .unwrap();
        assert_eq!(frame(&mut client).await["ok"], true);
        engine.emit(zero_api::RawApiEvent::new(
            "actual.pipe.event",
            "test.actual.pipe",
            0,
            serde_json::json!({"wire":"original"}),
        ));
        let event = frame(&mut client).await;
        assert_eq!(event["event_type"], "test.actual.pipe");
        assert_eq!(event["payload"]["wire"], "original");
        let originals = actual_pending(&owner).await;
        assert_eq!(budget.usage().connections, 2);
        assert_eq!(
            budget.usage().pipe_workspace_bytes,
            2 * ControlledPipeOwner::workspace_bytes()
        );
        assert_eq!(budget.usage().subscriptions, 1);
        assert!(originals
            .iter()
            .all(|entry| entry.original.closed().is_none()));
        // Keep the real client and native read pending while shutdown requests
        // stop, joins the subscription and cancels/drains exact OVERLAPPED IO.
        owner.shutdown().await.unwrap();
        assert!(owner.retirement_complete() && owner.originals.is_empty());
        originals_closed(&originals);
        budget.close_admission();
        budget.confirm_released().unwrap();
        drop(client);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn pipe_workspace_capacity_precedes_native_and_refund_waits_for_real_host_ack() {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let cost = ControlledPipeOwner::workspace_bytes();
        let mut limits = budget().limits();
        limits.pipe_workspace_bytes = cost - 1;
        let small = ControlBudget::new(limits);
        let mut scope = TaskScope::suspended_hosted(
            tokio::runtime::Handle::current(),
            std::num::NonZeroUsize::new(1),
        );
        scope.enable_registration().unwrap();
        let originals = Originals::default();
        let path = path();
        assert_eq!(
            originals
                .prepare(&path, scope.registrar(), &small)
                .err()
                .unwrap()
                .kind(),
            io::ErrorKind::WouldBlock
        );
        assert!(originals.is_empty());
        assert_eq!(small.usage().connections, 0);
        assert_eq!(small.usage().pipe_workspace_bytes, 0);
        limits.pipe_workspace_bytes = cost;
        let budget = ControlBudget::new(limits);
        let entry = originals
            .prepare(&path, scope.registrar(), &budget)
            .unwrap();
        let view = entry.original.open().await.unwrap();
        while !entry.original.pending_io().0 {
            tokio::task::yield_now().await;
        }
        assert_eq!(budget.usage().pipe_workspace_bytes, cost);
        let worker = entry.original.worker().unwrap();
        drop(view);
        let receipt = scope.next_receipt().await.unwrap();
        let joined = receipt.joined();
        assert!(worker.matches_joined(joined) && !joined.registration_released());
        joined.result().unwrap();
        // std and Scope joining/native closure remain independent of ACK.
        let closed = entry
            .original
            .close(super::super::owner::close_deadline())
            .await
            .unwrap();
        assert!(entry.original.matches_closed(&closed) && closed.had_native_acquisition());
        assert!(entry.original.std_worker_joined());
        assert_eq!(
            originals.reap(true).await.unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        assert_eq!(budget.usage().connections, 1);
        assert_eq!(budget.usage().pipe_workspace_bytes, cost);
        scope.acknowledge(&receipt).unwrap();
        assert!(worker.try_joined().unwrap().registration_released());
        originals.reap(true).await.unwrap();
        assert!(originals.is_empty());
        assert_eq!(budget.usage().pipe_workspace_bytes, 0);
        budget.close_admission();
        budget.confirm_released().unwrap();
    })
    .await
    .unwrap();
}
