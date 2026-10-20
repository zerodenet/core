use super::*;
use crate::runtime::{inbound_owners::InboundOwnerRegistry, Proxy};
use std::{
    num::NonZeroUsize,
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};
use tokio::{
    sync::{oneshot, watch, Mutex},
    time::Instant,
};

#[tokio::test]
async fn association_body_runs_once_and_its_metadata_does_not_reclaim_the_actual_scope_slot() {
    let proxy = Proxy::new(
        zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap(),
    )
    .unwrap();
    let registry = Arc::new(InboundOwnerRegistry::default());
    let (stop, _) = watch::channel(false);
    let installed = registry
        .install("actual-association", &proxy.protocols, 1, stop)
        .unwrap();
    let group = installed
        .child_tasks(NonZeroUsize::new(1).unwrap())
        .unwrap();
    let owner = installed.owner();
    let (stop_parent, parent_stopped) = oneshot::channel();
    owner
        .spawn_driver(async move {
            parent_stopped.await.map_err(std::io::Error::other)?;
            Ok(())
        })
        .unwrap();
    let (completed, receiver) = mpsc::channel(1);
    let receiver = Mutex::new(receiver);
    let mut tasks = ScopedTasks {
        scope: AssociationScope {
            group: group.clone(),
            host: installed.tasks(),
        },
        completed,
        receiver: receiver.lock().await,
        lost: |result: Result<(), EngineError>| result,
    };
    let calls = Arc::new(AtomicUsize::new(0));
    let entered = calls.clone();
    tasks
        .spawn(move |binding| async move {
            binding
                .unwrap()
                .wait_bound()
                .await
                .map_err(std::io::Error::other)?;
            entered.fetch_add(1, Ordering::AcqRel);
            Ok(())
        })
        .unwrap();
    tasks.receiver.recv().await.unwrap().unwrap();
    assert_eq!(calls.load(Ordering::Acquire), 1);
    assert_eq!(
        group.occupied(),
        1,
        "body output is not the original Scope's consumed TaskExit"
    );
    let called = calls.clone();
    assert!(tasks
        .spawn(move |_| {
            called.fetch_add(100, Ordering::AcqRel);
            async { Ok(()) }
        })
        .is_err());
    assert_eq!(calls.load(Ordering::Acquire), 1);
    stop_parent.send(()).unwrap();
    owner
        .shutdown(Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    assert_eq!(group.occupied(), 0);
    assert_eq!(owner.terminal_for_test().unwrap().joined_tasks(), 2);
}
