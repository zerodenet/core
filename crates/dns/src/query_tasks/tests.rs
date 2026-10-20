use super::*;
use std::{
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};
use tokio::{
    runtime::Handle,
    time::{timeout, Instant},
};
use zero_platform_tokio::contract::scope::TaskScope;

#[tokio::test]
async fn original_query_origin_is_bound_before_io_and_cancel_requires_real_scope_join() {
    let mut scope = TaskScope::new(Handle::current(), NonZeroUsize::new(1).unwrap());
    let context = OwnedDnsQueryTasks::new(&scope.registrar(), NonZeroUsize::new(1).unwrap());
    let cancel = CancellationToken::new();
    let (started, entering) = oneshot::channel();
    let dropped = Arc::new(AtomicBool::new(false));
    struct Guard(Arc<AtomicBool>);
    impl Drop for Guard {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }
    let guard = Guard(dropped.clone());
    context
        .spawn(
            async move {
                let _guard = guard;
                started
                    .send(OwnedDnsQueryTasks::current_witness().unwrap())
                    .unwrap();
                std::future::pending::<()>().await;
            },
            cancel.clone(),
        )
        .unwrap();
    let actual = context.0.state.lock().unwrap().records[0]
        .witness
        .get()
        .unwrap()
        .clone()
        .unwrap();
    let origin = entering.await.unwrap();
    assert!(origin.same_registration(&actual));
    assert!(!actual.is_joined());
    cancel.cancel();
    timeout(Duration::from_secs(1), async {
        while !dropped.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(
        !actual.is_joined(),
        "query future Drop does not consume its original Scope task"
    );
    assert!(context.shutdown(Instant::now()).await.is_err());
    assert_eq!(context.0.state.lock().unwrap().records.len(), 1);
    let exit = scope.next_exit().await.unwrap();
    let joined = actual.try_joined().unwrap();
    assert_eq!(exit.id, joined.id());
    assert_eq!(exit.class, TaskClass::Auxiliary);
    assert!(joined.result().is_ok());
    context
        .shutdown(Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
    assert!(context.0.state.lock().unwrap().records.is_empty());
}

#[tokio::test]
async fn finished_body_does_not_reuse_explicit_query_capacity_before_actual_join() {
    let mut scope = TaskScope::new(Handle::current(), NonZeroUsize::new(2).unwrap());
    let context = OwnedDnsQueryTasks::new(&scope.registrar(), NonZeroUsize::new(1).unwrap());
    let (ended, end) = oneshot::channel();
    context
        .spawn(
            async move {
                ended.send(()).unwrap();
            },
            CancellationToken::new(),
        )
        .unwrap();
    end.await.unwrap();
    assert_eq!(
        context
            .spawn(async {}, CancellationToken::new())
            .unwrap_err()
            .kind(),
        io::ErrorKind::WouldBlock
    );
    scope.next_exit().await.unwrap();
    context.spawn(async {}, CancellationToken::new()).unwrap();
    scope.next_exit().await.unwrap();
    context
        .shutdown(Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
}

#[tokio::test]
async fn query_panic_retains_same_original_join_error_arc_and_blocks_new_admission() {
    let mut scope = TaskScope::new(Handle::current(), NonZeroUsize::new(1).unwrap());
    let context = OwnedDnsQueryTasks::new(&scope.registrar(), NonZeroUsize::new(1).unwrap());
    context
        .spawn(
            async { panic!("original DNS query worker") },
            CancellationToken::new(),
        )
        .unwrap();
    let witness = context.0.state.lock().unwrap().records[0]
        .witness
        .get()
        .unwrap()
        .clone()
        .unwrap();
    let exit = scope.next_exit().await.unwrap();
    assert!(exit.result.is_err());
    let original = witness.try_joined().unwrap().result().unwrap_err();
    let failed = context.ensure_open().unwrap_err();
    let retained = failed
        .get_ref()
        .unwrap()
        .downcast_ref::<Arc<ChannelError>>()
        .unwrap();
    assert!(Arc::ptr_eq(retained, &original));
    assert!(context.spawn(async {}, CancellationToken::new()).is_err());
    assert!(context.0.state.lock().unwrap().records.is_empty());
    assert!(Arc::ptr_eq(
        context
            .0
            .state
            .lock()
            .unwrap()
            .first_failure
            .as_ref()
            .unwrap(),
        &original
    ));
}

#[tokio::test]
async fn original_scope_drop_is_not_kept_alive_by_query_context() {
    let scope = TaskScope::new(Handle::current(), NonZeroUsize::new(1).unwrap());
    let context = OwnedDnsQueryTasks::new(&scope.registrar(), NonZeroUsize::new(1).unwrap());
    drop(scope);
    assert_eq!(
        context
            .spawn(async {}, CancellationToken::new())
            .unwrap_err()
            .kind(),
        io::ErrorKind::NotConnected
    );
    assert!(context.0.state.lock().unwrap().records.is_empty());
    assert!(OwnedDnsQueryTasks::current_witness().is_err());
}

#[tokio::test]
async fn coordinator_registers_only_its_original_coalesced_leader_in_the_same_host_scope() {
    use crate::coordinator::{QueryCoordinator, QueryKey, QueryScope};
    let mut scope = TaskScope::new(Handle::current(), NonZeroUsize::new(1).unwrap());
    let context = OwnedDnsQueryTasks::new(&scope.registrar(), NonZeroUsize::new(1).unwrap());
    let coordinator =
        QueryCoordinator::<u8>::with_graph_and_tasks(Default::default(), Some(context.clone()));
    let key = QueryKey::new(
        "owned.example",
        1,
        QueryScope::new(crate::DnsQueryRole::Default, 0),
    );
    let (entered, entering) = oneshot::channel();
    let (finish, finishing) = oneshot::channel();
    let first = {
        let coordinator = coordinator.clone();
        let key = key.clone();
        tokio::spawn(async move {
            coordinator
                .resolve(key, async move {
                    entered
                        .send(OwnedDnsQueryTasks::current_witness().unwrap())
                        .unwrap();
                    finishing.await.unwrap();
                    Ok(7)
                })
                .await
        })
    };
    let witness = entering.await.unwrap();
    let second = coordinator.resolve(key, async { panic!("follower ran another backend") });
    tokio::pin!(second);
    std::future::poll_fn(|cx| {
        assert!(
            second.as_mut().poll(cx).is_pending(),
            "follower is actually attached while the original backend is pending"
        );
        std::task::Poll::Ready(())
    })
    .await;
    finish.send(()).unwrap();
    assert_eq!(first.await.unwrap().unwrap(), 7);
    assert_eq!(second.await.unwrap(), 7);
    assert_eq!(context.0.state.lock().unwrap().records.len(), 1);
    assert!(!witness.is_joined());
    let actual = scope.next_exit().await.unwrap();
    assert_eq!(witness.try_joined().unwrap().id(), actual.id);
    coordinator
        .shutdown(Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
}
