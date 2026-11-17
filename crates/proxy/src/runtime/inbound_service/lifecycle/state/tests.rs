//! The same installed production Scope retains actual pending IO and errors.
//! These assertions prove task custody/ACK only, never a native close report.
use super::*;
use std::{future::Future, task::Poll};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use zero_platform_tokio::contract::{Impact, Reason, Stage};
fn ceiling() -> NonZeroUsize {
    NonZeroUsize::new(4).unwrap()
}

#[tokio::test]
async fn interrupted_service_cleanup_retains_original_error_and_pending_task_until_real_join_and_ack(
) {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let budget = TaskRegistrationBudget::new(ceiling());
        let mut state = State::default();
        state.install(ceiling(), budget.clone()).unwrap();
        let registrar = state.scope.as_ref().unwrap().registrar();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let connected = TcpStream::connect(listener.local_addr().unwrap());
        let (client, accepted) = tokio::join!(connected, listener.accept());
        let mut client = client.unwrap();
        let (mut peer, _) = accepted.unwrap();
        let (pending, observed) = tokio::sync::oneshot::channel();
        let (blocked, witness) = registrar
            .spawn_tracked(TaskClass::Auxiliary, async move {
                let mut byte = [0];
                let read = client.read_exact(&mut byte);
                tokio::pin!(read);
                let mut pending = Some(pending);
                std::future::poll_fn(|cx| match read.as_mut().poll(cx) {
                    Poll::Pending => {
                        if let Some(pending) = pending.take() {
                            pending.send(()).unwrap();
                        }
                        Poll::Pending
                    }
                    Poll::Ready(result) => Poll::Ready(result),
                })
                .await
                .map_err(|source| {
                    ChannelError::new(Stage::Receive, Reason::Io, Impact::Component)
                        .with_source(source)
                })?;
                Ok(())
            })
            .unwrap();
        observed.await.unwrap(); // The actual original TCP read is Pending.
        let (failed, failed_witness) = registrar
            .spawn_tracked(TaskClass::Critical, async {
                Err(
                    ChannelError::new(Stage::Close, Reason::Io, Impact::Component)
                        .with_source(std::io::Error::other("original Service failure")),
                )
            })
            .unwrap();
        // Consume that real failed exit through the actual owner, retaining its
        // exact Arc before ACK. Cancellation of the later close cannot erase it.
        let receipt = state.scope.as_mut().unwrap().next_receipt().await.unwrap();
        assert_eq!(receipt.id(), failed);
        let original = receipt.result().unwrap_err();
        assert!(state.receive(receipt).is_err());
        assert!(Arc::ptr_eq(
            &state.failures.get(&failed).unwrap().result().unwrap_err(),
            &original
        ));
        assert_eq!(budget.usage().charged, 1);
        let mut closing = Box::pin(state.close(Instant::now() + std::time::Duration::from_secs(2)));
        std::future::poll_fn(|cx| {
            assert!(closing.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        drop(closing);
        assert!(state.scope.is_some());
        assert!(!witness.is_joined());
        assert!(state.install(ceiling(), budget.clone()).is_err());
        assert!(Arc::ptr_eq(
            &state.failures.get(&failed).unwrap().result().unwrap_err(),
            &original
        ));
        peer.write_all(&[1]).await.unwrap();
        assert!(state
            .close(Instant::now() + std::time::Duration::from_secs(2))
            .await
            .is_err());
        assert!(state.scope.is_none());
        assert_eq!(witness.task_id(), Some(blocked));
        assert_eq!(failed_witness.task_id(), Some(failed));
        assert!(witness.is_joined() && failed_witness.is_joined());
        assert!(Arc::ptr_eq(
            &failed_witness.try_joined().unwrap().result().unwrap_err(),
            &original
        ));
        assert_eq!(budget.usage().charged, 0);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn expired_service_cleanup_keeps_the_same_scope_until_its_original_await_exits() {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let mut state = State::default();
        let budget = TaskRegistrationBudget::new(ceiling());
        state.install(ceiling(), budget.clone()).unwrap();
        let registrar = state.scope.as_ref().unwrap().registrar();
        let original_scope = registrar.retirement_observation();
        let (release, released) = tokio::sync::oneshot::channel::<()>();
        let (waiting, observed) = tokio::sync::oneshot::channel();
        let (_, witness) = registrar
            .spawn_tracked(TaskClass::Critical, async move {
                let mut released = Box::pin(released);
                let mut waiting = Some(waiting);
                std::future::poll_fn(|cx| match released.as_mut().poll(cx) {
                    Poll::Pending => {
                        if let Some(waiting) = waiting.take() {
                            waiting.send(()).unwrap();
                        }
                        Poll::Pending
                    }
                    Poll::Ready(result) => Poll::Ready(result),
                })
                .await
                .unwrap();
                Ok(())
            })
            .unwrap();
        observed.await.unwrap();
        let expired = state.close(Instant::now()).await.unwrap_err();
        let _original_operation_failure = state.fail_transition(expired);
        assert!(state.scope.is_some());
        assert!(!original_scope.is_drained() && !witness.is_joined());
        assert_eq!(budget.usage().charged, 1);
        release.send(()).unwrap();
        state
            .close(Instant::now() + std::time::Duration::from_secs(2))
            .await
            .unwrap();
        assert!(state.scope.is_none());
        assert!(
            state.scope_failure().is_ok(),
            "historical timeout is not an unclosed Scope"
        );
        assert!(
            state.retained_failure().is_err(),
            "the failed operation is still observable"
        );
        assert!(original_scope.is_drained() && witness.is_joined());
        assert_eq!(budget.usage().charged, 0);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn cold_service_task_refusal_preserves_the_actual_running_scope_and_its_original_grant() {
    let limit = NonZeroUsize::new(2).unwrap();
    let budget = TaskRegistrationBudget::new(limit);
    let mut state = State::default();
    state.install(limit, budget.clone()).unwrap();
    let registrar = state.scope.as_ref().unwrap().registrar();
    let actual_scope = registrar.scope_identity();
    let (release, released) = tokio::sync::oneshot::channel::<()>();
    let (_, witness) = registrar
        .spawn_tracked(TaskClass::Critical, async move {
            released.await.unwrap();
            Ok(())
        })
        .unwrap();
    assert!(State::prepare(limit, budget.clone(), 2).is_err());
    assert!(actual_scope.registration_open());
    assert!(actual_scope.same_scope(&state.scope.as_ref().unwrap().registrar().scope_identity()));
    assert!(!registrar.cancellation().is_cancelled());
    assert!(!witness.is_joined());
    assert_eq!(budget.usage().charged, 1);
    release.send(()).unwrap();
    state
        .close(Instant::now() + std::time::Duration::from_secs(2))
        .await
        .unwrap();
    assert!(witness.is_joined());
    assert_eq!(budget.usage().charged, 0);
}

#[tokio::test]
async fn original_accepted_wire_failure_is_retained_locally_and_does_not_stop_service_reconnect() {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let budget = TaskRegistrationBudget::new(ceiling());
        let mut state = State::default();
        state.install(ceiling(), budget.clone()).unwrap();
        let registrar = state.scope.as_ref().unwrap().registrar();
        let originals = zero_runtime::HostOriginalBudgets::default();
        let identity = zero_core::component::ComponentInstanceId {
            component: zero_core::component::ComponentId(601),
            generation: zero_core::component::ResourceGeneration(1),
        };
        let failures = zero_platform_tokio::AcceptedTaskFailures::prepare(
            identity,
            &registrar,
            &originals.packet.driver_metadata_view(),
        )
        .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let connected = TcpStream::connect(listener.local_addr().unwrap());
        let (client, accepted) = tokio::join!(connected, listener.accept());
        let mut client = client.unwrap();
        let (mut peer, _) = accepted.unwrap();
        let local = client.local_addr().unwrap();
        let (publish, installed) = tokio::sync::oneshot::channel();
        let (pending, observed) = tokio::sync::oneshot::channel();
        let mut liability = failures.reserve().unwrap();
        liability.begin_registration();
        let (task, witness) = registrar
            .spawn_tracked(TaskClass::Auxiliary, async move {
                let _installed: zero_platform_tokio::AcceptedPeerScope = installed.await.unwrap();
                let mut bytes = [0];
                let read = client.read_exact(&mut bytes);
                tokio::pin!(read);
                let mut pending = Some(pending);
                std::future::poll_fn(|cx| match read.as_mut().poll(cx) {
                    Poll::Pending => {
                        if let Some(pending) = pending.take() {
                            pending.send(()).unwrap();
                        }
                        Poll::Pending
                    }
                    Poll::Ready(result) => Poll::Ready(result),
                })
                .await
                .map_err(|error| {
                    ChannelError::new(Stage::Receive, Reason::Io, Impact::Session)
                        .with_source(error)
                })?;
                Ok(())
            })
            .unwrap();
        let original_fault = liability.registered(&witness).unwrap();
        let graph = zero_platform_tokio::AcceptedPeerScope::for_carrier(
            &registrar.downgrade(),
            task,
            witness.clone(),
            registrar.cancellation().child_token(),
            ceiling(),
            4,
            local,
        )
        .unwrap();
        graph
            .bind_task_failure_registration(&original_fault)
            .unwrap();
        state.retained.install_group(graph.clone(), witness);
        publish.send(graph).ok().unwrap();
        observed.await.unwrap();
        peer.shutdown().await.unwrap();
        let receipt = state.scope.as_mut().unwrap().next_receipt().await.unwrap();
        assert!(receipt.result().is_err());
        state.receive(receipt).unwrap();
        assert!(state.failures.is_empty() && !state.stopping);
        assert!(!registrar.cancellation().is_cancelled());
        let actual = failures.iter().next().unwrap();
        assert_eq!(actual.id(), task);
        assert!(actual.result().is_err());
        state
            .close(Instant::now() + std::time::Duration::from_secs(2))
            .await
            .unwrap();
        assert!(state.scope.is_none() && state.retained.is_empty());
        assert_eq!(budget.usage().charged, 0);
    })
    .await
    .unwrap();
}
