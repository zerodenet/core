//! Protocol cleanup callbacks follow an actual retained original's initializer.
use super::*;
use crate::contract::component::ResourceDeclaration;

#[tokio::test]
async fn accepted_protocol_callback_requires_its_retained_original_initializer() {
    timeout(Duration::from_secs(5), async {
        let mut fixture = Fixture::cold(1).await;
        let mut native = TaskScope::new(tokio::runtime::Handle::current(), NonZeroUsize::MIN);
        let tasks = native.registrar();
        let owner = fixture.owner.registration();
        owner
            .bind_declared(
                &ResourceDeclaration::ControlledNodeCarrier {
                    endpoint: fixture.destination.clone(),
                    dial: fixture.transport.binding().dial.clone(),
                    max_sockets: NonZeroUsize::MIN,
                },
                &tasks,
                &Arc::new(CancellationToken::new()),
            )
            .unwrap();
        fixture.owner.start(deadline()).await.unwrap();
        let mut source = TaskScope::new(
            tokio::runtime::Handle::current(),
            NonZeroUsize::new(2).unwrap(),
        );
        let cancel = CancellationToken::new();
        let stop = cancel.clone();
        let (id, primary) = source
            .registrar()
            .spawn_tracked(TaskClass::Auxiliary, async move {
                stop.cancelled().await;
                Ok(())
            })
            .unwrap();
        let mut graph = crate::AcceptedPeerScope::for_carrier(
            &source.registrar().downgrade(),
            id,
            primary.clone(),
            cancel,
            NonZeroUsize::new(2).unwrap(),
            1,
            fixture.listener.local_addr().unwrap(),
        )
        .unwrap();
        let caller = graph.tasks().caller().unwrap();
        assert!(
            caller.current_protocol_caller().is_err(),
            "the test task cannot impersonate the primary"
        );
        let mut grant = fixture.grants.reserve().unwrap();
        let (view, _) = grant.claim_preparing_transferred_view().unwrap();
        let original = grant.adoption();
        let (begin, begun) = tokio::sync::oneshot::channel();
        let (checked, checked_unretained) = tokio::sync::oneshot::channel();
        let (pending, suspended) = tokio::sync::oneshot::channel();
        let actual = caller.clone();
        let transport = fixture.transport.clone();
        let destination = fixture.destination.clone();
        let (_, auxiliary) = original
            .spawn_auxiliary(&tasks, async move {
                assert!(
                    actual.current_protocol_caller().is_err(),
                    "an unretained original grants no cleanup lineage"
                );
                checked.send(()).unwrap();
                begun.await.unwrap();
                let caller = actual.current_protocol_caller().unwrap();
                assert_eq!(caller.native_task_id(), tokio::task::try_id());
                let carrier = transport
                    .prepare_controlled_stream(destination, context(), grant)?
                    .open(deadline())
                    .await?;
                let (mut io, _) = carrier.into_parts();
                let mut byte = [0];
                let read = io.read_exact(&mut byte);
                tokio::pin!(read);
                std::future::poll_fn(|cx| {
                    use std::future::Future;
                    assert!(read.as_mut().poll(cx).is_pending());
                    std::task::Poll::Ready(())
                })
                .await;
                pending.send(()).unwrap();
                read.await.unwrap();
                drop(view);
                Ok(())
            })
            .unwrap();
        // The auxiliary reaches its explicit pre-publication checkpoint first.
        checked_unretained.await.unwrap();
        caller.observe_pending_carrier(original.clone()).unwrap();
        begin.send(()).unwrap();
        let (mut peer, _) = fixture.listener.accept().await.unwrap();
        suspended.await.unwrap();
        let foreign = caller.clone();
        let (foreign_id, _) = source
            .registrar()
            .spawn_tracked(TaskClass::Auxiliary, async move {
                assert!(
                    foreign.current_protocol_caller().is_err(),
                    "sharing the Source Scope is insufficient"
                );
                Ok(())
            })
            .unwrap();
        let exit = source.next_exit().await.unwrap();
        assert_eq!(exit.id, foreign_id);
        assert!(exit.result.is_ok());
        assert!(!auxiliary.is_joined() && original.closed().is_none());
        peer.write_all(b"x").await.unwrap();
        let exit = native.next_exit().await.unwrap();
        assert!(exit.result.is_ok());
        assert!(auxiliary.is_joined());
        original.accept_auxiliary().await.unwrap();
        let report = original.retire(deadline()).await.unwrap();
        assert!(report.is_complete(), "{report:?}");
        assert!(original.closed().unwrap().matches(&original));
        assert!(original.users_closed().unwrap().matches(&original));
        graph.revoke(crate::contract::CloseReason::Cancelled);
        let exit = source.next_exit().await.unwrap();
        assert_eq!(exit.id, id);
        assert!(graph.observe_joined(id).is_some());
        assert!(graph.shutdown(deadline()).await.unwrap().is_complete());
        assert!(fixture
            .owner
            .shutdown(deadline())
            .await
            .unwrap()
            .is_complete());
        assert!(native.shutdown(deadline()).await.is_complete());
        assert!(source.shutdown(deadline()).await.is_complete());
    })
    .await
    .unwrap();
}
