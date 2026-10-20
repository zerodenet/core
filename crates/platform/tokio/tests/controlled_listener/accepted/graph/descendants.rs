use super::*;
#[tokio::test]
async fn derived_original_scope_join_and_escaped_byte_view_are_independent_obligations() {
    timeout(Duration::from_secs(5), async {
        let (mut scope, mut owner, mut listener, address) = graph_bound(2);
        let (mut peer, accepted) = graph_pair(&mut listener, address).await;
        let claim = accepted.claim();
        let (send, receive) = tokio::sync::oneshot::channel();
        let (primary, primary_join) = accepted
            .spawn_handler(
                &scope.registrar(),
                TaskClass::Auxiliary,
                move |socket, tasks| async move {
                    let child = tasks.spawn(TaskClass::Auxiliary, move |child| async move {
                        let _socket = socket;
                        send.send(child).ok().unwrap();
                        std::future::pending::<()>().await;
                        Ok(())
                    })?;
                    assert_ne!(tasks.current_task().unwrap().0, child.0);
                    Ok(())
                },
            )
            .unwrap();
        let child = receive.await.unwrap();
        let child_id = child.current_task().unwrap().0;
        assert_ne!(child_id, primary);
        let exit = scope.next_exit().await.unwrap();
        assert_eq!(exit.id, primary);
        assert!(primary_join.is_joined());
        assert!(owner
            .close_accepted_graph(&claim, Instant::now() + Duration::from_millis(10))
            .await
            .unwrap()
            .is_none());
        assert!(!child.current_task().unwrap().1.is_joined());
        assert_eq!(owner.accepted_evidence().unwrap().retained, 1);
        assert_eq!(
            peer.try_read(&mut [0]).unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        let exit = scope.next_exit().await.unwrap();
        assert_eq!(exit.id, child_id);
        assert!(owner
            .close_accepted_graph(&claim, deadline())
            .await
            .unwrap()
            .unwrap()
            .matches(&claim));
        assert_eq!(peer.read(&mut [0]).await.unwrap(), 0);
        assert!(owner.wait_closed(deadline()).await.unwrap().is_complete());
        scope.shutdown(deadline()).await;
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn foreign_scope_and_explicit_task_ceiling_reject_without_forging_task_acceptance() {
    let (mut scope, mut owner, mut listener, address) = graph_bound(1);
    let mut foreign = TaskScope::new(tokio::runtime::Handle::current(), one());
    let (mut peer, accepted) = graph_pair(&mut listener, address).await;
    let claim = accepted.claim();
    assert!(accepted
        .spawn_handler(&foreign.registrar(), TaskClass::Auxiliary, |_, _| async {
            Ok(())
        })
        .is_err());
    assert!(owner
        .close_accepted_graph(&claim, deadline())
        .await
        .unwrap()
        .unwrap()
        .matches(&claim));
    assert_eq!(peer.read(&mut [0]).await.unwrap(), 0);
    let (mut peer, accepted) = graph_pair(&mut listener, address).await;
    let claim = accepted.claim();
    let (send, receive) = tokio::sync::oneshot::channel();
    accepted
        .spawn_handler(
            &scope.registrar(),
            TaskClass::Auxiliary,
            move |_socket, tasks| async move {
                let failed = tasks
                    .spawn(TaskClass::Auxiliary, |_| async { Ok(()) })
                    .err()
                    .unwrap();
                send.send(failed.reason).unwrap();
                Ok(())
            },
        )
        .unwrap();
    assert_eq!(receive.await.unwrap(), Reason::Capacity);
    scope.next_exit().await.unwrap();
    assert!(owner
        .close_accepted_graph(&claim, deadline())
        .await
        .unwrap()
        .is_some());
    assert_eq!(peer.read(&mut [0]).await.unwrap(), 0);
    assert!(owner.wait_closed(deadline()).await.unwrap().is_complete());
    scope.shutdown(deadline()).await;
    foreign.shutdown(deadline()).await;
}
