use super::*;
#[tokio::test]
async fn actual_rejected_client_retains_original_join_source_and_same_listener_accepts_again() {
    timeout(Duration::from_secs(5), async {
        let (mut scope, mut owner, mut listener, address) = graph_bound(1);
        let receiver = owner.accepted_task_receiver().unwrap();
        let (mut rejected, accepted) = graph_pair(&mut listener, address).await;
        let claim = accepted.claim();
        let (id, witness) = accepted
            .spawn_handler(
                &scope.registrar(),
                TaskClass::Auxiliary,
                |mut socket, _| async move {
                    let mut request = [0];
                    socket.read_exact(&mut request).await.map_err(io_error)?;
                    assert_eq!(request, [b'!']);
                    Err(io_error(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "original client rejection",
                    )))
                },
            )
            .unwrap();
        rejected.write_all(b"!").await.unwrap();
        let exit = scope.next_exit().await.unwrap();
        assert_eq!(exit.id, id);
        assert!(exit.result.is_err());
        let original = witness.try_joined().unwrap().result().unwrap_err();
        let delivered = receiver.observe_joined(id).unwrap();
        assert_eq!(delivered.id(), id);
        assert_eq!(delivered.class(), TaskClass::Auxiliary);
        assert!(Arc::ptr_eq(&original, &delivered.result().unwrap_err()));
        assert!(owner
            .close_accepted_graph(&claim, deadline())
            .await
            .unwrap()
            .unwrap()
            .matches(&claim));
        assert_eq!(rejected.read(&mut [0]).await.unwrap(), 0);
        // A rejected session is not the listener's native acquisition/close fault.
        assert!(owner.accepted_evidence().unwrap().first_failure.is_none());
        let (mut peer, accepted) = graph_pair(&mut listener, address).await;
        let claim = accepted.claim();
        let (id, witness) = accepted
            .spawn_handler(
                &scope.registrar(),
                TaskClass::Auxiliary,
                |mut socket, _| async move {
                    let mut request = [0];
                    socket.read_exact(&mut request).await.map_err(io_error)?;
                    socket.write_all(&request).await.map_err(io_error)?;
                    Ok(())
                },
            )
            .unwrap();
        peer.write_all(b"a").await.unwrap();
        let mut byte = [0];
        peer.read_exact(&mut byte).await.unwrap();
        assert_eq!(byte, [b'a']);
        assert_eq!(scope.next_exit().await.unwrap().id, id);
        assert!(witness.is_joined());
        assert!(receiver.observe_joined(id).unwrap().result().is_ok());
        assert!(owner
            .close_accepted_graph(&claim, deadline())
            .await
            .unwrap()
            .is_some());
        let saved = owner
            .accepted_evidence()
            .unwrap()
            .first_user_failure
            .unwrap();
        assert_eq!(saved.id(), delivered.id());
        assert_eq!(saved.class(), delivered.class());
        assert!(Arc::ptr_eq(&original, &saved.result().unwrap_err()));
        assert!(owner.wait_closed(deadline()).await.unwrap().is_complete());
        assert!(scope.shutdown(deadline()).await.is_complete());
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn same_connection_reuses_its_finite_derived_task_slots_after_real_join() {
    timeout(Duration::from_secs(5), async {
        let (mut scope, mut owner, mut listener, address) = graph_bound(2);
        let receiver = owner.accepted_task_receiver().unwrap();
        let (_peer, accepted) = graph_pair(&mut listener, address).await;
        let claim = accepted.claim();
        let (send, receive) = tokio::sync::oneshot::channel();
        let (_, primary) = accepted
            .spawn_handler(
                &scope.registrar(),
                TaskClass::Auxiliary,
                |socket, tasks| async move {
                    let caller = tasks.caller().unwrap();
                    caller.check_open().unwrap();
                    send.send((tasks, caller)).ok().unwrap();
                    std::future::pending::<()>().await;
                    drop(socket);
                    Ok(())
                },
            )
            .unwrap();
        let (tasks, caller) = receive.await.unwrap();
        assert!(caller.scope_identity().matches(&scope.registrar()));
        let caller_witness = caller.task_join_witness().unwrap();
        assert!(!caller_witness.is_joined());
        for _ in 0..65 {
            let (id, witness) = tasks
                .spawn(TaskClass::Auxiliary, |_| async { Ok(()) })
                .unwrap();
            assert_eq!(scope.next_exit().await.unwrap().id, id);
            assert!(witness.is_joined());
            assert!(receiver.observe_joined(id).unwrap().result().is_ok());
            caller.check_open().unwrap();
        }
        assert!(owner
            .close_accepted_graph(&claim, Instant::now() + Duration::from_millis(10))
            .await
            .unwrap()
            .is_none());
        assert!(caller.check_open().is_err());
        scope.next_exit().await.unwrap();
        assert!(primary.is_joined() && caller_witness.is_joined());
        assert!(owner
            .close_accepted_graph(&claim, deadline())
            .await
            .unwrap()
            .is_some());
        assert!(owner.wait_closed(deadline()).await.unwrap().is_complete());
        assert!(scope.shutdown(deadline()).await.is_complete());
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn joined_caller_is_rejected_before_graph_receiver_reaps_its_node() {
    let (mut scope, mut owner, mut listener, address) = graph_bound(1);
    let (mut peer, accepted) = graph_pair(&mut listener, address).await;
    let claim = accepted.claim();
    let (send, receive) = tokio::sync::oneshot::channel();
    let (finish, finished) = tokio::sync::oneshot::channel();
    let (id, witness) = accepted
        .spawn_handler(
            &scope.registrar(),
            TaskClass::Auxiliary,
            |socket, tasks| async move {
                send.send(tasks.caller().unwrap()).ok().unwrap();
                finished.await.unwrap();
                drop(socket);
                Ok(())
            },
        )
        .unwrap();
    let caller = receive.await.unwrap();
    caller.check_open().unwrap();
    finish.send(()).unwrap();
    assert_eq!(scope.next_exit().await.unwrap().id, id);
    assert!(witness.is_joined());
    assert!(caller.scope_identity().registration_open());
    assert!(caller.check_open().is_err());
    assert_eq!(owner.accepted_evidence().unwrap().retained, 1);
    assert_eq!(
        peer.try_read(&mut [0]).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    assert!(owner
        .close_accepted_graph(&claim, deadline())
        .await
        .unwrap()
        .is_some());
    assert_eq!(peer.read(&mut [0]).await.unwrap(), 0);
    assert!(owner.wait_closed(deadline()).await.unwrap().is_complete());
    assert!(scope.shutdown(deadline()).await.is_complete());
}
