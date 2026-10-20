use super::*;
use crate::contract::{lifecycle::TaskClass, scope::TaskScope};
fn graph_bound(
    tasks: usize,
) -> (
    TaskScope,
    ControlledTcpListenerOwner,
    ControlledTcpListener,
    SocketAddr,
) {
    let mut scope =
        TaskScope::suspended(tokio::runtime::Handle::current(), NonZeroUsize::new(tasks));
    let mut owner = ControlledTcpListenerOwner::new_with_accepted_graph(
        &declaration(),
        one(),
        CancellationToken::new(),
        one(),
        NonZeroUsize::new(tasks).unwrap(),
        &scope.registrar(),
    )
    .unwrap();
    let view = owner.bind_listener("127.0.0.1:0".parse().unwrap()).unwrap();
    let address = view.local_addr().unwrap();
    scope.enable_registration().unwrap();
    (scope, owner, view, address)
}
async fn graph_pair(
    view: &mut ControlledTcpListener,
    address: SocketAddr,
) -> (TcpStream, AcceptedTcp) {
    let (peer, accepted) = tokio::join!(TcpStream::connect(address), view.accept_owned());
    let (accepted, remote) = accepted.unwrap();
    let peer = peer.unwrap();
    assert_eq!(peer.local_addr().unwrap(), remote);
    (peer, accepted)
}
fn io_error(source: io::Error) -> ChannelError {
    ChannelError::new(Stage::Receive, Reason::Io, Impact::Session).with_source(source)
}
#[tokio::test]
async fn real_pending_handler_body_exit_is_not_scope_join_or_original_native_close() {
    timeout(Duration::from_secs(5), async {
        let (mut scope, mut owner, mut listener, address) = graph_bound(1);
        let (mut peer, accepted) = graph_pair(&mut listener, address).await;
        let claim = accepted.claim();
        let entered = Arc::new(tokio::sync::Notify::new());
        let entering = entered.clone();
        let (sent, context) = tokio::sync::oneshot::channel();
        let (id, witness) = accepted
            .spawn_handler(
                &scope.registrar(),
                TaskClass::Auxiliary,
                move |mut socket, tasks| async move {
                    assert!(tasks.current_task().is_some());
                    sent.send(tasks).ok().unwrap();
                    let mut byte = [0];
                    std::future::poll_fn(|cx| {
                        let mut buffer = tokio::io::ReadBuf::new(&mut byte);
                        let result =
                            tokio::io::AsyncRead::poll_read(Pin::new(&mut socket), cx, &mut buffer);
                        if result.is_pending() {
                            entering.notify_one();
                        }
                        result
                    })
                    .await
                    .map_err(io_error)?;
                    Ok(())
                },
            )
            .unwrap();
        entered.notified().await;
        let tasks = context.await.unwrap();
        assert_eq!(tasks.current_task().unwrap().0, id);
        assert!(!witness.is_joined());
        assert!(owner
            .close_accepted_graph(&claim, Instant::now() + Duration::from_millis(10))
            .await
            .unwrap()
            .is_none());
        assert!(!witness.is_joined());
        assert_eq!(owner.accepted_evidence().unwrap().retained, 1);
        assert!(owner.take_accepted_report().closed_io.is_empty());
        assert_eq!(
            peer.try_read(&mut [0]).unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        let actual = observe_exit(&mut scope, &owner).await;
        assert_eq!(actual.id, id);
        assert!(actual.result.is_ok());
        assert!(witness.is_joined());
        assert_eq!(
            peer.try_read(&mut [0]).unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        let closed = owner
            .close_accepted_graph(&claim, deadline())
            .await
            .unwrap()
            .unwrap();
        assert!(closed.matches(&claim));
        assert_eq!(peer.read(&mut [0]).await.unwrap(), 0);
        assert_eq!(
            owner.accepted_evidence().unwrap().users,
            AcceptedTcpUsers::Confirmed
        );
        assert_eq!(owner.accepted_evidence().unwrap().retained, 0);
        assert!(tasks
            .spawn(TaskClass::Auxiliary, |_| async { Ok(()) })
            .is_err());
        assert!(owner.wait_closed(deadline()).await.unwrap().is_complete());
        assert!(owner.accepted_evidence().unwrap().provider_confirmed());
        assert!(scope.shutdown(deadline()).await.is_complete());
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn actual_healthy_graph_reuses_one_slot_for_sixty_five_real_connections() {
    timeout(Duration::from_secs(10), async {
        let (mut scope, mut owner, mut listener, address) = graph_bound(1);
        for sequence in 0u8..65 {
            let (mut peer, accepted) = graph_pair(&mut listener, address).await;
            let claim = accepted.claim();
            let (id, witness) = accepted
                .spawn_handler(
                    &scope.registrar(),
                    TaskClass::Auxiliary,
                    |mut socket, _| async move {
                        let mut byte = [0];
                        socket.read_exact(&mut byte).await.map_err(io_error)?;
                        socket.write_all(&byte).await.map_err(io_error)?;
                        Ok(())
                    },
                )
                .unwrap();
            peer.write_all(&[sequence]).await.unwrap();
            let mut byte = [0];
            peer.read_exact(&mut byte).await.unwrap();
            assert_eq!(byte, [sequence]);
            let actual = observe_exit(&mut scope, &owner).await;
            assert_eq!(actual.id, id);
            assert!(actual.result.is_ok());
            assert!(witness.is_joined());
            assert_eq!(owner.accepted_evidence().unwrap().retained, 1);
            assert!(owner
                .close_accepted_graph(&claim, deadline())
                .await
                .unwrap()
                .unwrap()
                .matches(&claim));
            assert_eq!(peer.read(&mut byte).await.unwrap(), 0);
        }
        assert_eq!(
            owner.accepted_evidence().unwrap().confirmed_native_closes,
            65
        );
        assert_eq!(owner.accepted_evidence().unwrap().retained, 0);
        assert!(owner.wait_closed(deadline()).await.unwrap().is_complete());
        assert!(owner.accepted_evidence().unwrap().provider_confirmed());
        assert!(scope.shutdown(deadline()).await.is_complete());
    })
    .await
    .unwrap();
}
#[path = "graph/descendants.rs"]
mod descendants;

#[path = "graph/receiver.rs"]
mod receiver;

async fn observe_exit(
    scope: &mut TaskScope,
    owner: &ControlledTcpListenerOwner,
) -> crate::contract::scope::TaskExit {
    let exit = scope.next_exit().await.unwrap();
    assert!(owner
        .accepted_task_receiver()
        .unwrap()
        .observe_joined(exit.id)
        .is_some());
    exit
}
