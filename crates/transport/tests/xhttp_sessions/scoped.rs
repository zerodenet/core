//! Session expiration/cancellation uses an actual installed accepted task graph.
use super::*;
use std::{future::Future, num::NonZeroUsize, task::Poll};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zero_core::{request::Destination, Address};
use zero_platform_tokio::{
    contract::{
        component::{AcceptedTcpLimits, ResourceDeclaration},
        scope::{TaskClass, TaskScope},
    },
    ControlledTcpListenerOwner,
};
fn n(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap()
}

#[tokio::test(start_paused = true)]
async fn original_session_task_expires_or_cancels_and_joins_before_native_receipts() {
    for expire in [true, false] {
        // Explicit native fixture; no synthetic AcceptedCaller or successful
        // provider receipt is manufactured. Both originals stay retained.
        let mut scope = TaskScope::suspended(tokio::runtime::Handle::current(), Some(n(4)));
        let cancel = scope.registrar().cancellation();
        let declaration = ResourceDeclaration::TcpListener {
            destination: Destination::new(Address::Ipv4([127, 0, 0, 1]), 0),
            max_bind_attempts: n(1),
            accepted: Some(AcceptedTcpLimits {
                max_sockets: n(1),
                max_tasks_per_socket: n(4),
                max_resources_per_socket: 1,
            }),
        };
        let mut original = ControlledTcpListenerOwner::new_with_accepted_graph(
            &declaration,
            n(1),
            cancel,
            n(1),
            n(4),
            &scope.registrar(),
        )
        .unwrap();
        let mut listener = original
            .bind_listener("127.0.0.1:0".parse().unwrap())
            .unwrap();
        let receiver = original.accepted_task_receiver().unwrap();
        scope.enable_registration().unwrap();
        let (peer, accepted) = tokio::join!(
            tokio::net::TcpStream::connect(listener.local_addr().unwrap()),
            listener.accept_owned()
        );
        let mut peer = peer.unwrap();
        let (accepted, _) = accepted.unwrap();
        let claim = accepted.claim();
        let (ready, waiting) = tokio::sync::oneshot::channel();
        let (done, finished) = tokio::sync::oneshot::channel();
        let (primary_id, primary) = accepted
            .spawn_handler_bound(TaskClass::Auxiliary, move |mut socket, tasks| async move {
                let caller = tasks.caller()?;
                let mut byte = [0];
                {
                    let mut read = std::pin::pin!(socket.read_exact(&mut byte));
                    std::future::poll_fn(|cx| {
                        assert!(
                            read.as_mut().poll(cx).is_pending(),
                            "real original TCP read suspends before cancellation"
                        );
                        Poll::Ready(())
                    })
                    .await;
                    ready.send(caller).ok().unwrap();
                    read.await.map_err(|source| {
                        zero_platform_tokio::contract::ChannelError::new(
                            zero_platform_tokio::contract::Stage::Receive,
                            zero_platform_tokio::contract::Reason::Io,
                            zero_platform_tokio::contract::Impact::Session,
                        )
                        .with_source(source)
                    })?;
                }
                assert_eq!(byte, [b'q']);
                done.send(()).unwrap();
                Ok(())
            })
            .unwrap();
        let caller = super::super::super::body::Caller(waiting.await.unwrap());
        let sessions = Sessions::default();
        let (source, mut incoming) = tokio::sync::mpsc::channel(1);
        let session = sessions
            .get_scoped(
                "pending",
                2,
                &caller,
                Source::Tcp {
                    _original: source.clone(),
                },
            )
            .unwrap();
        session
            .push(0, Bytes::from_static(b"retained"), false)
            .await
            .unwrap();
        // Existing-session lookup shares the exact original and adds no worker.
        let reused = sessions
            .get_scoped(
                "pending",
                2,
                &caller,
                Source::Tcp {
                    _original: source.clone(),
                },
            )
            .unwrap();
        assert!(Arc::ptr_eq(&session, &reused));
        drop(reused);
        drop(source);
        assert!(
            !incoming.is_closed(),
            "the actual worker retains its incoming publisher"
        );
        let mut joined = 0;
        if expire {
            // The deadline is captured at creation, even if first poll is late.
            tokio::time::advance(Duration::from_secs(31)).await;
            let exit = scope.next_exit().await.unwrap();
            assert_ne!(exit.id, primary_id);
            assert!(exit.result.is_ok());
            assert!(receiver.observe_joined(exit.id).unwrap().result().is_ok());
            joined += 1;
            assert!(sessions.0.lock().unwrap().is_empty());
            assert!(session.life.error().is_err());
            assert!(incoming.recv().await.is_none());
            assert!(!primary.is_joined());
            assert!(
                original.take_accepted_report().closed_io.is_empty(),
                "logical expiration is no native close receipt"
            );
        }
        peer.write_all(b"q").await.unwrap();
        finished.await.unwrap();
        original.revoke();
        scope.begin_shutdown();
        while let Some(exit) = scope.next_exit().await {
            assert!(exit.result.is_ok());
            assert!(receiver.observe_joined(exit.id).unwrap().result().is_ok());
            joined += 1;
        }
        assert_eq!(
            joined, 2,
            "exactly one primary and one original session worker joined"
        );
        assert!(primary.is_joined());
        assert!(sessions.0.lock().unwrap().is_empty());
        assert!(session.life.error().is_err());
        assert!(incoming.recv().await.is_none());
        assert!(caller.0.check_open().is_err());
        let end = tokio::time::Instant::now() + Duration::from_secs(3);
        assert!(receiver
            .close(&claim, end)
            .await
            .unwrap()
            .unwrap()
            .matches(&claim));
        assert_eq!(peer.read(&mut [0]).await.unwrap(), 0);
        drop(listener);
        assert!(original.wait_closed(end).await.unwrap().is_complete());
        assert!(original.accepted_evidence().unwrap().provider_confirmed());
        drop(session);
        assert_eq!(sessions.1.available_permits(), 64 * 1024 * 1024);
    }
}
