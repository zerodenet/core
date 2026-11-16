//! Original registered caller drives pending group work and real native close.
use super::*;
use std::{future::Future, num::NonZeroUsize, task::Poll};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zero_platform_tokio::contract::{
    component::{AcceptedTcpLimits, ResourceDeclaration},
    scope::{TaskClass, TaskScope},
    CancellationToken,
};
fn n(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap()
}
#[tokio::test]
async fn original_caller_completes_group_io_with_its_native_receipt() {
    original_io(false).await;
}
#[tokio::test]
async fn cancelled_group_wait_returns_to_the_same_caller_and_original_native_owner() {
    original_io(true).await;
}
async fn original_io(cancel: bool) {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let mut scope = TaskScope::suspended(tokio::runtime::Handle::current(), Some(n(1)));
        let declaration = ResourceDeclaration::TcpListener {
            destination: zero_core::request::Destination::new(
                zero_core::Address::Ipv4([127, 0, 0, 1]),
                0,
            ),
            max_bind_attempts: n(1),
            accepted: Some(AcceptedTcpLimits {
                max_sockets: n(1),
                max_tasks_per_socket: n(1),
                max_resources_per_socket: 1,
            }),
        };
        let mut original =
            zero_platform_tokio::ControlledTcpListenerOwner::new_with_accepted_graph(
                &declaration,
                n(1),
                CancellationToken::new(),
                n(1),
                n(1),
                &scope.registrar(),
            )
            .unwrap();
        let mut listener = original
            .bind_listener("127.0.0.1:0".parse().unwrap())
            .unwrap();
        scope.enable_registration().unwrap();
        let (peer, accepted) = tokio::join!(
            tokio::net::TcpStream::connect(listener.local_addr().unwrap()),
            listener.accept_owned()
        );
        let mut peer = peer.unwrap();
        let (accepted, _) = accepted.unwrap();
        let claim = accepted.claim();
        let receiver = original.accepted_task_receiver().unwrap();
        let (ready, pending) = tokio::sync::oneshot::channel();
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let (id, witness) = accepted
            .spawn_handler_bound(TaskClass::Auxiliary, move |mut io, tasks| async move {
                tasks.caller()?.check_open()?;
                let original_task = tokio::task::id();
                let work = Box::pin(async move {
                    assert_eq!(
                        tokio::task::id(),
                        original_task,
                        "the original registered task drives the group continuation"
                    );
                    let mut byte = [0];
                    let mut read = std::pin::pin!(io.read_exact(&mut byte));
                    std::future::poll_fn(|cx| {
                        assert!(read.as_mut().poll(cx).is_pending());
                        Poll::Ready(())
                    })
                    .await;
                    ready.send(()).unwrap();
                    read.await?;
                    Ok::<_,std::io::Error>(())
                });
                let mut schedules = Schedules::default();
                schedules.push(async move { work.await.map_err(EngineError::Io)?; Ok(()) });
                if cancel {
                    let mut next = Box::pin(schedules.join_next());
                    tokio::select! { biased; _=stopped=>{}, result=&mut next=>panic!("actual read completed before cancellation: {result:?}") }
                    drop(next);
                    schedules.clear();
                    assert!(schedules.is_empty());
                } else {
                    schedules.join_next().await.unwrap().unwrap().unwrap();
                }
                Ok(())
            })
            .unwrap();
        pending.await.unwrap();
        assert!(!witness.is_joined());
        if cancel { stop.send(()).unwrap(); } else { peer.write_all(b"x").await.unwrap(); }
        let exit = scope.next_exit().await.unwrap();
        assert_eq!(exit.id, id);
        assert!(exit.result.is_ok());
        assert!(receiver.observe_joined(id).unwrap().result().is_ok());
        assert!(witness.is_joined());
        let end = tokio::time::Instant::now() + std::time::Duration::from_secs(3);
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
        assert!(scope.shutdown(end).await.is_complete());
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn original_group_panic_payload_survives_without_a_synthetic_task_result() {
    let payload = std::sync::Arc::new(String::from("original group panic"));
    let original = payload.clone();
    let mut schedules = Schedules::default();
    schedules.push(async move {
        std::panic::panic_any(original);
        #[allow(unreachable_code)]
        Ok(())
    });
    let failure = schedules.join_next().await.unwrap().unwrap_err();
    let panic = failure
        .get_ref()
        .unwrap()
        .downcast_ref::<OriginalPanic>()
        .unwrap();
    let held = panic.0.lock().unwrap();
    assert!(std::sync::Arc::ptr_eq(
        held.downcast_ref::<std::sync::Arc<String>>().unwrap(),
        &payload
    ));
}
