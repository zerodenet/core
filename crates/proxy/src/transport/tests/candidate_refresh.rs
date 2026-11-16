//! One actual accepted task polls the DNS continuation; no private task set.
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
fn source() -> CandidateRefresh {
    CandidateRefresh(DirectCandidate {
        peer: "127.0.0.1:443".parse().unwrap(),
        generation: NetworkGeneration(1),
    })
}
#[tokio::test]
async fn actual_caller_retains_pending_refresh_io_and_the_same_native_close_receipt() {
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
        let (id, witness) = accepted
            .spawn_handler_bound(TaskClass::Auxiliary, move |mut io, tasks| async move {
                tasks.caller()?.check_open()?;
                let original_task = tokio::task::id();
                let work = Box::pin(async move {
                    assert_eq!(
                        tokio::task::id(),
                        original_task,
                        "the original registered task drives the DNS continuation"
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
                    Ok(vec!["127.0.0.1:443".parse().unwrap()])
                });
                let result = LegacyCandidateOpener
                    .refresh(work, source())
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(result, ["127.0.0.1:443".parse::<SocketAddr>().unwrap()]);
                Ok(())
            })
            .unwrap();
        pending.await.unwrap();
        assert!(!witness.is_joined());
        peer.write_all(b"x").await.unwrap();
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
async fn inline_refresh_retains_original_panic_payload_instead_of_a_synthetic_task_result() {
    let payload = Arc::new(String::from("original resolver panic"));
    let original = payload.clone();
    let work = Box::pin(async move {
        std::panic::panic_any(original);
        #[allow(unreachable_code)]
        Ok(Vec::new())
    });
    let failure = LegacyCandidateOpener
        .refresh(work, source())
        .await
        .unwrap_err();
    let panic = failure
        .get_ref()
        .unwrap()
        .downcast_ref::<RefreshPanic>()
        .unwrap();
    let held = panic.0.lock().unwrap();
    assert!(Arc::ptr_eq(
        held.downcast_ref::<Arc<String>>().unwrap(),
        &payload
    ));
}
