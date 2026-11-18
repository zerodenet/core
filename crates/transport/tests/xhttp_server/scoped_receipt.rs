//! HTTP receipts return request credit; received packets keep actual Scope debt.
use super::*;
use std::{future::Future, num::NonZeroUsize, task::Poll};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zero_platform_tokio::{
    contract::{
        component::{AcceptedTcpLimits, ResourceDeclaration},
        scope::{TaskClass, TaskScope},
    },
    ControlledTcpListenerOwner,
};

#[tokio::test]
async fn scoped_http1_receipt_frees_request_credit_with_bounded_pending_original_tasks() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let one = NonZeroUsize::MIN;
        let four = NonZeroUsize::new(4).unwrap();
        let mut scope = TaskScope::suspended(tokio::runtime::Handle::current(), Some(four));
        let declaration = ResourceDeclaration::TcpListener {
            destination: zero_core::request::Destination::new(
                zero_core::Address::Ipv4([127, 0, 0, 1]),
                0,
            ),
            max_bind_attempts: one,
            accepted: Some(AcceptedTcpLimits {
                max_sockets: one,
                max_tasks_per_socket: four,
                max_resources_per_socket: 1,
            }),
        };
        let mut original = ControlledTcpListenerOwner::new_with_accepted_graph(
            &declaration,
            one,
            scope.registrar().cancellation(),
            one,
            four,
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
        let (ready, pending) = tokio::sync::oneshot::channel();
        let (done, finished) = tokio::sync::oneshot::channel();
        let (_, primary) = accepted
            .spawn_handler_bound(TaskClass::Auxiliary, move |mut io, tasks| async move {
                let caller = tasks.caller()?;
                let mut byte = [0];
                let read = io.read_exact(&mut byte);
                tokio::pin!(read);
                std::future::poll_fn(|cx| {
                    assert!(read.as_mut().poll(cx).is_pending());
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
                done.send(()).unwrap();
                Ok(())
            })
            .unwrap();
        let caller = super::super::super::body::Caller(pending.await.unwrap());
        let sessions = super::super::super::sessions::Sessions::default();
        let (accepted, _incoming) = mpsc::channel(1);
        let session = sessions
            .get_scoped(
                "receipt",
                1,
                &caller,
                super::super::super::sessions::Source::Tcp {
                    _original: accepted.clone(),
                },
            )
            .unwrap();
        session
            .push(0, Bytes::from_static(b"first"), false)
            .await
            .unwrap();
        let permits = Arc::new(tokio::sync::Semaphore::new(1));
        let profile = Profile::new(&crate::profile::OwnedSplitHttpProfile {
            host: Some("example.com".into()),
            path: "/tunnel".into(),
            mode: "packet-up".into(),
            options: Default::default(),
        });
        for sequence in 1..=3 {
            let mut request = profile
                .packet_request("receipt", sequence, b"next")
                .unwrap();
            request.extensions_mut().insert(caller.clone());
            let response = upload(
                request,
                profile.clone(),
                session.clone(),
                sequence,
                permits
                    .clone()
                    .try_acquire_owned()
                    .expect("receipt returned HTTP credit"),
                accepted.clone(),
            )
            .await
            .unwrap();
            assert_eq!(response.status(), if sequence <= 2 { 200 } else { 500 });
            assert_eq!(permits.available_permits(), 1);
        }
        assert!(!primary.is_joined());
        assert!(original.take_accepted_report().closed_io.is_empty());
        assert_eq!(session.next().await.unwrap().unwrap().data, "first");
        for _ in 0..2 {
            assert_eq!(session.next().await.unwrap().unwrap().data, "next");
        }
        session.life.close();
        peer.write_all(b"q").await.unwrap();
        finished.await.unwrap();
        original.revoke();
        scope.begin_shutdown();
        let mut joined = 0;
        while let Some(exit) = scope.next_exit().await {
            assert!(exit.result.is_ok());
            assert!(receiver.observe_joined(exit.id).unwrap().result().is_ok());
            joined += 1;
        }
        assert_eq!(
            joined, 4,
            "primary, session maintenance and two admitted packets"
        );
        assert!(primary.is_joined());
        let end = tokio::time::Instant::now() + Duration::from_secs(2);
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
        let restored: Vec<_> = (0..4)
            .map(|_| session.reserve_upload(16 * 1024 * 1024).unwrap())
            .collect();
        assert!(session.reserve_upload(1).is_err());
        drop(restored);
    })
    .await
    .unwrap();
}
