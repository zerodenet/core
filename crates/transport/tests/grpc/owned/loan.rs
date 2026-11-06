//! A cancelled logical borrower returns its actual workers without closing peers.
use super::*;
use std::{future::Future, task::Context};
use zero_platform_tokio::controlled_carrier::loan::StreamLoan;

#[tokio::test]
async fn real_stream_loan_cancellation_returns_its_workers_and_preserves_same_pool_peers() {
    timeout(Duration::from_secs(8), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let peer = listener.local_addr().unwrap();
        let (finish, finished) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (io, _) = listener.accept().await.unwrap();
            let incoming = crate::grpc::accept_grpc_connection(
                io,
                &crate::profile::OwnedGrpcProfile {
                    service_names: vec!["owned".into()],
                    ..Default::default()
                },
            )
            .unwrap();
            let mut workers = tokio::task::JoinSet::new();
            for _ in 0..3 {
                let mut stream = incoming.accept().await.unwrap();
                workers.spawn(async move {
                    let mut payload = [0; 1];
                    while let Ok(1) = stream.read(&mut payload).await {
                        stream.write_all(&payload).await.unwrap();
                        stream.flush().await.unwrap();
                    }
                });
            }
            finished.await.unwrap();
            incoming.close();
            while let Some(result) = workers.join_next().await {
                result.unwrap();
            }
        });
        let mut fixture = Fixture::cold(peer, 1, 3, None, None);
        assert_eq!(
            NativePoolFactory.describe(&fixture.options).resources.len(),
            1
        );
        fixture.start().await;
        let managed = fixture.managed();
        let StreamLoan {
            io: mut first,
            owner: mut first_control,
        } = fixture
            .options
            .open(fixture.actual, fixture.demand())
            .await
            .unwrap()
            .into_stream_loan()
            .unwrap();
        first.write_all(b"1").await.unwrap();
        first.flush().await.unwrap();
        let mut byte = [0; 1];
        first.read_exact(&mut byte).await.unwrap();
        assert_eq!(byte, *b"1");
        let StreamLoan {
            io: mut second,
            owner: mut second_control,
        } = fixture
            .options
            .open(fixture.actual, fixture.demand())
            .await
            .unwrap()
            .into_stream_loan()
            .unwrap();
        second.write_all(b"2").await.unwrap();
        second.flush().await.unwrap();
        second.read_exact(&mut byte).await.unwrap();
        assert_eq!(byte, *b"2");
        assert!(
            first_control
                .original()
                .same_original(second_control.original())
        );
        let physical = managed.records.lock().unwrap()[0].clone();
        let witness = physical.witness.lock().unwrap().clone().unwrap();
        assert!(physical.adoption.retained_driver().is_some());
        {
            let mut waiting = Box::pin(first.read_exact(&mut byte));
            let mut cx = Context::from_waker(std::task::Waker::noop());
            assert!(waiting.as_mut().poll(&mut cx).is_pending());
            first_control.cancel(zero_platform_tokio::contract::CloseReason::Cancelled);
        }
        drop(first);
        let closed = first_control.close(deadline()).await.unwrap();
        assert!(closed.is_complete(), "{closed:?}");
        assert!(first_control.closed().unwrap().matches(&first_control));
        assert!(
            !witness.is_joined(),
            "a logical cancel must preserve the actual pool driver"
        );
        assert!(physical.adoption.closed().is_none());
        second.write_all(b"s").await.unwrap();
        second.flush().await.unwrap();
        second.read_exact(&mut byte).await.unwrap();
        assert_eq!(byte, *b"s");
        let StreamLoan {
            io: mut third,
            owner: mut third_control,
        } = fixture
            .options
            .open(fixture.actual, fixture.demand())
            .await
            .unwrap()
            .into_stream_loan()
            .unwrap();
        assert!(
            third_control
                .original()
                .same_original(second_control.original())
        );
        third.write_all(b"3").await.unwrap();
        third.flush().await.unwrap();
        third.read_exact(&mut byte).await.unwrap();
        assert_eq!(byte, *b"3");
        assert_eq!(fixture.traffic.boundaries.load(Ordering::SeqCst), 1);
        drop(second);
        drop(third);
        assert!(
            second_control
                .close(deadline())
                .await
                .unwrap()
                .is_complete()
        );
        assert!(third_control.close(deadline()).await.unwrap().is_complete());
        let (protocol, tasks) = fixture.close().await;
        assert!(protocol.is_complete(), "{protocol:?}");
        assert!(tasks.is_complete(), "{tasks:?}");
        let closed = physical.adoption.closed().unwrap();
        assert!(closed.matches(&physical.adoption));
        assert!(
            physical
                .adoption
                .users_closed()
                .unwrap()
                .matches(&physical.adoption)
        );
        assert!(witness.is_joined());
        finish.send(()).unwrap();
        server.await.unwrap();
    })
    .await
    .unwrap();
}
