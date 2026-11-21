//! Deadline-free physical continuations reclaim only original returned slots.
use super::{accepted, fixtures::*, request};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zero_platform_tokio::contract::{stream::ConnectRequest, CloseReason, Reason};

#[tokio::test]
async fn deadline_free_service_reuses_one_original_after_its_real_view_returns() {
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let remote = tokio::spawn(async move {
            for _ in 0..3 {
                let (socket, _) = listener.accept().await.unwrap();
                let mut socket = zero_platform_tokio::TcpRelayStream::new(socket);
                assert_eq!(
                    request(&mut socket).await,
                    (target().address, target().port)
                );
                accepted(&mut socket).await;
                let (mut read, mut write) = tokio::io::split(socket);
                tokio::io::copy(&mut read, &mut write).await.unwrap();
            }
        });
        let mut owner = Installed::new(10, endpoint(address)).await;
        let registration = owner.owner();
        let mut originals = Vec::new();
        for byte in 0..3 {
            let mut call = context(owner.root(address));
            call.deadline = None;
            let role = owner.instance.roles.stream.as_ref().unwrap();
            let mut connected = role
                .connect(
                    ConnectRequest {
                        destination: target(),
                    },
                    call,
                )
                .await
                .unwrap();
            connected.io.write_all(&[byte]).await.unwrap();
            assert_eq!(connected.io.read_u8().await.unwrap(), byte);
            let snapshot = registration.observe().unwrap();
            assert_eq!(snapshot.slots().len(), 1);
            originals.push(snapshot.slots()[0].original().clone());
            for original in &originals[..originals.len() - 1] {
                assert!(original.matches_closed(&original.closed().unwrap()));
            }
            let mut blocked = context(owner.root(address));
            blocked.deadline = None;
            let denied = role
                .connect(
                    ConnectRequest {
                        destination: target(),
                    },
                    blocked,
                )
                .await
                .err()
                .expect("a live original still occupies the unchanged N=1 limit");
            assert_eq!(denied.reason, Reason::Capacity);
            assert!(originals.last().unwrap().closed().is_none());
            drop(connected);
        }
        owner.instance.lifecycle.revoke(CloseReason::Shutdown);
        assert!(owner
            .instance
            .lifecycle
            .shutdown(end())
            .await
            .unwrap()
            .is_complete());
        assert!(registration.closed().unwrap().matches(&registration));
        assert!(registration.users_closed().unwrap().matches(&registration));
        for original in originals {
            assert!(original.matches_closed(&original.closed().unwrap()));
        }
        assert!(owner.scope.shutdown(end()).await.is_complete());
        remote.await.unwrap();
    })
    .await
    .unwrap();
}
