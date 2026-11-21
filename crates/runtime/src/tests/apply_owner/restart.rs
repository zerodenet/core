//! The original task and closed Scope, not an empty counter, permit reuse.
use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
#[tokio::test]
async fn same_apply_owner_reopens_only_after_original_pending_io_task_and_scope_retire() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let owner = Arc::new(ApplyOwner::default());
        let serial = Arc::new(Mutex::new(()));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (client, accepted) = tokio::join!(
            tokio::net::TcpStream::connect(listener.local_addr().unwrap()),
            listener.accept()
        );
        let mut peer = client.unwrap();
        let (mut io, _) = accepted.unwrap();
        let (entered, pending) = tokio::sync::oneshot::channel();
        let actual = owner.clone();
        let guard = serial.clone();
        let work = tokio::spawn(async move {
            actual
                .run(guard, async move {
                    let mut entered = Some(entered);
                    let mut byte = [0];
                    let read = io.read(&mut byte);
                    tokio::pin!(read);
                    let count = std::future::poll_fn(|cx| {
                        let result = std::future::Future::poll(read.as_mut(), cx);
                        if result.is_pending() {
                            if let Some(entered) = entered.take() {
                                entered.send(()).unwrap();
                            }
                        }
                        result
                    })
                    .await
                    .map_err(|error| error.to_string())?;
                    assert_eq!(count, 0);
                    Ok(11)
                })
                .await
        });
        pending.await.unwrap();
        let (original, observation) = {
            let state = owner.owner.lock().await;
            (
                state.registration.clone().unwrap(),
                state
                    .scope
                    .as_ref()
                    .unwrap()
                    .registrar()
                    .retirement_observation(),
            )
        };
        owner.begin_shutdown();
        assert!(owner.reopen_after_retirement().await.is_err());
        assert!(!original.is_joined() && !observation.is_drained());
        peer.shutdown().await.unwrap();
        assert_eq!(work.await.unwrap().unwrap(), 11);
        owner.wait_idle().await.unwrap();
        assert!(original.is_joined());
        assert!(!observation.is_drained(), "body/join is not a sealed Scope");
        assert!(owner.reopen_after_retirement().await.is_err());
        owner
            .shutdown(Instant::now() + Duration::from_secs(2))
            .await
            .unwrap();
        assert!(observation.is_drained());
        assert!(original.try_joined().unwrap().registration_released());
        owner.reopen_after_retirement().await.unwrap();
        assert_eq!(owner.run(serial, async { Ok(22) }).await.unwrap(), 22);
        owner
            .shutdown(Instant::now() + Duration::from_secs(2))
            .await
            .unwrap();
    })
    .await
    .unwrap();
}
