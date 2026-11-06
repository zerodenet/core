use super::*;
use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream},
};
async fn original_read(owner: &HostedScope) -> (TcpStream, TaskId, TaskJoinWitness) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let remote = TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let (mut original, _) = listener.accept().await.unwrap();
    let (entered, pending) = oneshot::channel();
    let (id, witness) = owner
        .registrar()
        .spawn_tracked(TaskClass::Auxiliary, async move {
            let mut entered = Some(entered);
            let mut bytes = [0];
            let read = original.read_exact(&mut bytes);
            tokio::pin!(read);
            std::future::poll_fn(|cx| {
                let result = read.as_mut().poll(cx);
                if result.is_pending() {
                    if let Some(entered) = entered.take() {
                        let _ = entered.send(());
                    }
                }
                result
            })
            .await
            .map_err(|source| {
                ChannelError::new(Stage::Receive, Reason::Io, Impact::Component).with_source(source)
            })?;
            Ok(())
        })
        .unwrap();
    pending.await.unwrap();
    (remote, id, witness)
}
#[tokio::test]
async fn real_join_does_not_release_hosted_registration_credit_before_its_actual_ack() {
    let mut owner = scope(1);
    let (mut remote, id, witness) = original_read(&owner).await;
    remote.write_u8(1).await.unwrap();
    let receipt = next(&mut owner).await;
    assert_eq!(receipt.id(), id);
    let joined = witness.wait().await;
    assert!(!joined.registration_released());
    let mut released = Box::pin(witness.wait_registration_released());
    assert!(
        released
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    assert_eq!(
        owner
            .registrar()
            .spawn(TaskClass::Critical, async { Ok(()) }),
        Err(SpawnError::Capacity)
    );
    owner.acknowledge(&receipt).unwrap();
    let actual = timeout(Duration::from_secs(2), released).await.unwrap();
    assert!(witness.matches_joined(&actual));
    assert!(actual.registration_released());
    // The same N=1 now admits actual new socket I/O, not a fabricated credit.
    let (mut remote, next_id, next_witness) = original_read(&owner).await;
    assert_ne!(next_id, id);
    remote.write_u8(2).await.unwrap();
    let receipt = next(&mut owner).await;
    assert_eq!(receipt.id(), next_id);
    owner.acknowledge(&receipt).unwrap();
    assert!(
        next_witness
            .wait_registration_released()
            .await
            .registration_released()
    );
    owner.begin_shutdown();
    assert!(owner.is_drained());
}
#[tokio::test]
async fn deferred_ack_keeps_the_same_registration_charged_until_parent_callback_retirement() {
    let mut owner = scope(1);
    let (mut remote, id, witness) = original_read(&owner).await;
    remote.write_u8(1).await.unwrap();
    let callbacks = owner.defer_callbacks();
    let receipt = next(&mut owner).await;
    assert_eq!(receipt.id(), id);
    owner.acknowledge(&receipt).unwrap();
    let joined = witness.try_joined().unwrap();
    assert!(
        !joined.registration_released(),
        "ACK accepted is not actual entry removal"
    );
    assert_eq!(counts(&owner), (0, 1, 0));
    assert_eq!(
        owner
            .registrar()
            .spawn(TaskClass::Critical, async { Ok(()) }),
        Err(SpawnError::Capacity)
    );
    drop(callbacks);
    let actual = timeout(Duration::from_secs(2), witness.wait_registration_released())
        .await
        .unwrap();
    assert!(witness.matches_joined(&actual));
    assert!(actual.registration_released());
    assert_eq!(counts(&owner), (0, 0, 0));
    owner.begin_shutdown();
    assert!(owner.is_drained());
}
