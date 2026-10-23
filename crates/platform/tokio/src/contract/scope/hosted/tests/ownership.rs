use super::*;

struct BodyDropped(Option<oneshot::Sender<()>>);
impl Drop for BodyDropped {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(());
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn registrar_reaping_preserves_the_same_unacknowledged_credit_and_native_receipt() {
    let mut owner = scope(1);
    let register = owner.registrar();
    let (send, receive) = oneshot::channel();
    let (id, witness) = register
        .spawn_tracked(TaskClass::Auxiliary, async move {
            let _body = BodyDropped(Some(send));
            Ok(())
        })
        .unwrap();
    receive.await.unwrap();
    assert!(
        !witness.is_joined(),
        "body destruction alone is not a real join"
    );
    // Same original registrar may reap ready native exits while checking its
    // existing limit. It cannot remove the resulting hosted publication entry.
    assert_eq!(
        register.spawn(TaskClass::Critical, async { Ok(()) }),
        Err(SpawnError::Capacity)
    );
    let original = witness.wait().await;
    assert_eq!(original.id(), id);
    assert_eq!(counts(&owner), (0, 1, 0));
    let receipt = next(&mut owner).await;
    assert!(original.same_receipt(receipt.joined()));
    owner.begin_shutdown();
    assert!(!owner.is_drained());
    owner.acknowledge(&receipt).unwrap();
    assert!(owner.is_drained());
}

#[tokio::test]
async fn dropping_the_hosted_owner_only_aborts_and_never_fabricates_a_native_join() {
    let owner = scope(1);
    let register = owner.registrar();
    let (entered, receive) = oneshot::channel();
    let (dropped, observed) = oneshot::channel();
    let (_, witness) = register
        .spawn_tracked(TaskClass::Critical, async move {
            let _body = BodyDropped(Some(dropped));
            entered.send(()).unwrap();
            std::future::pending::<Result<(), ComponentError>>().await
        })
        .unwrap();
    receive.await.unwrap();
    drop(owner); // The real retained native task future is aborted as a fallback.
    observed.await.unwrap();
    assert!(!witness.is_joined());
    assert!(witness.try_joined().is_none());
    assert_eq!(
        register.spawn(TaskClass::Critical, async { Ok(()) }),
        Err(SpawnError::Closing)
    );
    assert!(!witness.is_joined());
}
