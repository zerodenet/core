//! Same authoritative sealed map, actual receiver transfer and real Host ACK.
use super::*;
struct Outcome(Arc<AtomicUsize>);
impl Drop for Outcome {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
#[tokio::test]
async fn sealed_census_keeps_original_ack_but_neither_scope_owner_nor_output_body() {
    let mut scope =
        TaskScope::suspended_hosted(tokio::runtime::Handle::current(), NonZeroUsize::new(1));
    scope.enable_registration().unwrap();
    let (owner, mut receiver) =
        DriverOwner::<u8, Outcome>::new(scope.registrar(), NonZeroUsize::new(1).unwrap());
    let weak_owner = owner.downgrade();
    let drops = Arc::new(AtomicUsize::new(0));
    let counter = drops.clone();
    let id = owner
        .spawn(7, TaskClass::Auxiliary, |body| async move {
            body.record(Outcome(counter));
            Err(
                ChannelError::new(Stage::Receive, Reason::Io, Impact::Session)
                    .with_source(io::Error::from_raw_os_error(111)),
            )
        })
        .unwrap();
    owner.revoke_registration();
    let host = scope.next_receipt().await.unwrap();
    assert_eq!(host.id(), id);
    let delivery = receiver
        .receive(id, Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
    assert!(
        owner.owned_registration_completion().is_none(),
        "joined/delivered is not body transfer"
    );
    let (_, mut foreign) =
        DriverOwner::<u8, Outcome>::new(scope.registrar(), NonZeroUsize::new(1).unwrap());
    assert!(
        foreign.release_body(&delivery).is_err(),
        "same Scope cannot substitute another ledger"
    );
    let mut altered = delivery.clone();
    altered.body = None;
    assert!(receiver.release_body(&altered).is_err());
    let retained = delivery.clone(); // Original finite receiving row first.
    receiver.release_body(&retained).unwrap();
    receiver.release_body(&retained).unwrap(); // Same actual transfer is idempotent.
    assert!(receiver.release_body(&altered).is_err());
    let closed = owner.owned_registration_completion().unwrap();
    assert!(Arc::ptr_eq(&owner.shared.state, &receiver.shared.state));
    let mut registrations = RegistrationSequence::Original(closed.clone()).iter();
    let witness = registrations.next().unwrap();
    assert!(registrations.next().is_none());
    assert!(witness.matches_joined(&retained.joined));
    assert!(host.local_failure_accepted());
    assert!(Arc::ptr_eq(
        &host.result().unwrap_err(),
        &retained.joined.result().unwrap_err()
    ));
    assert!(!retained.joined.registration_released());
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(delivery);
    drop(retained);
    drop(altered);
    assert_eq!(
        drops.load(Ordering::SeqCst),
        1,
        "receipt/map cannot hold B after real transfer"
    );
    drop(receiver);
    drop(owner);
    assert!(
        weak_owner.upgrade().is_none(),
        "receipt cannot retain Shared/registrar/Scope"
    );
    assert!(witness
        .try_joined()
        .is_some_and(|joined| !joined.registration_released()));
    scope.acknowledge(&host).unwrap();
    let released = witness.wait_registration_released().await;
    assert!(released.registration_released() && witness.matches_joined(&released));
    assert!(Arc::ptr_eq(
        &host.result().unwrap_err(),
        &released.result().unwrap_err()
    ));
    assert_eq!(RegistrationSequence::Original(closed).iter().len(), 1); // No reclaim occurred: exact original row remains.
    scope.begin_shutdown();
    assert!(scope.is_drained());
}
#[tokio::test]
async fn original_delivery_can_transfer_after_real_ack_reclaimed_its_map_row() {
    let mut scope =
        TaskScope::suspended_hosted(tokio::runtime::Handle::current(), NonZeroUsize::new(1));
    scope.enable_registration().unwrap();
    let (owner, mut receiver) =
        DriverOwner::<u8, Outcome>::new(scope.registrar(), NonZeroUsize::new(1).unwrap());
    let drops = Arc::new(AtomicUsize::new(0));
    let counter = drops.clone();
    let id = owner
        .spawn(1, TaskClass::Auxiliary, |body| async move {
            body.record(Outcome(counter));
            Ok(())
        })
        .unwrap();
    owner.revoke_registration();
    let host = scope.next_receipt().await.unwrap();
    let delivery = receiver
        .receive(id, Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
    scope.acknowledge(&host).unwrap();
    owner.reclaim_received();
    assert_eq!(owner.task_status().retained, 0);
    receiver.release_body(&delivery).unwrap(); // Exact token, never absence as authority.
    drop(delivery);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert!(owner.owned_registration_completion().is_some());
    scope.begin_shutdown();
    assert!(scope.is_drained());
}
