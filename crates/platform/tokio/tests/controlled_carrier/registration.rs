use super::*;

#[tokio::test]
async fn registration_identifies_the_actual_owner_not_equal_ids_and_retains_no_owner() {
    let mut fixture = Fixture::new(1).await;
    let mut other = Fixture::new(1).await;
    let strong = Arc::strong_count(&fixture.owner.0);
    let registration = fixture.owner.registration();
    let clone = registration.clone();
    assert_eq!(Arc::strong_count(&fixture.owner.0), strong);
    assert_eq!(registration.instance(), other.owner.instance());
    assert_eq!(registration.limit().get(), 1);
    assert!(registration.same_owner(&clone));
    assert!(!registration.same_owner(&other.owner.registration()));
    let grant = fixture.grants.reserve().unwrap();
    let foreign = other.grants.reserve().unwrap();
    assert!(registration.matches_grant(&grant));
    assert!(!registration.matches_grant(&foreign));
    assert_eq!(registration.observe().unwrap().retained(), 1);
    assert!(registration.observe().unwrap().is_live());
    drop(grant);
    drop(foreign);
    assert_eq!(registration.observe().unwrap().retained(), 0);
    assert!(
        registration.closed().is_none(),
        "empty live owner is not closed"
    );
    fixture.owner.shutdown(deadline()).await.unwrap();
    let closed = registration.closed().unwrap();
    assert!(closed.matches(&clone));
    assert!(!closed.matches(&other.owner.registration()));
    assert_eq!(closed.instance(), instance());
    let Fixture { owner, grants, .. } = fixture;
    drop(owner);
    drop(grants);
    assert!(registration.observe().is_none());
    assert!(closed.matches(&registration));
    assert!(registration.closed().unwrap().matches(&registration));
    other.owner.shutdown(deadline()).await.unwrap();
}

#[tokio::test]
async fn generation_receipt_waits_for_actual_driver_join_and_original_native_close() {
    timeout(Duration::from_secs(3), async {
        let mut fixture = Fixture::new(1).await;
        let registration = fixture.owner.registration();
        let (carrier, mut peer) = fixture.connected().await;
        let (io, adoption) = carrier.into_parts();
        let mut scope = TaskScope::new(
            tokio::runtime::Handle::current(),
            NonZeroUsize::new(1).unwrap(),
        );
        let (exited, body_exit) = tokio::sync::oneshot::channel();
        let (_, witness) = adoption
            .spawn_driver(
                &scope.registrar(),
                TaskClass::Auxiliary,
                io.into_driver(),
                |io| async move {
                    drop(io);
                    let _ = exited.send(());
                    Ok(())
                },
            )
            .unwrap();
        body_exit.await.unwrap();
        assert!(!witness.is_joined());
        let observed = registration.observe().unwrap();
        let actual = &observed.slots()[0];
        let (id, same_witness) = actual.driver().unwrap();
        assert_eq!(*id, adoption.slot.state().driver.as_ref().unwrap().0);
        assert!(!same_witness.is_joined());
        assert!(!actual.native().unwrap().is_closed());
        assert!(actual.native().unwrap().first_fault().is_none());
        assert!(registration.closed().is_none());
        let partial = fixture.owner.shutdown(Instant::now()).await.unwrap();
        assert!(!partial.is_complete());
        assert!(registration.observe().unwrap().is_stopped());
        assert!(registration.closed().is_none());
        scope.next_exit().await.unwrap();
        assert!(witness.is_joined());
        assert!(
            registration.closed().is_none(),
            "join did not close original FD"
        );
        assert_eq!(
            peer.try_read(&mut [0; 1]).unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
        assert!(fixture
            .owner
            .shutdown(deadline())
            .await
            .unwrap()
            .is_complete());
        assert_eq!(peer.read(&mut [0; 1]).await.unwrap(), 0);
        assert!(adoption.closed().unwrap().matches(&adoption));
        assert!(registration.closed().unwrap().matches(&registration));
        assert!(scope.shutdown(deadline()).await.is_complete());
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn lost_owner_and_logical_view_drop_never_issue_a_generation_receipt() {
    let fixture = Fixture::new(1).await;
    let registration = fixture.owner.registration();
    let (carrier, mut peer) = fixture.connected().await;
    let (io, adoption) = carrier.into_parts();
    let Fixture { owner, grants, .. } = fixture;
    drop(owner);
    drop(grants);
    assert!(registration.observe().is_none());
    assert!(registration.closed().is_none());
    drop(io);
    assert!(registration.closed().is_none());
    assert!(adoption.retire(deadline()).await.unwrap().is_complete());
    assert_eq!(peer.read(&mut [0; 1]).await.unwrap(), 0);
    assert!(adoption.closed().is_some());
    assert!(
        registration.closed().is_none(),
        "single-slot close cannot stand in for the lost generation owner"
    );
}
