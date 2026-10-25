use super::*;
#[tokio::test]
async fn actual_consuming_registration_unwind_keeps_original_unknown_and_never_empty_confirms() {
    let peer = "127.0.0.1:9".parse().unwrap();
    let mut fixture = Fixture::new(&[peer], one());
    let _access = fixture.resources.activate(deadline()).unwrap();
    let (done, wait) = tokio::sync::oneshot::channel();
    let query = fixture.spawn(move |_| async move {
        let _ = wait.await;
        Ok(())
    });
    let grant = fixture.grants[0].clone();
    let actual_query = query.clone();
    // Actual native acquisition on a joined, explicit test thread without an
    // entered reactor. Tokio consumes the original then panics at registration;
    // this is not a simulated errno, task exit, or successful close.
    let thread = std::thread::spawn(move || {
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = grant.open(peer, grant.network_generation(), &actual_query);
        }))
    });
    assert!(thread.join().unwrap().is_err());
    let original = fixture.resources.roots.entries[&0]
        .native
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .take_report()
        .unconfirmed
        .remove(0)
        .source;
    let proof = fixture.resources.io_evidence();
    assert!(!proof.physical_confirmed());
    assert!(!proof.roots[0].native_confirmed);
    assert!(Arc::ptr_eq(
        proof.roots[0].native_unknown.as_ref().unwrap(),
        &original
    ));
    assert!(fixture.grants[0]
        .open(peer, fixture.grants[0].network_generation(), &query)
        .is_err());
    done.send(()).unwrap();
    assert!(fixture.scope.next_exit().await.unwrap().result.is_ok());
    assert!(
        query.is_joined(),
        "actual query join is deliberately insufficient for native Unknown"
    );
    assert!(fixture.resources.shutdown_io(deadline()).await.is_err());
    let after = fixture.resources.io_evidence();
    assert!(!after.physical_confirmed());
    assert!(Arc::ptr_eq(
        after.roots[0].native_unknown.as_ref().unwrap(),
        &original
    ));
    assert_eq!(
        fixture.resources.roots.entries[&0]
            .native
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .occupied(),
        1
    );
}
