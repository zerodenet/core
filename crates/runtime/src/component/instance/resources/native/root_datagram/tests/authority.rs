use super::*;
#[tokio::test]
async fn exact_cold_foreign_scope_and_public_issuer_denials_do_not_spend_actual_query_attempt() {
    let target = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let peer = target.local_addr().unwrap();
    let mut fixture = Fixture::new(&[peer], one());
    let mut foreign = Fixture::new(&[peer], one());
    let (foreign_ready, foreign_wait) = tokio::sync::oneshot::channel();
    let foreign_query = foreign.spawn(move |_| async move {
        let _ = foreign_wait.await;
        Ok(())
    });
    let grant = fixture.grants[0].clone();
    assert!(grant
        .open(peer, grant.network_generation(), &foreign_query)
        .is_err());
    let (cold, cold_seen) = tokio::sync::oneshot::channel();
    let (live, live_wait) = tokio::sync::oneshot::channel();
    let actual_resources = fixture.resources.clone();
    let query = fixture.spawn(move |query| async move {
        assert!(grant
            .open(peer, grant.network_generation(), &query)
            .is_err());
        cold.send(()).unwrap();
        live_wait.await.unwrap();
        let mut wrong = peer;
        wrong.set_port(if peer.port() == 9 { 10 } else { 9 });
        assert!(grant
            .open(wrong, grant.network_generation(), &query)
            .is_err());
        assert!(grant
            .open(
                peer,
                NetworkGeneration(grant.network_generation().0 + 1),
                &query
            )
            .is_err());
        let fake = RootDatagramAuthority::issue();
        let access: Arc<dyn ResourceAccess> = actual_resources;
        let forged = fake.grant(
            Arc::downgrade(&access),
            instance(),
            0,
            peer,
            grant.network_generation(),
        );
        assert!(!forged.same_root(&grant));
        assert!(forged
            .open(peer, grant.network_generation(), &query)
            .is_err());
        let actual = grant
            .open(peer, grant.network_generation(), &query)
            .unwrap();
        grant.close(actual.io).unwrap();
        Ok(())
    });
    cold_seen.await.unwrap();
    let _access = fixture.resources.activate(deadline()).unwrap();
    assert!(fixture.grants[0]
        .open(peer, fixture.grants[0].network_generation(), &foreign_query)
        .is_err());
    live.send(()).unwrap();
    assert!(fixture.scope.next_exit().await.unwrap().result.is_ok());
    assert!(query.is_joined());
    assert!(fixture
        .resources
        .shutdown_io(deadline())
        .await
        .unwrap()
        .is_complete());
    assert!(fixture.resources.io_evidence().physical_confirmed());
    foreign_ready.send(()).unwrap();
    assert!(foreign.scope.next_exit().await.unwrap().result.is_ok());
    assert!(foreign
        .resources
        .shutdown_io(deadline())
        .await
        .unwrap()
        .is_complete());
}
#[tokio::test]
async fn actual_generation_invalidation_is_pre_native_denied_and_unissued_decl_is_accounted() {
    let peers = [
        "127.0.0.1:9".parse().unwrap(),
        "127.0.0.1:10".parse().unwrap(),
    ];
    let mut fixture = Fixture::new(&peers, one());
    assert!(fixture.grants[0].same_root(&fixture.grants[1]));
    let _access = fixture.resources.activate(deadline()).unwrap();
    fixture.egress.invalidate_network();
    let grant = fixture.grants[0].clone();
    fixture.spawn(move |query| async move {
        let failure = grant
            .open(grant.peer(), grant.network_generation(), &query)
            .err()
            .unwrap();
        assert!(
            failure.retry_receipt().is_none(),
            "public stale selection is no acquisition evidence"
        );
        Ok(())
    });
    assert!(fixture.scope.next_exit().await.unwrap().result.is_ok());
    let report = fixture.resources.shutdown_io(deadline()).await.unwrap();
    assert!(report.is_complete());
    assert!(report.closed_io.is_empty());
    assert!(fixture
        .resources
        .io_evidence()
        .roots
        .iter()
        .all(|root| root.known_no_acquisition));
    assert!(fixture.resources.io_evidence().physical_confirmed());
}
