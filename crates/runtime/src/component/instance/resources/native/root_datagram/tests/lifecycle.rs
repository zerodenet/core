use super::*;
#[tokio::test]
async fn actual_query_wire_native_close_and_original_join_are_independent_and_65_reuse_is_real() {
    let target = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let peer = target.local_addr().unwrap();
    let mut fixture = Fixture::new(&[peer], one());
    let access = fixture.resources.activate(deadline()).unwrap();
    drop(access); // root remains authorized for lazy queries after real start
    for number in 0..65u8 {
        let grant = fixture.grants[0].clone();
        let (sent, received) = tokio::sync::oneshot::channel();
        let witness = fixture.spawn(move |query| async move {
            let opened = grant
                .open(peer, grant.network_generation(), &query)
                .unwrap();
            let local = opened.io.local_addr().unwrap();
            assert_eq!(opened.instance, instance());
            opened.io.send_to_addr(&[number], peer).await.unwrap();
            let mut reply = [0; 1];
            assert_eq!(opened.io.recv_from_addr(&mut reply).await.unwrap().0, 1);
            assert_eq!(reply, [number]);
            let closed = grant.close(opened.io).unwrap();
            // Same original query cannot create a second native attempt even
            // after a successful close. Packet retries used the original view.
            assert!(grant
                .open(peer, grant.network_generation(), &query)
                .is_err());
            drop(closed);
            sent.send(local).unwrap();
            Ok(())
        });
        let mut bytes = [0; 1];
        let (_, source) = target.recv_from(&mut bytes).await.unwrap();
        assert_eq!(bytes, [number]);
        target.send_to(&bytes, source).await.unwrap();
        let local = received.await.unwrap();
        assert!(
            !witness.is_joined(),
            "task body return is not actual Scope consumption"
        );
        assert!(!fixture.resources.io_evidence().roots[0].queries_joined);
        let _rebound = tokio::net::UdpSocket::bind(local).await.unwrap();
        let exit = fixture.scope.next_exit().await.unwrap();
        assert!(exit.result.is_ok());
        assert!(witness.is_joined());
    }
    let report = fixture.resources.shutdown_io(deadline()).await.unwrap();
    assert!(report.is_complete());
    assert_eq!(
        report.closed_io.len(),
        1,
        "bounded aggregate, no historical per-query ledger"
    );
    assert!(report.closed_io[0].contains("65 original closes"));
    let proof = fixture.resources.io_evidence();
    assert!(proof.physical_confirmed());
    assert_eq!(proof.declarations[0].ordinal, 0);
    assert!(fixture
        .resources
        .shutdown_io(deadline())
        .await
        .unwrap()
        .closed_io
        .is_empty());
}
#[tokio::test]
async fn actual_pending_query_cancellation_returns_view_but_native_and_join_require_own_evidence() {
    let target = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let peer = target.local_addr().unwrap();
    let mut fixture = Fixture::new(&[peer], one());
    let _access = fixture.resources.activate(deadline()).unwrap();
    let grant = fixture.grants[0].clone();
    let (sent, received) = tokio::sync::oneshot::channel();
    let witness = fixture.spawn(move |query| async move {
        let opened = grant
            .open(peer, grant.network_generation(), &query)
            .unwrap();
        sent.send(opened.io.local_addr().unwrap()).unwrap();
        let mut bytes = [0; 8];
        assert!(opened.io.recv_from_addr(&mut bytes).await.is_err());
        drop(opened.io); // return only, host must explicitly close original
        Ok(())
    });
    let local = received.await.unwrap();
    let first = fixture.resources.shutdown_io(deadline()).await.unwrap();
    assert!(!first.is_complete());
    let exit = fixture.scope.next_exit().await.unwrap();
    assert!(exit.result.is_ok());
    assert!(witness.is_joined());
    assert!(
        !fixture.resources.io_evidence().physical_confirmed(),
        "no inferred native ACK"
    );
    let second = fixture.resources.shutdown_io(deadline()).await.unwrap();
    assert!(second.is_complete());
    assert_eq!(second.closed_io.len(), 1);
    assert!(fixture.resources.io_evidence().physical_confirmed());
    let _rebound = tokio::net::UdpSocket::bind(local).await.unwrap();
}
