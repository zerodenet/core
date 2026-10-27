use super::*;

#[tokio::test]
async fn standard_datagram_replacement_waits_for_original_control_eof_after_cancellation() {
    let (fixture, mut dispatch) = UpstreamFixture::establish().await;
    let original_session = fixture.session_id;
    assert!(dispatch.drop_upstream_association().is_some());
    assert!(dispatch.upstream_association_view().is_none());
    dispatch.touch_upstream_idle(Duration::from_secs(1));
    assert!(
        dispatch.poll_refs().2.is_none(),
        "retiring originals must not rearm the idle timer"
    );
    let mut original = fixture.control.lock().await.take().unwrap();
    let packet = inbound_packet();
    {
        let replacing = fixture
            .runtime
            .dispatch_inbound_packet(&mut dispatch, &packet, None, None);
        tokio::pin!(replacing);
        // The real old control stream has sent FIN, while its receive direction
        // is still awaiting EOF from this retained original server stream.
        tokio::time::timeout(Duration::from_secs(2), async {
            let mut eof = [0];
            tokio::select! {
                result = replacing.as_mut() => panic!("replacement completed before original EOF: {result:?}"),
                read = original.read(&mut eof) => assert_eq!(read.unwrap(), 0),
            }
        }).await.unwrap();
        assert!(futures_util::poll!(replacing.as_mut()).is_pending());
        let accepting = fixture.listener.accept();
        tokio::pin!(accepting);
        assert!(
            futures_util::poll!(accepting.as_mut()).is_pending(),
            "a candidate TCP socket opened before original closure"
        );
        assert_eq!(
            fixture
                .proxy
                .stats_snapshot()
                .udp_upstream
                .created_associations,
            1
        );
        assert_eq!(
            fixture
                .proxy
                .stats_snapshot()
                .udp_upstream
                .active_associations,
            1
        );
        // Interrupt the actual awaited close. The original remains retained.
    }
    {
        let replacing = fixture
            .runtime
            .dispatch_inbound_packet(&mut dispatch, &packet, None, None);
        tokio::pin!(replacing);
        assert!(futures_util::poll!(replacing.as_mut()).is_pending());
        let accepting = fixture.listener.accept();
        tokio::pin!(accepting);
        assert!(futures_util::poll!(accepting.as_mut()).is_pending());
        assert_eq!(
            fixture
                .proxy
                .stats_snapshot()
                .udp_upstream
                .created_associations,
            1
        );
    }
    // Only the real server EOF releases the retained original controller.
    drop(original);
    let relay_port = fixture.relay.local_addr().unwrap().port();
    let (sent, mut replacement) = tokio::time::timeout(Duration::from_secs(2), async {
        tokio::join!(
            fixture
                .runtime
                .dispatch_inbound_packet(&mut dispatch, &packet, None, None),
            accept_association(&fixture.listener, relay_port),
        )
    })
    .await
    .expect("confirmed closure must admit the replacement");
    assert_eq!(sent.unwrap(), original_session);
    receive_packet(&fixture.relay).await;
    assert_control_pending(&mut replacement).await;
    let stats = fixture.proxy.stats_snapshot().udp_upstream;
    assert_eq!(stats.created_associations, 2);
    assert_eq!(stats.active_associations, 1);
    assert_eq!(stats.dropped_associations, 1);
    assert_eq!(dispatch.finish_all().len(), 1);
    let retired = fixture
        .runtime
        .services()
        .packet_path_execution()
        .tcp()
        .retired_datagrams();
    let (report, ()) = tokio::join!(
        retired.progress_retired(tokio::time::Instant::now() + Duration::from_secs(2)),
        async {
            assert_eq!(replacement.read(&mut [0]).await.unwrap(), 0);
            drop(replacement);
        }
    );
    assert!(
        report.unwrap().is_complete(),
        "consume the replacement's actual original receipt"
    );
    assert_eq!(
        fixture
            .proxy
            .stats_snapshot()
            .udp_upstream
            .active_associations,
        0
    );
}
