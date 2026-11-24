use super::*;
use zero_platform_tokio::controlled_carrier::{CarrierNativeKind, CarrierOriginObservation};

#[tokio::test]
async fn standard_datagram_replacement_waits_for_original_receipts_after_cancellation() {
    UpstreamFixture::run(|fixture, mut dispatch| async move {
        let original_session = fixture.session_id;
        let owner = {
            let components = fixture
                .proxy
                .protocols
                .current_components(&fixture.proxy.engine().config())
                .unwrap();
            let slot = components.configured("upstream").unwrap();
            components
                .original_carrier_owner(slot.instance_id(), 0)
                .unwrap()
        };
        let opened = owner.observe().unwrap();
        assert_eq!(opened.slots().len(), 2, "the original TCP and UDP carriers");
        let mut kinds = Vec::new();
        let originals: Vec<CarrierOriginObservation> = opened
            .slots()
            .iter()
            .map(|slot| {
                assert!(slot.physical_view_live() && slot.user_view_live());
                let native = slot.native().unwrap();
                assert!(!native.is_closed());
                kinds.push(native.kind());
                slot.original().clone()
            })
            .collect();
        assert!(kinds.contains(&CarrierNativeKind::Stream));
        assert!(kinds.contains(&CarrierNativeKind::Datagram));

        let retired = fixture
            .runtime
            .services()
            .packet_path_execution()
            .tcp()
            .retired_datagrams();
        // Hold the actual installed controller. A peer withholding its own
        // write EOF does not keep a native local socket or its controller open.
        let control = retired.hold_single_active_controller_for_test();
        let mut original_peer = fixture.control.lock().await.take().unwrap();
        assert!(dispatch.drop_upstream_association().is_some());
        assert!(dispatch.upstream_association_view().is_none());
        dispatch.touch_upstream_idle(Duration::from_secs(1));
        assert!(
            dispatch.poll_refs().2.is_none(),
            "retiring originals must not rearm the idle timer"
        );

        let packet = inbound_packet();
        for _ in 0..2 {
            let replacing =
                fixture
                    .runtime
                    .dispatch_inbound_packet(&mut dispatch, &packet, None, None);
            tokio::pin!(replacing);
            assert!(futures_util::poll!(replacing.as_mut()).is_pending());
            let accepting = fixture.listener.accept();
            tokio::pin!(accepting);
            assert!(
                futures_util::poll!(accepting.as_mut()).is_pending(),
                "a candidate TCP socket opened while original cleanup was pending"
            );
            assert!(originals.iter().all(|original| original.closed().is_none()));
            let stats = fixture.proxy.stats_snapshot().udp_upstream;
            assert_eq!(stats.created_associations, 1);
            assert_eq!(stats.active_associations, 1);
            // Dropping this waiter interrupts retirement of the same controller.
        }
        drop(control);

        let relay_port = fixture.relay.local_addr().unwrap().port();
        let (sent, mut replacement, _) = tokio::time::timeout(Duration::from_secs(2), async {
            tokio::join!(
                fixture
                    .runtime
                    .dispatch_inbound_packet(&mut dispatch, &packet, None, None),
                async {
                    let (mut stream, _) = fixture.listener.accept().await.unwrap();
                    for original in &originals {
                        let closed = original
                            .closed()
                            .expect("original native close before new TCP");
                        assert!(
                            original.matches_closed(&closed) && closed.had_native_acquisition()
                        );
                    }
                    let current = owner.observe().unwrap();
                    assert!(current
                        .slots()
                        .iter()
                        .filter(|slot| originals
                            .iter()
                            .any(|old| old.same_original(slot.original())))
                        .all(|slot| !slot.physical_view_live() && !slot.user_view_live()));
                    super::complete_association(&mut stream, relay_port).await;
                    stream
                },
                async { assert_eq!(original_peer.read(&mut [0]).await.unwrap(), 0) },
            )
        })
        .await
        .expect("confirmed closure must admit the replacement");
        drop(original_peer);
        assert_eq!(sent.unwrap(), original_session);
        receive_packet(&fixture.relay).await;
        assert_control_pending(&mut replacement).await;
        let stats = fixture.proxy.stats_snapshot().udp_upstream;
        assert_eq!(stats.created_associations, 2);
        assert_eq!(stats.active_associations, 1);
        assert_eq!(stats.dropped_associations, 1);
        assert_eq!(fixture.runtime.finish_dispatch(dispatch).len(), 1);
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
    })
    .await;
}
