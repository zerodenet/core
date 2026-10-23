use super::*;

#[tokio::test]
async fn actual_engine_current_slot_and_registered_owner_preserve_business_id_none_deadline_and_real_wire(
) {
    timeout(Duration::from_secs(4), async {
        let fixture = Fixture::new().await;
        let host = TestContext::new([fixture.slot.as_ref()]);
        host.drive(fixture.slot.start(deadline())).await.unwrap();
        fixture
            .slot
            .host_admission()
            .unwrap()
            .publish(std::iter::once(fixture.slot.admission_grant()));
        let (session, invocation) = admitted(&fixture.proxy);
        assert!(invocation.deadline.is_none());
        let original = invocation.identity.clone();
        let opener = fixture.opener(&invocation).unwrap();
        let context = CarrierContext {
            session: Some(SessionId(session.id)),
            deadline: None,
            cancel: invocation.cancellation(),
        };
        let grants = fixture.grants();
        let prepared = opener
            .prepare_controlled_stream(
                &fixture.binding,
                fixture.endpoint.clone(),
                context,
                grants.reserve().unwrap(),
            )
            .unwrap();
        let carrier = host.drive(prepared.open_with_deadline(None)).await.unwrap();
        let (mut peer, actual_local) = fixture.listener.accept().await.unwrap();
        assert_eq!(
            carrier.endpoints().peer(),
            fixture.listener.local_addr().unwrap()
        );
        assert_eq!(carrier.endpoints().local(), Some(actual_local));
        assert!(opener.take_evidence().is_some());
        let (io, adoption) = carrier.into_parts();
        let (_, witness) = adoption
            .spawn_driver(
                &fixture.tasks(),
                TaskClass::Auxiliary,
                io.into_driver(),
                |mut io| async move {
                    let mut request = [0; 4];
                    io.read_exact(&mut request).await.map_err(io_error)?;
                    assert_eq!(&request, b"ping");
                    io.write_all(b"pong").await.map_err(io_error)?;
                    io.shutdown().await.map_err(io_error)?;
                    Ok(())
                },
            )
            .unwrap();
        host.drive(async {
            peer.write_all(b"ping").await.unwrap();
            peer.shutdown().await.unwrap();
            let mut reply = Vec::new();
            peer.read_to_end(&mut reply).await.unwrap();
            assert_eq!(reply, b"pong");
            let joined = witness.wait().await;
            assert_eq!(joined.class(), TaskClass::Auxiliary);
            assert!(joined.result().is_ok());
        })
        .await;
        let (
            zero_platform_tokio::contract::context::RequestIdentity::Business(before),
            zero_platform_tokio::contract::context::RequestIdentity::Business(after),
        ) = (&original, &invocation.identity)
        else {
            panic!("same real business facts");
        };
        assert!(Arc::ptr_eq(before, after));
        assert_eq!(before.session, SessionId(session.id));
        assert_eq!(
            before.effective_source,
            Some("192.0.2.8:48000".parse().unwrap())
        );
        assert_eq!(
            before.local_endpoint,
            Some("192.0.2.1:1080".parse().unwrap())
        );
        assert_eq!(
            before.subject.as_ref().unwrap().principal.0.as_ref(),
            "actual-business-carrier"
        );
        assert_eq!(before.peer.as_ref().unwrap().0.as_ref(), "actual-peer");
        assert_eq!(before.sni.as_deref(), Some("original-business.test"));
        let auth = session.auth.as_ref().unwrap();
        assert_eq!(
            (
                auth.up_bps,
                auth.down_bps,
                auth.device_limit,
                auth.quota_remaining_bytes,
                auth.policy_revision
            ),
            (Some(4096), Some(8192), Some(1), Some(64), Some(7))
        );
        assert!(fixture.registration().closed().is_none());
        assert!(
            adoption.closed().is_none(),
            "actual join does not mint native close"
        );
        assert!(host
            .drive(adoption.retire(deadline()))
            .await
            .unwrap()
            .is_complete());
        let native = adoption.closed().unwrap();
        assert_eq!(
            native.driver_exit().unwrap().id(),
            witness.try_joined().unwrap().id()
        );
        // The canonical provider deliberately still reports transferred users
        // unaccounted. This wire evidence does not activate production replacement.
        let report = host.drive(fixture.slot.shutdown(deadline())).await.unwrap();
        assert!(!report.is_complete());
        let actual = fixture.registration();
        assert!(actual.closed().unwrap().matches(&actual));
        fixture
            .proxy
            .engine()
            .track_session(session.id)
            .finish(zero_engine::SessionOutcome::DirectRelayed)
            .unwrap();
    })
    .await
    .unwrap();
}
fn io_error(source: std::io::Error) -> ChannelError {
    ChannelError::new(Stage::Receive, Reason::Io, Impact::Component).with_source(source)
}
