use super::*;

#[tokio::test]
async fn real_slot_rejects_preparing_foreign_engine_same_numeric_owner_and_public_identity_before_io(
) {
    timeout(Duration::from_secs(4), async {
        let fixture = Fixture::new().await;
        let host = TestContext::new([fixture.slot.as_ref()]);
        host.drive(fixture.slot.start(deadline())).await.unwrap();
        let (session, invocation) = admitted(&fixture.proxy);
        let preparing = fixture.opener(&invocation).unwrap();
        let failure = preparing
            .prepare_controlled_stream(
                &fixture.binding,
                fixture.endpoint.clone(),
                CarrierContext {
                    session: Some(SessionId(session.id)),
                    deadline: None,
                    cancel: invocation.cancellation(),
                },
                fixture.grants().reserve().unwrap(),
            )
            .err()
            .unwrap();
        assert_eq!(
            failure.reason,
            Reason::StalePlan,
            "preparing is not current Business admission"
        );
        fixture
            .slot
            .host_admission()
            .unwrap()
            .publish(std::iter::once(fixture.slot.admission_grant()));
        let foreign = proxy();
        let (foreign_session, foreign_invocation) = admitted(&foreign);
        assert_eq!(session.id, foreign_session.id);
        assert!(BusinessCarrierRequest::new(
            fixture.proxy.engine(),
            &fixture.slot,
            &foreign_invocation
        )
        .is_err());
        let opener = fixture.opener(&invocation).unwrap();
        let mut other =
            CarrierOwner::new(fixture.slot.instance_id(), one(), &CancellationToken::new());
        other.start(deadline()).await.unwrap();
        let fail = opener
            .prepare_controlled_stream(
                &fixture.binding,
                fixture.endpoint.clone(),
                CarrierContext {
                    session: Some(SessionId(session.id)),
                    deadline: None,
                    cancel: invocation.cancellation(),
                },
                other.grants().reserve().unwrap(),
            )
            .err()
            .unwrap();
        assert_eq!(fail.reason, Reason::StalePlan);
        assert_eq!(fail.delivery, DeliveryState::NotSubmitted);
        let public = fixture
            .proxy
            .tcp_runtime_services()
            .for_inbound("in".into())
            .with_business_identity(&session)
            .unwrap()
            .execution()
            .invocation
            .unwrap();
        assert!(public.admitted.is_none());
        let public = fixture.opener(&public).unwrap();
        let fail = public
            .prepare_controlled_stream(
                &fixture.binding,
                fixture.endpoint.clone(),
                CarrierContext {
                    session: Some(SessionId(session.id)),
                    deadline: None,
                    cancel: CancellationToken::new(),
                },
                fixture.grants().reserve().unwrap(),
            )
            .err()
            .unwrap();
        assert_eq!(fail.reason, Reason::Unsupported);
        assert_eq!(
            fixture.grants().reserve().unwrap().instance(),
            fixture.slot.instance_id()
        );
        std::future::poll_fn(|cx| {
            assert!(fixture.listener.poll_accept(cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        assert_eq!(fixture.registration().observe().unwrap().slots().len(), 0);
        assert!(other.shutdown(deadline()).await.unwrap().is_complete());
        assert!(!host
            .drive(fixture.slot.shutdown(deadline()))
            .await
            .unwrap()
            .is_complete());
        fixture
            .proxy
            .engine()
            .track_session(session.id)
            .finish(zero_engine::SessionOutcome::Cancelled)
            .unwrap();
        foreign
            .engine()
            .track_session(foreign_session.id)
            .finish(zero_engine::SessionOutcome::Cancelled)
            .unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn retained_request_rechecks_actual_engine_finish_and_current_host_at_native_acquisition() {
    timeout(Duration::from_secs(4), async {
        let fixture = Fixture::new().await;
        let host = TestContext::new([fixture.slot.as_ref()]);
        host.drive(fixture.slot.start(deadline())).await.unwrap();
        let admission = fixture.slot.host_admission().unwrap();
        admission.publish(std::iter::once(fixture.slot.admission_grant()));
        let (session, invocation) = admitted(&fixture.proxy);
        let opener = fixture.opener(&invocation).unwrap();
        admission.publish(std::iter::empty());
        let failure = opener
            .prepare_controlled_stream(
                &fixture.binding,
                fixture.endpoint.clone(),
                CarrierContext {
                    session: Some(SessionId(session.id)),
                    deadline: None,
                    cancel: invocation.cancellation(),
                },
                fixture.grants().reserve().unwrap(),
            )
            .err()
            .unwrap();
        assert_eq!(failure.reason, Reason::StalePlan);
        admission.publish(std::iter::once(fixture.slot.admission_grant()));
        fixture
            .proxy
            .engine()
            .track_session(session.id)
            .finish(zero_engine::SessionOutcome::Cancelled)
            .unwrap();
        assert!(invocation.cancellation().is_cancelled());
        let failure = opener
            .prepare_controlled_stream(
                &fixture.binding,
                fixture.endpoint.clone(),
                CarrierContext {
                    session: Some(SessionId(session.id)),
                    deadline: None,
                    cancel: invocation.cancellation(),
                },
                fixture.grants().reserve().unwrap(),
            )
            .err()
            .unwrap();
        assert_eq!(failure.reason, Reason::Revoked);
        assert_eq!(failure.session, Some(SessionId(session.id)));
        assert!(std::error::Error::source(&failure).is_some());
        std::future::poll_fn(|cx| {
            assert!(fixture.listener.poll_accept(cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        assert_eq!(fixture.registration().observe().unwrap().slots().len(), 0);
        assert!(!host
            .drive(fixture.slot.shutdown(deadline()))
            .await
            .unwrap()
            .is_complete());
    })
    .await
    .unwrap();
}
