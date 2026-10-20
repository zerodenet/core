use super::*;

#[tokio::test]
async fn same_declared_scope_drives_actual_wire_and_only_original_join_and_close_confirm_the_owner()
{
    timeout(Duration::from_secs(3), async {
        let mut fixture = Fixture::cold(1).await;
        let mut scope = TaskScope::new(
            tokio::runtime::Handle::current(),
            NonZeroUsize::new(1).unwrap(),
        );
        let registration = fixture.owner.registration();
        registration
            .bind_declared(
                &declaration(&fixture, ControlledStreamDemandClass::Service),
                &scope.registrar(),
            )
            .unwrap();
        fixture.owner.start(deadline()).await.unwrap();
        let (carrier, mut peer) = fixture.connected().await;
        let (io, adoption) = carrier.into_parts();
        let (_, witness) = adoption
            .spawn_driver(
                &scope.registrar(),
                TaskClass::Auxiliary,
                io.into_driver(),
                |mut io| async move {
                    let mut byte = [0; 1];
                    io.read_exact(&mut byte).await.map_err(|source| {
                        error(
                            instance(),
                            Stage::Receive,
                            Reason::Io,
                            DeliveryState::Unknown,
                        )
                        .with_source(source)
                    })?;
                    assert_eq!(byte, *b"a");
                    io.write_all(b"b").await.map_err(|source| {
                        error(instance(), Stage::Send, Reason::Io, DeliveryState::Unknown)
                            .with_source(source)
                    })?;
                    io.shutdown().await.map_err(|source| {
                        error(instance(), Stage::Send, Reason::Io, DeliveryState::Unknown)
                            .with_source(source)
                    })?;
                    Ok(())
                },
            )
            .unwrap();
        peer.write_all(b"a").await.unwrap();
        let mut bytes = Vec::new();
        peer.read_to_end(&mut bytes).await.unwrap();
        assert_eq!(bytes, b"b");
        scope.next_exit().await.unwrap();
        assert!(witness.wait().await.result().is_ok());
        assert!(registration.closed().is_none());
        assert!(fixture
            .owner
            .shutdown(deadline())
            .await
            .unwrap()
            .is_complete());
        assert!(registration.closed().unwrap().matches(&registration));
        assert!(scope.shutdown(deadline()).await.is_complete());
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn actual_native_acquisition_checks_declared_endpoint_before_creating_any_socket() {
    let mut fixture = Fixture::cold(1).await;
    let mut scope = TaskScope::new(
        tokio::runtime::Handle::current(),
        NonZeroUsize::new(1).unwrap(),
    );
    let mut description = declaration(&fixture, ControlledStreamDemandClass::Service);
    let ResourceDeclaration::ControlledOutboundStream {
        ref mut endpoint, ..
    } = description
    else {
        unreachable!()
    };
    endpoint.port = if endpoint.port == 1 { 2 } else { 1 };
    fixture
        .owner
        .registration()
        .bind_declared(&description, &scope.registrar())
        .unwrap();
    fixture.owner.start(deadline()).await.unwrap();
    let failure = fixture
        .transport
        .prepare_controlled_stream(
            fixture.destination.clone(),
            context(),
            fixture.grants.reserve().unwrap(),
        )
        .err()
        .unwrap();
    assert_eq!(failure.reason, Reason::StalePlan);
    assert_eq!(failure.delivery, DeliveryState::NotSubmitted);
    assert_eq!(fixture.owner.retained(), 0);
    assert!(
        timeout(Duration::from_millis(20), fixture.listener.accept())
            .await
            .is_err()
    );
    assert!(fixture
        .owner
        .shutdown(deadline())
        .await
        .unwrap()
        .is_complete());
    assert!(scope.shutdown(deadline()).await.is_complete());
}
