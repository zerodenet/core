use super::*;
#[tokio::test]
async fn controlled_carrier_cross_slot_rejection_preserves_both_original_controls_and_capacity() {
    timeout(Duration::from_secs(3), async {
        let mut fixture = Fixture::new(2).await;
        let (a, mut peer_a) = fixture.connected().await;
        let (b, mut peer_b) = fixture.connected().await;
        let (io_a, adoption_a) = a.into_parts();
        let (io_b, adoption_b) = b.into_parts();
        let mut scope = TaskScope::new(
            tokio::runtime::Handle::current(),
            NonZeroUsize::new(1).unwrap(),
        );
        let failure = adoption_a
            .spawn_driver(
                &scope.registrar(),
                TaskClass::Critical,
                io_b.into_driver(),
                |_| async {
                    panic!("wrong origin must reject before building or spawning a task");
                    #[allow(unreachable_code)]
                    Ok(())
                },
            )
            .err()
            .unwrap();
        assert_eq!(failure.reason, Reason::StalePlan);
        assert_eq!(fixture.owner.retained(), 2);
        assert_eq!(
            fixture.grants.reserve().err().unwrap().reason,
            Reason::Capacity
        );
        assert!(adoption_a.closed().is_none());
        assert!(adoption_b.closed().is_none());
        assert_eq!(
            peer_a.try_read(&mut [0; 1]).unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
        assert_eq!(
            peer_b.try_read(&mut [0; 1]).unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
        assert!(timeout(Duration::from_millis(20), scope.next_exit())
            .await
            .is_err());
        // Rejection did not revoke A or steal B's original close responsibility.
        adoption_a
            .spawn_driver(
                &scope.registrar(),
                TaskClass::Critical,
                io_a.into_driver(),
                |mut io| async move {
                    io.write_all(b"A").await.unwrap();
                    Ok(())
                },
            )
            .unwrap();
        let mut byte = [0; 1];
        peer_a.read_exact(&mut byte).await.unwrap();
        assert_eq!(&byte, b"A");
        scope.next_exit().await.unwrap();
        assert!(adoption_a.retire(deadline()).await.unwrap().is_complete());
        let receipt = adoption_a.closed().unwrap();
        assert!(receipt.driver_exit().is_some());
        assert!(!receipt.matches(&adoption_b));
        assert_eq!(peer_a.read(&mut byte).await.unwrap(), 0);
        assert_eq!(fixture.owner.retained(), 1);
        assert!(adoption_b.retire(deadline()).await.unwrap().is_complete());
        assert_eq!(peer_b.read(&mut byte).await.unwrap(), 0);
        assert_eq!(fixture.owner.retained(), 0);
        assert!(scope.shutdown(deadline()).await.is_complete());
        assert!(fixture
            .owner
            .shutdown(deadline())
            .await
            .unwrap()
            .is_complete());
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn controlled_carrier_real_protocol_prepare_and_driver_split_preserve_origin_and_native_io() {
    timeout(Duration::from_secs(3), async {
        let mut fixture = Fixture::new(1).await;
        let (carrier, mut peer) = fixture.connected().await;
        let (io, adoption) = carrier.into_parts();
        // Same shape as Mieru tcp::prepare: perform handshake inline, then split
        // logical metadata from a prepared future that owns the original stream.
        let prepared = io
            .prepare_driver(|mut io| async move {
                io.write_all(b"hello").await?;
                let driver = async move {
                    let mut bytes = [0; 5];
                    io.read_exact(&mut bytes).await.map_err(|source| {
                        ChannelError::new(Stage::Receive, Reason::Io, Impact::Component)
                            .with_source(source)
                    })?;
                    assert_eq!(&bytes, b"reply");
                    Ok(())
                };
                Ok::<_, std::io::Error>((7_u8, driver))
            })
            .await
            .unwrap();
        let (metadata, driver) = prepared.split(|(metadata, driver)| (metadata, driver));
        assert_eq!(metadata, 7);
        let mut scope = TaskScope::new(
            tokio::runtime::Handle::current(),
            NonZeroUsize::new(1).unwrap(),
        );
        let (id, witness) = adoption
            .spawn_driver(&scope.registrar(), TaskClass::Critical, driver, |driver| {
                driver
            })
            .unwrap();
        let mut bytes = [0; 5];
        peer.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"hello");
        peer.write_all(b"reply").await.unwrap();
        let exit = scope.next_exit().await.unwrap();
        assert_eq!(exit.id, id);
        assert!(exit.result.is_ok());
        assert!(witness.is_joined());
        assert!(adoption.closed().is_none());
        assert_eq!(
            fixture.grants.reserve().err().unwrap().reason,
            Reason::Capacity
        );
        assert!(adoption.retire(deadline()).await.unwrap().is_complete());
        assert_eq!(adoption.closed().unwrap().driver_exit().unwrap().id(), id);
        assert_eq!(peer.read(&mut bytes).await.unwrap(), 0);
        assert_eq!(fixture.owner.retained(), 0);
        assert!(scope.shutdown(deadline()).await.is_complete());
        assert!(fixture
            .owner
            .shutdown(deadline())
            .await
            .unwrap()
            .is_complete());
    })
    .await
    .unwrap();
}
