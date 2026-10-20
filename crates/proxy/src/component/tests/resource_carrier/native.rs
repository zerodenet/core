use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zero_platform_tokio::contract::lifecycle::TaskClass;

#[tokio::test]
async fn original_driver_join_and_native_eof_remain_distinct_from_unaccounted_users() {
    tokio::time::timeout(Duration::from_secs(3), async {
        let mut fixture = Fixture::new().await;
        fixture.scope.enable_registration().unwrap();
        let _access = fixture.resources.activate(deadline()).unwrap();
        let actual = fixture.owner.registration();
        fixture
            .resources
            .register_controlled_carrier(0, actual.clone(), &fixture.scope.registrar())
            .unwrap();
        fixture.owner.start(deadline()).await.unwrap();
        let prepared = fixture
            .transport
            .prepare_controlled_stream(
                fixture.destination(),
                fixture.context(),
                fixture.owner.grants().reserve().unwrap(),
            )
            .unwrap();
        let carrier = prepared.open(deadline()).await.unwrap();
        let (mut peer, _) = fixture.listener.accept().await.unwrap();
        let (io, adoption) = carrier.into_parts();
        let (_, witness) = adoption
            .spawn_driver(
                &fixture.scope.registrar(),
                TaskClass::Auxiliary,
                io.into_driver(),
                |mut io| async move {
                    let mut byte = [0; 1];
                    io.read_exact(&mut byte).await.map_err(|source| {
                        zero_platform_tokio::contract::ChannelError::new(
                            zero_platform_tokio::contract::Stage::Receive,
                            Reason::Io,
                            zero_platform_tokio::contract::Impact::Component,
                        )
                        .with_source(source)
                    })?;
                    io.write_all(&byte).await.map_err(|source| {
                        zero_platform_tokio::contract::ChannelError::new(
                            zero_platform_tokio::contract::Stage::Send,
                            Reason::Io,
                            zero_platform_tokio::contract::Impact::Component,
                        )
                        .with_source(source)
                    })?;
                    Ok(())
                },
            )
            .unwrap();
        peer.write_all(b"x").await.unwrap();
        let mut byte = [0; 1];
        peer.read_exact(&mut byte).await.unwrap();
        assert_eq!(&byte, b"x");
        fixture.scope.next_exit().await.unwrap();
        assert!(witness.is_joined());
        assert!(!fixture.resources.io_evidence().carriers[0].native_generation_closed);
        assert_eq!(
            peer.try_read(&mut byte).unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
        let owner_report = fixture.owner.shutdown(deadline()).await.unwrap();
        assert!(owner_report.is_complete());
        assert_eq!(owner_report.closed_io.len(), 1);
        assert_eq!(peer.read(&mut byte).await.unwrap(), 0);
        assert!(actual.closed().unwrap().matches(&actual));
        let proof = fixture.resources.io_evidence();
        assert!(proof.carriers[0].native_generation_closed);
        assert!(
            !proof.physical_confirmed(),
            "native generation proof cannot invent external users closure"
        );
        let provider_report = fixture.resources.shutdown_io(deadline()).await.unwrap();
        assert!(!provider_report.is_complete() && provider_report.closed_io.is_empty());
        assert_eq!(provider_report.unfinished.len(), 1);
        assert!(fixture.scope.shutdown(deadline()).await.is_complete());
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn actual_host_cancel_fences_native_prepare_with_an_independently_live_owner() {
    let mut fixture = Fixture::new().await;
    fixture.scope.enable_registration().unwrap();
    let _access = fixture.resources.activate(deadline()).unwrap();
    fixture
        .resources
        .register_controlled_carrier(0, fixture.owner.registration(), &fixture.scope.registrar())
        .unwrap();
    fixture.owner.start(deadline()).await.unwrap();
    let grant = fixture.owner.grants().reserve().unwrap();
    fixture.host_cancel.cancel();
    assert!(fixture
        .transport
        .prepare_controlled_stream(fixture.destination(), fixture.context(), grant)
        .is_err());
    std::future::poll_fn(|cx| {
        assert!(
            fixture.listener.poll_accept(cx).is_pending(),
            "host revoke must precede any native connect"
        );
        std::task::Poll::Ready(())
    })
    .await;
    assert!(!fixture.resources.io_evidence().physical_confirmed());
    assert!(fixture
        .owner
        .shutdown(deadline())
        .await
        .unwrap()
        .is_complete());
    assert!(!fixture
        .resources
        .shutdown_io(deadline())
        .await
        .unwrap()
        .is_complete());
    assert!(fixture.scope.shutdown(deadline()).await.is_complete());
}
