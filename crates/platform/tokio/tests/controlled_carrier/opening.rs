use super::*;
use std::{future::Future, task::Poll};
#[tokio::test]
async fn controlled_carrier_unpolled_open_and_pre_ready_cancel_keep_owner_and_capacity() {
    timeout(Duration::from_secs(3), async {
        let mut fixture = Fixture::new(1).await;
        let prepared = fixture.prepare();
        let opening = prepared.open(deadline());
        drop(opening);
        assert_eq!(fixture.owner.retained(), 1);
        assert_eq!(
            fixture.grants.reserve().err().unwrap().reason,
            Reason::Capacity
        );
        assert!(!fixture
            .owner
            .shutdown(Instant::now())
            .await
            .unwrap()
            .is_complete());
        assert_eq!(fixture.owner.retained(), 1);
        let report = fixture.owner.shutdown(deadline()).await.unwrap();
        assert!(report.is_complete());
        assert_eq!(report.closed_io.len(), 1);
        assert_eq!(fixture.owner.retained(), 0);
        assert!(
            timeout(Duration::from_millis(20), fixture.listener.accept())
                .await
                .is_err()
        );
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn controlled_carrier_half_close_actual_reverse_bytes_and_adoption_driver_join_then_native_close(
) {
    timeout(Duration::from_secs(3), async {
        let mut fixture = Fixture::new(1).await;
        let (carrier, mut peer) = fixture.connected().await;
        let (io, adoption) = carrier.into_parts();
        let mut scope = TaskScope::new(
            tokio::runtime::Handle::current(),
            NonZeroUsize::new(1).unwrap(),
        );
        let (_, witness) = adoption
            .spawn_driver(
                &scope.registrar(),
                TaskClass::Critical,
                io.into_driver(),
                |mut io| async move {
                    io.write_all(b"request").await.unwrap();
                    io.shutdown().await.unwrap();
                    let mut bytes = Vec::new();
                    io.read_to_end(&mut bytes).await.unwrap();
                    assert_eq!(bytes, b"reverse");
                    Ok(())
                },
            )
            .unwrap();
        let mut bytes = Vec::new();
        peer.read_to_end(&mut bytes).await.unwrap();
        assert_eq!(bytes, b"request");
        peer.write_all(b"reverse").await.unwrap();
        peer.shutdown().await.unwrap();
        let exit = scope.next_exit().await.unwrap();
        assert!(exit.result.is_ok());
        assert!(witness.is_joined());
        assert!(adoption.closed().is_none());
        let report = adoption.retire(deadline()).await.unwrap();
        assert!(report.is_complete());
        assert_eq!(report.closed_io.len(), 1);
        assert!(adoption.closed().is_some());
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
#[cfg(target_os = "linux")]
#[tokio::test]
async fn controlled_carrier_actual_pending_connect_cancel_keeps_persistent_original_owner() {
    timeout(Duration::from_secs(3), async {
        let listener = tokio::net::TcpSocket::new_v4().unwrap();
        listener.bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let listener = listener.listen(0).unwrap();
        let _filler = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let egress = Arc::new(crate::EgressInterfaceControl::default());
        let binding = UpstreamBinding {
            prefix: vec![],
            dial: Default::default(),
            network_generation: NetworkGeneration(egress.generation()),
        };
        let mut owner = CarrierOwner::new(
            instance(),
            NonZeroUsize::new(1).unwrap(),
            &CancellationToken::new(),
        );
        owner.start(deadline()).await.unwrap();
        let grants = owner.grants();
        let transport = UpstreamTransport::new(
            binding.clone(),
            Arc::new(
                NativeCarrierOpener::new(
                    instance(),
                    binding,
                    listener.local_addr().unwrap(),
                    egress,
                    None,
                )
                .unwrap(),
            ),
        );
        let prepared = transport
            .prepare_controlled_stream(
                destination(listener.local_addr().unwrap()),
                context(),
                grants.reserve().unwrap(),
            )
            .unwrap();
        let slot = prepared.slot.clone();
        let mut opening = Box::pin(prepared.open(deadline()));
        std::future::poll_fn(|cx| {
            assert!(opening.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        drop(opening);
        assert_eq!(owner.retained(), 1);
        assert!(
            slot.state().control.is_some(),
            "first connect await never owned the only control"
        );
        assert_eq!(grants.reserve().err().unwrap().reason, Reason::Capacity);
        assert!(!owner.shutdown(Instant::now()).await.unwrap().is_complete());
        assert!(owner.shutdown(deadline()).await.unwrap().is_complete());
        assert!(slot.state().closed);
        assert_eq!(owner.retained(), 0);
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn controlled_carrier_real_refused_error_source_survives_until_original_close() {
    timeout(Duration::from_secs(3), async {
        let reserved = tokio::net::TcpSocket::new_v4().unwrap();
        reserved.bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let peer = reserved.local_addr().unwrap();
        let egress = Arc::new(crate::EgressInterfaceControl::default());
        let binding = UpstreamBinding {
            prefix: vec![],
            dial: Default::default(),
            network_generation: NetworkGeneration(egress.generation()),
        };
        let mut owner = CarrierOwner::new(
            instance(),
            NonZeroUsize::new(1).unwrap(),
            &CancellationToken::new(),
        );
        owner.start(deadline()).await.unwrap();
        let grants = owner.grants();
        let transport = UpstreamTransport::new(
            binding.clone(),
            Arc::new(NativeCarrierOpener::new(instance(), binding, peer, egress, None).unwrap()),
        );
        let failure = transport
            .prepare_controlled_stream(destination(peer), context(), grants.reserve().unwrap())
            .unwrap()
            .open(deadline())
            .await
            .err()
            .unwrap();
        assert_eq!(failure.reason, Reason::Io);
        assert_eq!(failure.delivery, DeliveryState::Unknown);
        let mut source: &(dyn std::error::Error + 'static) = &failure;
        let original = loop {
            if let Some(io) = source.downcast_ref::<std::io::Error>() {
                if let Some(code) = io.raw_os_error() {
                    break code;
                }
            }
            source = source
                .source()
                .expect("actual native socket error remains in the source chain");
        };
        #[cfg(unix)]
        assert_eq!(original, libc::ECONNREFUSED);
        #[cfg(windows)]
        assert_eq!(
            original,
            windows_sys::Win32::Networking::WinSock::WSAECONNREFUSED
        );
        assert_eq!(owner.retained(), 1);
        assert_eq!(grants.reserve().err().unwrap().reason, Reason::Capacity);
        let report = owner.shutdown(deadline()).await.unwrap();
        assert!(report.is_complete());
        assert_eq!(report.closed_io.len(), 1);
        assert_eq!(owner.retained(), 0);
        assert_eq!(failure.component, Some(instance().component));
    })
    .await
    .unwrap();
}
