use super::*;
use crate::contract::network::UpstreamOpener;
use crate::contract::{component::ResourceDeclaration, lifecycle::Unfinished};
use tokio::net::UdpSocket;

#[tokio::test]
async fn controlled_packet_waits_for_its_actual_driver_and_original_native_close() {
    timeout(Duration::from_secs(6), async {
        let peer = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let address = peer.local_addr().unwrap();
        let egress = Arc::new(crate::EgressInterfaceControl::default());
        let binding = UpstreamBinding {
            prefix: vec![],
            dial: zero_traits::DialPolicy {
                source_ip: Some("127.0.0.1".parse().unwrap()),
                ..Default::default()
            },
            network_generation: NetworkGeneration(egress.generation()),
        };
        let endpoint = destination(address);
        let opener =
            NativeCarrierOpener::new(instance(), binding.clone(), address, egress, None).unwrap();
        let mut owner = CarrierOwner::new(
            instance(),
            NonZeroUsize::new(1).unwrap(),
            &CancellationToken::new(),
        );
        let registration = owner.registration();
        let mut scope = TaskScope::new(
            tokio::runtime::Handle::current(),
            NonZeroUsize::new(1).unwrap(),
        );
        registration
            .bind_declared(
                &ResourceDeclaration::ControlledDatagramCarrier {
                    endpoint: endpoint.clone(),
                    binding: binding.clone(),
                    max_sockets: NonZeroUsize::new(1).unwrap(),
                },
                &scope.registrar(),
                &Arc::new(CancellationToken::new()),
            )
            .unwrap();
        owner.start(deadline()).await.unwrap();
        let mut grant = owner.grants().reserve().unwrap();
        let view = grant.claim_driver_view().unwrap();
        let adoption = grant.adoption();
        assert!(
            opener
                .prepare_controlled_stream(&binding, endpoint.clone(), context(), grant)
                .is_err(),
            "packet declaration cannot authorize a TCP original"
        );
        drop(view);
        assert!(adoption.retire(deadline()).await.unwrap().is_complete());
        assert!(!adoption.closed().unwrap().had_native_acquisition());
        let mut grant = owner.grants().reserve().unwrap();
        let view = grant.claim_driver_view().unwrap();
        let adoption = grant.adoption();
        let io = opener
            .prepare_controlled_datagram(&binding, endpoint, context(), grant)
            .unwrap();
        let original = io.local_addr().unwrap();
        let (release, released) = tokio::sync::oneshot::channel::<()>();
        let (ready, observed) = tokio::sync::oneshot::channel();
        let (id, witness) = adoption
            .spawn_driver(
                &scope.registrar(),
                TaskClass::Critical,
                io.into_driver(),
                |io| async move {
                    let _view = view;
                    io.send(b"packet").await.unwrap();
                    let mut bytes = [0; 16];
                    assert_eq!(io.receive(&mut bytes).await.unwrap(), 6);
                    ready.send(()).unwrap();
                    let _ = released.await;
                    Ok(())
                },
            )
            .unwrap();
        let mut bytes = [0; 16];
        let (size, source) = peer.recv_from(&mut bytes).await.unwrap();
        assert_eq!(source, original);
        peer.send_to(&bytes[..size], source).await.unwrap();
        observed.await.unwrap();
        let report = owner
            .shutdown(Instant::now() + Duration::from_millis(25))
            .await
            .unwrap();
        assert!(report.unfinished.contains(&Unfinished::Task(id)));
        assert!(adoption.closed().is_none());
        assert!(registration.closed().is_none());
        assert!(!witness.is_joined());
        assert!(owner.grants().reserve().is_err());
        release.send(()).unwrap();
        let exit = scope.next_exit().await.unwrap();
        assert_eq!(exit.id, id);
        assert!(exit.result.is_ok());
        assert!(witness.is_joined());
        let report = owner.shutdown(deadline()).await.unwrap();
        assert!(report.is_complete(), "{report:?}");
        assert_eq!(report.closed_io.len(), 1);
        let receipt = adoption.closed().unwrap();
        assert!(receipt.matches(&adoption) && receipt.had_native_acquisition());
        assert_eq!(receipt.driver_exit().unwrap().id(), id);
        assert!(registration.closed().unwrap().matches(&registration));
        assert!(registration.users_closed().unwrap().matches(&registration));
        assert!(scope.shutdown(deadline()).await.is_complete());
    })
    .await
    .unwrap();
}
