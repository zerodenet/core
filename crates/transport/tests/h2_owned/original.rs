use super::*;
use std::{num::NonZeroUsize, sync::Arc};
use tokio::time::Instant;
use zero_core::{
    Address,
    component::{ComponentId, ComponentInstanceId, NetworkGeneration, ResourceGeneration},
    request::Destination,
};
use zero_platform_tokio::{
    EgressInterfaceControl,
    contract::{
        CancellationToken, CloseReason, Reason,
        component::ResourceDeclaration,
        lifecycle::ResourceLifecycle,
        network::{CarrierContext, UpstreamBinding, UpstreamTransport},
        scope::{TaskClass, TaskScope},
    },
    controlled_carrier::{CarrierOwner, NativeCarrierOpener},
};
fn end() -> Instant {
    Instant::now() + Duration::from_secs(4)
}
#[tokio::test]
async fn actual_loan_inline_cancellation_keeps_original_native_users_and_real_caller_join_distinct()
{
    tokio::time::timeout(Duration::from_secs(8), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let peer = listener.local_addr().unwrap();
        let instance = ComponentInstanceId {
            component: ComponentId(914),
            generation: ResourceGeneration(3),
        };
        let egress = Arc::new(EgressInterfaceControl::default());
        let binding = UpstreamBinding {
            prefix: vec![],
            dial: Default::default(),
            network_generation: NetworkGeneration(egress.generation()),
        };
        let opener =
            NativeCarrierOpener::new(instance, binding.clone(), peer, egress, None).unwrap();
        let upstream = UpstreamTransport::new(binding, Arc::new(opener));
        let mut scope = TaskScope::new(
            tokio::runtime::Handle::current(),
            NonZeroUsize::new(1).unwrap(),
        );
        let host = Arc::new(CancellationToken::new());
        let mut original = CarrierOwner::new(
            instance,
            NonZeroUsize::new(1).unwrap(),
            &CancellationToken::new(),
        );
        original
            .registration()
            .bind_declared(
                &ResourceDeclaration::ControlledNodeCarrier {
                    endpoint: Destination::new(Address::Ipv4([127, 0, 0, 1]), peer.port()),
                    dial: Default::default(),
                    max_sockets: NonZeroUsize::new(1).unwrap(),
                },
                &scope.registrar(),
                &host,
            )
            .unwrap();
        original.start(end()).await.unwrap();
        let mut grant = original.grants().reserve().unwrap();
        let adoption = grant.adoption();
        let view = grant.claim_transferred_view().unwrap();
        let carrier = upstream
            .prepare_controlled_stream(
                Destination::new(Address::Ipv4([127, 0, 0, 1]), peer.port()),
                CarrierContext {
                    session: None,
                    deadline: Some(end()),
                    cancel: CancellationToken::new(),
                },
                grant,
            )
            .unwrap()
            .open(end())
            .await
            .unwrap();
        let loan = carrier.into_parts().0.into_stream_loan(view).unwrap();
        let mut control = loan.owner;
        let (server_wire, _) = listener.accept().await.unwrap();
        let (reached_tx, reached_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let mut connection = h2::server::handshake(server_wire).await.unwrap();
            let (request, _response) = connection.accept().await.unwrap().unwrap();
            assert_eq!(request.uri().path(), "/original");
            reached_tx.send(()).unwrap();
            let _ = connection.accept().await;
            release_rx.await.unwrap();
        });
        let (_, witness) = scope
            .registrar()
            .spawn_tracked(TaskClass::Auxiliary, async move {
                let mut stream = connect_h2(loan.io, &profile(), "127.0.0.1", peer.port())
                    .await
                    .unwrap();
                let error = stream.read(&mut [0]).await.unwrap_err();
                assert!(matches!(
                    error.kind(),
                    std::io::ErrorKind::Interrupted
                        | std::io::ErrorKind::Other
                        | std::io::ErrorKind::ConnectionAborted
                ));
                drop(stream);
                Ok(())
            })
            .unwrap();
        reached_rx.await.unwrap();
        assert!(adoption.closed().is_none());
        assert!(!witness.is_joined());
        assert_eq!(original.retained(), 1);
        assert_eq!(
            original.grants().reserve().err().unwrap().reason,
            Reason::Capacity
        );
        control.cancel(CloseReason::Shutdown);
        let exit = scope.next_exit().await.unwrap();
        assert_eq!(Some(exit.id), witness.task_id());
        assert!(witness.is_joined());
        assert!(witness.try_joined().unwrap().registration_released());
        let report = control.close(end()).await.unwrap();
        assert!(report.is_complete());
        let native = adoption.closed().unwrap();
        assert!(native.matches(&adoption));
        assert!(native.had_native_acquisition());
        assert!(
            native.driver_exit().is_none(),
            "inline execution does not manufacture a driver registration"
        );
        assert!(adoption.users_closed().unwrap().matches(&adoption));
        assert_eq!(original.retained(), 0);
        assert!(scope.shutdown(end()).await.is_complete());
        assert!(original.shutdown(end()).await.unwrap().is_complete());
        release_tx.send(()).unwrap();
        server.await.unwrap();
    })
    .await
    .unwrap();
}
