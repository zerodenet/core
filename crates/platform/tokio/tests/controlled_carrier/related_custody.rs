//! Real negotiated TCP/UDP originals; metadata alone cannot transfer cleanup.
use super::*;
use crate::contract::{component::ResourceDeclaration, network::UpstreamOpener};
use tokio::net::UdpSocket;
use zero_core::component::{AssociationId, OutboundId, RoleId, RoleInstanceId};

#[tokio::test]
async fn only_the_stored_actual_pair_controller_retains_parent_cleanup() {
    for retain in [false, true] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let peer = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let endpoint = destination(peer.local_addr().unwrap());
        let egress = Arc::new(crate::EgressInterfaceControl::default());
        let binding = UpstreamBinding {
            prefix: vec![],
            dial: Default::default(),
            network_generation: NetworkGeneration(egress.generation()),
        };
        let opener = NativeCarrierOpener::new(
            instance(),
            binding.clone(),
            listener.local_addr().unwrap(),
            egress,
            None,
        )
        .unwrap();
        let mut scope = TaskScope::empty(tokio::runtime::Handle::current());
        let mut donor = CarrierOwner::new(
            instance(),
            NonZeroUsize::new(2).unwrap(),
            &CancellationToken::new(),
        );
        donor
            .registration()
            .bind_declared(
                &ResourceDeclaration::ControlledAssociationPool {
                    endpoint: destination(listener.local_addr().unwrap()),
                    dial: binding.dial.clone(),
                    max_sockets: NonZeroUsize::new(2).unwrap(),
                },
                &scope.registrar(),
                &Arc::new(CancellationToken::new()),
            )
            .unwrap();
        donor.start(deadline()).await.unwrap();
        let mut grant = donor.grants().reserve().unwrap();
        let tcp_view = grant.claim_transferred_view().unwrap();
        let parent = grant.adoption();
        let tcp = opener
            .prepare_controlled_stream(
                &binding,
                destination(listener.local_addr().unwrap()),
                context(),
                grant,
            )
            .unwrap()
            .open(deadline())
            .await
            .unwrap();
        let (_peer, _) = listener.accept().await.unwrap();
        let candidates = opener
            .resolve_related_datagram(&binding, parent.clone(), endpoint.clone(), context())
            .await
            .unwrap();
        let mut grant = donor.grants().reserve().unwrap();
        let view = grant.claim_transferred_view().unwrap();
        let child = grant.adoption();
        let mut loan = candidates
            .prepare_datagram(0, grant)
            .unwrap()
            .into_datagram_loan(view, AssociationId(1))
            .unwrap();
        assert!(loan.owner.original().related_control().is_some());
        assert!(loan.owner.original().parent().is_none());
        if retain {
            loan.retain_related_control(parent.clone()).unwrap();
        }
        let borrowed_binding = UpstreamBinding {
            prefix: vec![RoleInstanceId {
                component: instance().component,
                role: RoleId::Outbound(OutboundId(1)),
                generation: instance().generation,
            }],
            ..binding.clone()
        };
        let mut receiver = CarrierOwner::new(
            instance(),
            NonZeroUsize::new(1).unwrap(),
            &CancellationToken::new(),
        );
        receiver
            .registration()
            .bind_declared(
                &ResourceDeclaration::ControlledCarrierPool {
                    endpoint: endpoint.clone(),
                    dial: binding.dial.clone(),
                    datagram: true,
                    max_sockets: NonZeroUsize::new(1).unwrap(),
                },
                &scope.registrar(),
                &Arc::new(CancellationToken::new()),
            )
            .unwrap();
        receiver.start(deadline()).await.unwrap();
        let grant = receiver.grants().reserve().unwrap();
        let receiving = grant.adoption();
        let io = grant
            .adopt_datagram_loan(&borrowed_binding, endpoint, &context(), loan)
            .unwrap();
        let proof = receiving.retained_loan().unwrap();
        assert!(proof.retains(&child));
        assert_eq!(
            proof.retains(&parent),
            retain,
            "read-only related origin is not strong custody"
        );
        io.send(b"original").await.unwrap();
        let mut bytes = [0; 16];
        assert_eq!(peer.recv_from(&mut bytes).await.unwrap().0, 8);
        drop(tcp);
        drop(tcp_view);
        assert!(
            !receiving
                .retire(Instant::now() + Duration::from_millis(5))
                .await
                .unwrap()
                .is_complete()
        );
        assert!(parent.closed().is_none() && child.closed().is_none());
        drop(io);
        assert!(receiving.retire(deadline()).await.unwrap().is_complete());
        assert!(child.closed().unwrap().matches(&child));
        if retain {
            assert!(parent.closed().unwrap().matches(&parent));
            assert!(parent.users_closed().unwrap().matches(&parent));
        } else {
            assert!(
                parent.closed().is_none(),
                "unretained parent is still the donor's responsibility"
            );
            assert!(parent.retire(deadline()).await.unwrap().is_complete());
        }
        assert!(!proof.retains(&parent));
        assert!(receiver.shutdown(deadline()).await.unwrap().is_complete());
        assert!(donor.shutdown(deadline()).await.unwrap().is_complete());
        assert!(scope.shutdown(deadline()).await.is_complete());
    }
}
