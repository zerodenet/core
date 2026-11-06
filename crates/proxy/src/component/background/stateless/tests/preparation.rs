//! Actual rejected TCP preparation cannot wait on its still-live Business task.
use super::*;
#[tokio::test]
async fn unpublished_original_waits_for_real_auxiliary_ack_then_same_business_can_use_n1() {
    tokio::time::timeout(std::time::Duration::from_secs(8), async {
        let socket = tokio::net::TcpSocket::new_v4().unwrap();
        socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let peer = socket.local_addr().unwrap(); // real bound, not listening
        let endpoint = Destination::new(Address::Ipv4([127, 0, 0, 1]), peer.port());
        let one = NonZeroUsize::new(1).unwrap();
        let mut scope = TaskScope::suspended_hosted(tokio::runtime::Handle::current(), Some(one));
        scope.enable_registration().unwrap();
        let mut caller = TaskScope::suspended_hosted(tokio::runtime::Handle::current(), Some(one));
        caller.enable_registration().unwrap();
        let (send_io, receive_io) = tokio::sync::oneshot::channel::<BoxStream>();
        let (_, witness) = caller.registrar().spawn_tracked(
            zero_platform_tokio::contract::lifecycle::TaskClass::Critical,
            async move {
                let mut io = receive_io.await.unwrap();
                io.write_all(b"same admitted caller").await.unwrap();
                let mut answer = [0; 20];
                io.read_exact(&mut answer).await.unwrap();
                assert_eq!(&answer, b"same admitted caller");
                drop(io); Ok(())
            }).unwrap();
        let instance = ComponentInstanceId { component: ComponentId(944), generation: ResourceGeneration(1) };
        let host = Arc::new(CancellationToken::new());
        let mut owner = CarrierOwner::new(instance, one, &host);
        let bound = owner.registration().bind_declared(&ResourceDeclaration::ControlledNodeCarrier {
            endpoint: endpoint.clone(), dial: Default::default(), max_sockets: one,
        }, &scope.registrar(), &host).unwrap();
        owner.start(deadline()).await.unwrap();
        let egress = Arc::new(zero_platform_tokio::EgressInterfaceControl::default());
        let binding = UpstreamBinding { prefix: vec![], dial: Default::default(),
            network_generation: zero_core::component::NetworkGeneration(egress.generation()) };
        let facts = facts(&endpoint);
        let demand = Arc::new(Demand(CancellationToken::new()));
        let transport = UpstreamTransport::new(binding.clone(), Arc::new(Root {
            bound, host, peer, binding, egress, caller: witness.clone(), demand: demand.clone(), facts: facts.clone(),
        }));
        let transfers = Arc::new(Mutex::new(Vec::new()));
        let role = Outbound { inner: erase_stream(RawRole(endpoint.clone())),
            tasks: Some(scope.registrar()), process: None, endpoint: endpoint.clone(), dial: Default::default(),
            grants: owner.grants(), transfers: transfers.clone(), limit: one };
        let context = || OpenContext { identity: RequestIdentity::Business(facts.clone()),
            deadline: None, cancel: demand.0.clone(), upstream: transport.clone(), observer: None };
        let request = || ConnectRequest { destination: endpoint.clone() };
        {
            let opening = role.connect(request(), context());
            tokio::pin!(opening);
            let receipt = tokio::select! {
                result = &mut opening => panic!("publication before actual host ACK: {:?}", result.err()),
                receipt = scope.next_receipt() => receipt.unwrap(),
            };
            assert!(receipt.result().is_err());
            let transfer = transfers.lock().unwrap()[0].clone();
            let actual = transfer.adoption.accept_auxiliary().await.unwrap().unwrap();
            assert_eq!(actual.id(), receipt.id());
            assert!(!actual.registration_released());
            let preparation = transfer.preparation.as_ref().unwrap();
            assert!(preparation.publish(&witness).is_err());
            assert!(!preparation.is_published());
            assert!(!witness.is_joined(), "Business caller still owns the same live invocation");
            assert!(!transfer.adoption.try_retire().unwrap().is_complete());
            assert_eq!(owner.retained(), 1);
            assert_eq!(role.reserve_transfer(Some(witness.clone())).err().unwrap().reason, Reason::Capacity);
            let deferred = scope.defer_callbacks();
            scope.acknowledge(&receipt).unwrap();
            assert!(!actual.registration_released());
            assert_eq!(owner.retained(), 1);
            drop(deferred);
            let failure = opening.await.err().unwrap();
            assert!(zero_platform_tokio::controlled_carrier::original_connect_error(&failure).is_some());
            assert!(transfer.adoption.try_retire().unwrap().is_complete());
            let closed = transfer.adoption.users_closed().unwrap();
            assert!(transfer.preparation.as_ref().unwrap().matches_unpublished_closed(&closed));
            assert!(!closed.matches_caller(&witness));
            assert_eq!(owner.retained(), 0);
        }
        let listener = socket.listen(1).unwrap();
        let server = tokio::spawn(async move {
            let (mut io, _) = listener.accept().await.unwrap();
            let mut bytes = [0; 20]; io.read_exact(&mut bytes).await.unwrap();
            assert_eq!(&bytes, b"same admitted caller"); io.write_all(&bytes).await.unwrap();
            let mut remainder = Vec::new(); io.read_to_end(&mut remainder).await.unwrap(); assert!(remainder.is_empty());
        });
        let connected = {
            let opening = role.connect(request(), context());
            tokio::pin!(opening);
            let receipt = tokio::select! {
                result = &mut opening => panic!("publication before actual host ACK: {:?}", result.err()),
                receipt = scope.next_receipt() => receipt.unwrap(),
            };
            assert!(receipt.result().is_ok());
            let transfer = transfers.lock().unwrap().last().unwrap().clone();
            assert!(transfer.preparation.as_ref().unwrap().publish(&witness).is_err());
            assert!(!transfer.preparation.as_ref().unwrap().is_published());
            scope.acknowledge(&receipt).unwrap();
            opening.await.unwrap()
        };
        let transfer = transfers.lock().unwrap().last().unwrap().clone();
        assert!(transfer.preparation.as_ref().unwrap().is_published());
        assert!(!witness.is_joined());
        send_io.send(connected.io).unwrap_or_else(|_| panic!("actual caller awaiting IO"));
        let receipt = caller.next_receipt().await.unwrap(); assert!(receipt.result().is_ok());
        assert!(!transfer.adoption.try_retire().unwrap().is_complete());
        assert_eq!(owner.retained(), 1, "published view still requires original Business ACK");
        assert!(transfer.adoption.users_closed().unwrap().matches_caller(&witness));
        caller.acknowledge(&receipt).unwrap();
        assert!(transfer.adoption.try_retire().unwrap().is_complete());
        assert_eq!(owner.retained(), 0);
        assert!(owner.shutdown(deadline()).await.unwrap().is_complete());
        scope.begin_shutdown(); caller.begin_shutdown(); assert!(scope.is_drained()); assert!(caller.is_drained());
        server.await.unwrap();
    }).await.unwrap();
}
