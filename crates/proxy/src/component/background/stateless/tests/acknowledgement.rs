//! A returned view and consumed caller exit do not refund an unacknowledged slot.
use super::*;
#[tokio::test]
async fn same_taskless_record_keeps_n1_through_real_business_join_and_deferred_host_ack_before_service(
) {
    tokio::time::timeout(std::time::Duration::from_secs(8), async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let peer = listener.local_addr().unwrap();
        let endpoint = Destination::new(Address::Ipv4([127, 0, 0, 1]), peer.port());
        let server = tokio::spawn(async move {
            for expected in [b'b', b's'] {
                let (mut io, _) = listener.accept().await.unwrap();
                let mut bytes = [0];
                io.read_exact(&mut bytes).await.unwrap();
                assert_eq!(bytes, [expected]);
                io.write_all(&bytes).await.unwrap();
                let mut remainder = Vec::new();
                io.read_to_end(&mut remainder).await.unwrap();
                assert!(remainder.is_empty());
            }
        });
        let one = NonZeroUsize::new(1).unwrap();
        let mut scope = TaskScope::suspended(tokio::runtime::Handle::current(), None);
        scope.enable_registration().unwrap();
        let mut caller = TaskScope::suspended_hosted(tokio::runtime::Handle::current(), Some(one));
        caller.enable_registration().unwrap();
        let (send_io, receive_io) = tokio::sync::oneshot::channel::<BoxStream>();
        let (_, witness) = caller
            .registrar()
            .spawn_tracked(
                zero_platform_tokio::contract::lifecycle::TaskClass::Critical,
                async move {
                    let mut io = receive_io.await.unwrap();
                    io.write_all(b"b").await.unwrap();
                    let mut bytes = [0];
                    io.read_exact(&mut bytes).await.unwrap();
                    assert_eq!(bytes, *b"b");
                    drop(io);
                    Ok(())
                },
            )
            .unwrap();
        let instance = ComponentInstanceId {
            component: ComponentId(941),
            generation: ResourceGeneration(1),
        };
        let host = Arc::new(CancellationToken::new());
        let mut owner = CarrierOwner::new(instance, one, &host);
        let descriptor = ResourceDeclaration::ControlledNodeCarrier {
            endpoint: endpoint.clone(),
            dial: Default::default(),
            max_sockets: one,
        };
        let bound = owner
            .registration()
            .bind_declared(&descriptor, &scope.registrar(), &host)
            .unwrap();
        owner.start(deadline()).await.unwrap();
        let egress = Arc::new(zero_platform_tokio::EgressInterfaceControl::default());
        let binding = UpstreamBinding {
            prefix: vec![],
            dial: Default::default(),
            network_generation: zero_core::component::NetworkGeneration(egress.generation()),
        };
        let facts = facts(&endpoint);
        let demand = Arc::new(Demand(CancellationToken::new(), None));
        let root = Arc::new(Root {
            bound,
            host,
            peer,
            binding: binding.clone(),
            egress,
            caller: witness.clone(),
            demand: demand.clone(),
            facts: facts.clone(),
        });
        let transport = UpstreamTransport::new(binding, root);
        let transfers = Arc::new(Mutex::new(Vec::new()));
        let role = Outbound {
            inner: erase_stream(RawRole(endpoint.clone())),
            tasks: None,
            cleanup_network: None,
            process: None,
            endpoint: endpoint.clone(),
            dial: Default::default(),
            grants: owner.grants(),
            transfers: transfers.clone(),
            limit: one,
        };
        let invalid = role.connect(
            ConnectRequest { destination: Destination::new(Address::Ipv4([127,0,0,1]), 0) },
            OpenContext { identity: RequestIdentity::Business(facts.clone()), deadline: None,
                cancel: demand.0.clone(), upstream: transport.clone(), observer: None },
        ).await.err().unwrap();
        assert_eq!(invalid.reason, Reason::InvalidConfig);
        assert!(!witness.is_joined(), "real caller can retry a positively unused reservation");
        let connected = role
            .connect(
                ConnectRequest {
                    destination: endpoint.clone(),
                },
                OpenContext {
                    identity: RequestIdentity::Business(facts),
                    deadline: None,
                    cancel: demand.0.clone(),
                    upstream: transport.clone(),
                    observer: None,
                },
            )
            .await
            .unwrap();
        send_io
            .send(connected.io)
            .unwrap_or_else(|_| panic!("actual caller must receive its view"));
        let receipt = caller.next_receipt().await.unwrap();
        assert!(receipt.result().is_ok());
        let joined = witness.try_joined().unwrap();
        assert!(!joined.registration_released());
        let original = transfers.lock().unwrap()[0].adoption.clone();
        let physical = original.retire_native(deadline()).await.unwrap();
        assert!(physical.report().is_complete());
        assert!(physical.closed().unwrap().matches(&original));
        assert!(!original.try_retire().unwrap().is_complete());
        assert_eq!(owner.retained(), 1, "native/users closed is distinct from caller ACK and N refund");
        assert!(original.closed().unwrap().had_native_acquisition());
        assert!(original.users_closed().unwrap().matches_caller(&witness));
        let service = || OpenContext {
            identity: RequestIdentity::Service(Arc::new(zero_core::request::ServiceRequestFacts {
                origin: zero_core::request::ServiceOrigin::Runtime,
                purpose: zero_core::request::ServicePurpose::BackgroundControl,
            })),
            deadline: Some(deadline()),
            cancel: CancellationToken::new(),
            upstream: transport.clone(),
            observer: None,
        };
        assert_eq!(
            role.connect_carrier(
                ConnectRequest {
                    destination: endpoint.clone()
                },
                service()
            )
            .await
            .err()
            .unwrap()
            .reason,
            Reason::Capacity
        );
        let deferred = caller.defer_callbacks();
        caller.acknowledge(&receipt).unwrap();
        assert!(!joined.registration_released());
        assert_eq!(
            role.connect_carrier(
                ConnectRequest {
                    destination: endpoint.clone()
                },
                service()
            )
            .await
            .err()
            .unwrap()
            .reason,
            Reason::Capacity
        );
        drop(deferred);
        let released = witness.wait_registration_released().await;
        assert!(witness.matches_joined(&released) && released.registration_released());
        assert!(original.retire(deadline()).await.unwrap().is_complete());
        assert_eq!(owner.retained(), 0);
        let mut loan = role
            .connect_carrier(
                ConnectRequest {
                    destination: endpoint,
                },
                service(),
            )
            .await
            .unwrap()
            .io;
        loan.io.write_all(b"s").await.unwrap();
        let mut bytes = [0];
        loan.io.read_exact(&mut bytes).await.unwrap();
        assert_eq!(bytes, *b"s");
        let next = transfers.lock().unwrap().last().unwrap().adoption.clone();
        assert!(!next.same_slot(&original));
        assert!(loan.owner.original().endpoints().unwrap().matches(&next));
        assert!(matches!(loan.owner.original().owner(), zero_platform_tokio::controlled_carrier::CarrierOriginOwner::Carrier(actual) if actual.same_owner(&owner.registration())));
        let StreamLoan {
            io,
            owner: mut loan_owner,
        } = loan;
        drop(io);
        assert!(loan_owner.close(deadline()).await.unwrap().is_complete());
        assert!(loan_owner.closed().unwrap().matches(&loan_owner));
        assert!(owner.shutdown(deadline()).await.unwrap().is_complete());
        assert!(scope.shutdown(deadline()).await.is_complete());
        caller.begin_shutdown();
        assert!(caller.is_drained());
        server.await.unwrap();
    })
    .await
    .unwrap();
}
