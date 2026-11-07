//! Caller deadline does not acknowledge a completed actual preparation task.
use super::*;
#[tokio::test]
async fn actual_business_deadline_expires_with_deferred_auxiliary_host_ack_without_publishing_or_refunding_n1(
) {
    tokio::time::timeout(std::time::Duration::from_secs(8), async {
        let listener=tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let peer=listener.local_addr().unwrap();
        let endpoint=Destination::new(Address::Ipv4([127,0,0,1]),peer.port());
        let server=tokio::spawn(async move {
            let(mut io,_)=listener.accept().await.unwrap();
            assert_eq!(io.read(&mut [0;1]).await.unwrap(),0,"expired unpublished IO really returns/closes");
        });
        let one=NonZeroUsize::new(1).unwrap();
        let mut scope=TaskScope::suspended_hosted(tokio::runtime::Handle::current(),Some(one)); scope.enable_registration().unwrap();
        let mut caller=TaskScope::suspended_hosted(tokio::runtime::Handle::current(),Some(one)); caller.enable_registration().unwrap();
        let(stop,stopped)=tokio::sync::oneshot::channel();
        let(_,witness)=caller.registrar().spawn_tracked(zero_platform_tokio::contract::lifecycle::TaskClass::Critical,async move {
            let _=stopped.await; Ok(())
        }).unwrap();
        let instance=ComponentInstanceId{component:ComponentId(946),generation:ResourceGeneration(1)};
        let host=Arc::new(CancellationToken::new()); let mut owner=CarrierOwner::new(instance,one,&host);
        let bound=owner.registration().bind_declared(&ResourceDeclaration::ControlledNodeCarrier{
            endpoint:endpoint.clone(),dial:Default::default(),max_sockets:one},&scope.registrar(),&host).unwrap();
        owner.start(deadline()).await.unwrap();
        let egress=Arc::new(zero_platform_tokio::EgressInterfaceControl::default());
        let binding=UpstreamBinding{prefix:vec![],dial:Default::default(),network_generation:zero_core::component::NetworkGeneration(egress.generation())};
        let facts=facts(&endpoint); let end=Instant::now()+std::time::Duration::from_secs(1);
        let demand=Arc::new(Demand(CancellationToken::new(),Some(end)));
        let transport=UpstreamTransport::new(binding.clone(),Arc::new(Root{bound,host,peer,binding,egress,
            caller:witness.clone(),demand:demand.clone(),facts:facts.clone()}));
        let transfers=Arc::new(Mutex::new(Vec::new()));
        let role=Outbound{inner:erase_stream(RawRole(endpoint.clone())),tasks:Some(scope.registrar()),
            cleanup_network:None,process:None,endpoint:endpoint.clone(),dial:Default::default(),grants:owner.grants(),transfers:transfers.clone(),limit:one};
        let opening=role.connect(ConnectRequest{destination:endpoint},OpenContext{identity:RequestIdentity::Business(facts),
            deadline:Some(end),cancel:demand.0.clone(),upstream:transport,observer:None});
        tokio::pin!(opening);
        let receipt=tokio::select!{
            result=&mut opening=>panic!("original setup should reach held Host ACK before this deadline: {:?}",result.err()),
            receipt=scope.next_receipt()=>receipt.unwrap(),
        };
        assert!(receipt.result().is_ok());
        let actual=receipt.joined().clone(); assert!(!actual.registration_released());
        let deferred=scope.defer_callbacks(); scope.acknowledge(&receipt).unwrap();
        assert!(!actual.registration_released());
        let error=opening.await.err().unwrap();
        assert_eq!(error.reason,Reason::Timeout);
        assert_eq!(error.delivery,zero_platform_tokio::contract::DeliveryState::Unknown);
        let mut source=std::error::Error::source(&error); let mut elapsed=false;
        for _ in 0..32 {
            let Some(current)=source else {break};
            if current.downcast_ref::<tokio::time::error::Elapsed>().is_some(){elapsed=true;break;}
            source=current.source();
        }
        assert!(elapsed,"real deadline source survives");
        let transfer=transfers.lock().unwrap()[0].clone();
        assert!(!transfer.preparation.as_ref().unwrap().is_published());
        assert!(!witness.is_joined(),"expired caller was never falsely joined");
        let native=transfer.adoption.retire_native(deadline()).await.unwrap();
        assert!(native.report().is_complete() && native.closed().unwrap().matches(&transfer.adoption));
        assert_eq!(owner.retained(),1);
        assert_eq!(owner.grants().reserve().err().unwrap().reason,Reason::Capacity);
        drop(deferred);
        assert!(transfer.adoption.retire(deadline()).await.unwrap().is_complete());
        assert_eq!(owner.retained(),0);
        assert!(transfer.preparation.as_ref().unwrap().matches_unpublished_closed(&transfer.adoption.users_closed().unwrap()));
        assert!(!transfer.adoption.users_closed().unwrap().matches_caller(&witness));
        stop.send(()).unwrap(); let receipt=caller.next_receipt().await.unwrap(); assert!(receipt.result().is_ok()); caller.acknowledge(&receipt).unwrap();
        assert!(owner.shutdown(deadline()).await.unwrap().is_complete()); scope.begin_shutdown();caller.begin_shutdown();assert!(scope.is_drained()&&caller.is_drained());
        server.await.unwrap();
    }).await.unwrap();
}
