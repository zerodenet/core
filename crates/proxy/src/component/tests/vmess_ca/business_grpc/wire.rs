use super::*;
#[tokio::test]
async fn real_engine_host_file_and_same_vm_actor_preserve_two_fresh_business_tls_grpc_clients() {
    timeout(Duration::from_secs(12), async {
        for caller_kind in crate::component::tests::business_carrier::sources::Kind::all() {
        for multi_mode in [false,true] {
            let proxy=proxy();
            let directory=Directory::new();
            let path=directory.0.join("identity.pem");
            let listener=TcpListener::bind("127.0.0.1:0").await.unwrap();
            let peer=listener.local_addr().unwrap();
            let (tls_acceptor,pem)=tls_server(true);
            let mut tls=tls_profile(); tls.alpn=vec!["h2".into()];
            let grpc=OwnedGrpcProfile { service_names:vec!["actual.business/Tun".into()],multi_mode,..Default::default() };
            let runtime=vmess::transport::VmessTransportRuntime::default();
            let upstream=proxy.tcp_runtime_services().execution().upstream();
            let binding=UpstreamBinding { prefix:vec![],dial:Default::default(),network_generation:NetworkGeneration(upstream.network_generation()) };
            let options=vmess::component::grpc::Options::from_business_options_refs_at(input(peer.port(),&tls,&grpc),binding.clone(),Some(&directory.0),NonZeroUsize::new(pem.len()),one(),Duration::from_secs(2),&runtime).unwrap();
            let captured=Arc::new(Captured::default());
            let factory=Arc::new(Factory(captured.clone()));
            let spec=factory.describe(&options);
            assert_eq!(spec.task_limit,Some(one()));
            assert_eq!(spec.resources.len(),2);
            assert!(matches!(&spec.resources[0],ResourceDeclaration::File {path:actual,max_bytes} if actual==&path && *max_bytes==pem.len()));
            assert!(matches!(&spec.resources[1],ResourceDeclaration::ControlledOutboundStream {demand:ControlledStreamDemandClass::Business,max_sockets,..} if *max_sockets==one()));
            assert!(!spec.resources.contains(&ResourceDeclaration::BackgroundCarrier));
            let slot=Arc::new(ComponentSlot::from_recipe(ComponentId(1981),ResourceGeneration(1),ComponentRecipe::from_validated(factory,Arc::new(options)),Default::default(),"business-grpc"));
            assert_eq!(captured.reads.load(Ordering::SeqCst),0);
            assert!(!path.exists());
            std::fs::write(&path,pem).unwrap();
            let host=TestContext::new([slot.as_ref()]);
            host.drive(slot.start(deadline())).await.unwrap();
            slot.host_admission().unwrap().publish(std::iter::once(slot.admission_grant()));
            assert_eq!(captured.reads.load(Ordering::SeqCst),1);
            std::fs::remove_file(path).unwrap();
            let actual=captured.owner.lock().unwrap().as_ref().unwrap().clone();
            let role=host.drive(slot.stream(deadline())).await.unwrap();
            let (finish,finished)=tokio::sync::watch::channel(false);
            let server=tokio::spawn(async move {
                let mut tasks=tokio::task::JoinSet::new();
                let mut sources=Vec::new();
                for _ in 0..2 {
                    let (io,source)=listener.accept().await.unwrap();sources.push(source);
                    let acceptor=tls_acceptor.clone();let profile=grpc.clone();let mut finished=finished.clone();
                    tasks.spawn(async move {
                        let tls=acceptor.accept(io).await.unwrap();
                        assert_eq!(tls.get_ref().1.alpn_protocol(),Some(b"h2".as_slice()));
                        let incoming=zero_transport::grpc::accept_grpc_connection(tls,&profile).unwrap();
                        let stream=incoming.accept().await.unwrap();
                        let (facts,mut io)=inbound().accept_request_stream(vmess::inbound::VmessInbound,zero_platform_tokio::TcpRelayStream::new(stream)).await.unwrap();
                        assert_eq!(facts.destination,destination());
                        let mut payload=Vec::new();io.read_to_end(&mut payload).await.unwrap();assert_eq!(payload,b"business-payload");
                        io.write_all(b"reply-after-half-close").await.unwrap();io.shutdown().await.unwrap();
                        if !*finished.borrow() { finished.wait_for(|done|*done).await.unwrap(); }
                        incoming.close();
                    });
                }
                assert_ne!(sources[0],sources[1],"ordinary VMess keeps a fresh TCP/H2 client per demand");
                while let Some(result)=tasks.join_next().await { result.unwrap(); }
            });
            let observations=Arc::new(Observations::default());
            for _ in 0..2 {
                let mut caller = crate::component::tests::business_carrier::sources::Caller::new(&proxy, caller_kind).await;
                let (session, invocation) = (caller.session().clone(), caller.invocation().clone());
                assert!(invocation.deadline.is_none());
                {
                    let auth=session.auth.as_ref().unwrap();
                    assert_eq!(auth.scheme,"verified");
                    assert_eq!(auth.principal_key.as_deref(),Some("business-grpc"));
                    assert_eq!((auth.up_bps,auth.down_bps,auth.device_limit,auth.quota_remaining_bytes,auth.policy_revision),
                        (Some(4096),Some(8192),Some(1),Some(4096),Some(7)));
                }
                let demand=BusinessCarrierRequest::new(proxy.engine(),&slot,&invocation).unwrap();
                let endpoint=Destination::new(Address::Ipv4([127,0,0,1]),peer.port());
                let opener=Arc::new(NodeOpener::new(upstream.clone().with_observer(Some(observations.clone())),endpoint).with_business(demand));
                let mut connected=host.drive(role.connect(ConnectRequest {destination:destination()},OpenContext {
                    identity:invocation.identity.clone(),deadline:None,cancel:invocation.cancellation(),observer:Some(observations.clone()),upstream:UpstreamTransport::new(binding.clone(),opener.clone()),
                })).await.unwrap();
                assert_eq!(connected.peer_confirmation,PeerConfirmation::Deferred);
                assert!(opener.take_evidence().is_some());
                assert_eq!(observations.protocol.lock().unwrap().as_ref(),Some(&(0,REQUEST_LEN)));
                host.drive(async {
                    connected.io.write_all(b"business-payload").await.unwrap();connected.io.shutdown().await.unwrap();
                    let mut response=Vec::new();connected.io.read_to_end(&mut response).await.unwrap();assert_eq!(response,b"reply-after-half-close");
                }).await;
                assert_eq!(observations.facts.lock().unwrap().last(),Some(&(Some(SessionId(session.id)),ConnectFact::PeerConfirmed)));
                let observation=actual.observe().unwrap();
                assert_eq!(observation.slots().len(),1);
                let witness=observation.slots()[0].driver().unwrap().1.clone();
                drop(connected);
                let exit=host.drive(witness.wait()).await;
                assert_eq!(exit.class(),zero_platform_tokio::contract::scope::TaskClass::Auxiliary);
                assert!(exit.result().is_ok());
                {
                    assert!(!caller.witness().is_joined(), "transport exit is not caller completion");
                    caller.join().await;
                }
                assert!(actual.closed().is_none());
                proxy.engine().track_session(session.id).finish(zero_engine::SessionOutcome::DirectRelayed).unwrap();
            }
            assert_eq!(captured.reads.load(Ordering::SeqCst),1);
            assert!(observations.outer.load(Ordering::Relaxed)>2*REQUEST_LEN);
            let report:CloseReport=host.drive(slot.shutdown(deadline())).await.unwrap();
            assert!(report.is_complete(), "same actual caller joins, returned logical views, actual drivers and native controls must close the real graph: {report:?}");
            assert!(actual.closed().unwrap().matches(&actual));
            finish.send_replace(true);server.await.unwrap();
        }
        }
    }).await.unwrap();
}
