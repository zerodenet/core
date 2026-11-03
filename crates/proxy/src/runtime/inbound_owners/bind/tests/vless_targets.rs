//! Real production factory, original target/native receipts and no-overlap restart.
use super::*;
use zero_platform_tokio::contract::stream::BoxStream;
#[tokio::test]
async fn vless_fallback_tcp_and_unix_originals_join_and_close_before_new_source() {
    timeout(Duration::from_secs(20),async {
        for unix in [false,true] {
            let tcp=TcpListener::bind("127.0.0.1:0").await.unwrap();
            let directory=tempfile::tempdir().unwrap();let path=directory.path().join("fallback.sock");
            let local=tokio::net::UnixListener::bind(&path).unwrap();
            let endpoint=if unix{serde_json::json!({"type":"unix","path":path})}else{serde_json::json!({"type":"tcp","server":"127.0.0.1","port":tcp.local_addr().unwrap().port()})};
            let config=RuntimeConfig::parse(&serde_json::json!({"inbounds":[{"tag":"first","listen":{"address":"127.0.0.1","port":0},"protocol":{"type":"vless","users":[{"id":"11111111-2222-3333-4444-555555555555"}],"fallback":{"rules":[{"destination":endpoint}]}}}],"route":{"final":{"type":"direct"}}}).to_string()).unwrap();
            let proxy=Proxy::new(config).unwrap();let config=proxy.engine().config();
            proxy.protocols.start_components(config.clone(),Instant::now()+Duration::from_secs(3)).await.unwrap();proxy.protocols.commit_components(&config,||Ok(())).unwrap();
            let factory=crate::runtime::route_runtime::InboundListenerRuntimeFactory::new(crate::runtime::route_runtime::SharedIngressRuntimeServices::new(proxy.tcp_runtime_services()),proxy.inbound_owners.clone());
            for _generation in 0..2 {
                let prepared=proxy.protocols.prepare_inbound_bind(config.inbounds[0].clone(),None).unwrap().expect("actual standard registration");
                let (stop,_)=watch::channel(false);
                let mut lease=proxy.inbound_owners.reserve_listener("first",&config,&proxy.protocols,prepared,stop.clone(),NonZeroUsize::new(1).unwrap()).unwrap();lease.bind_tcp().unwrap();
                let (definition,listener,installed)=promote_tcp(lease.finish().unwrap());let address=listener.local_addr().unwrap();let owner=installed.owner();
                let recipe=definition;
                assert!(recipe.spec().resources.iter().any(|resource|matches!(resource,ResourceDeclaration::InboundTargets{..})));
                let upstream=factory.for_inbound("first".into()).route_factory().for_connection(None).upstream_services();
                let (source,peers)=installed.build_inbound(recipe,NativeListenerView::Tcp(listener),Some(upstream)).unwrap();
                let runtime=factory.for_inbound("first".into()).with_inbound_instance(owner.identity()).with_readiness(owner.readiness()).with_datagram_source(peers);
                owner.spawn_driver(crate::runtime::inbound_operation::source::run(source,runtime,stop.subscribe(),NonZeroUsize::new(4096).unwrap(),"vless")).unwrap();owner.seal_listener_installation();
                let business=async {
                    owner.wait_ready().await.unwrap();
                    let mut client=TcpStream::connect(address).await.unwrap();let request=b"GET /original HTTP/1.1\r\nHost: test\r\n\r\n";client.write_all(request).await.unwrap();
                    let mut target:BoxStream=if unix{Box::pin(local.accept().await.unwrap().0)}else{Box::pin(tcp.accept().await.unwrap().0)};
                    let mut received=vec![0;request.len()];target.read_exact(&mut received).await.unwrap();assert_eq!(received,request);
                    target.write_all(b"real").await.unwrap();let mut reply=[0;4];client.read_exact(&mut reply).await.unwrap();assert_eq!(&reply,b"real");
                    assert!(proxy.engine().active_sessions().is_empty());assert!(proxy.engine().completed_sessions().is_empty(),"website fallback never mints a business SessionId");
                    let mut byte=[0];let mut waiting=Box::pin(target.read(&mut byte));
                    std::future::poll_fn(|cx|{assert!(waiting.as_mut().poll(cx).is_pending());Poll::Ready(())}).await;
                    assert!(owner.terminal().is_none());stop.send_replace(true);
                    assert_eq!(waiting.await.unwrap(),0,"original target peer observes the real native close");
                };
                let (observed,())=tokio::join!(owner.observe_driver(),business);observed.unwrap();
                let receipt=proxy.inbound_owners.confirm_shutdown("first",Instant::now()+Duration::from_secs(3)).await.unwrap().unwrap();
                assert!(receipt.terminal().failure().is_none(),"{:?}",receipt.terminal().failure());
                assert_eq!(receipt.terminal().joined_tasks(),3,"source, actual accepted primary and actual target driver");
                assert_eq!(receipt.terminal().closed_io_receipts(),3,"original listener, accepted native and original target provider");
                proxy.inbound_owners.release_closed(receipt,|_|Ok(())).unwrap();
            }
        }
    }).await.unwrap();
}
