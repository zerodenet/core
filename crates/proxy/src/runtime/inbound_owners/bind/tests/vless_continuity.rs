//! Real Proxy registration, actual UDP source reuse and original native receipts.
use super::*;
use tokio::net::UdpSocket;
use zero_platform_tokio::contract::stream::BoxStream;
const USER: &str = "11111111-2222-3333-4444-555555555555";
async fn connected(
    endpoint: std::net::SocketAddr,
    grpc: bool,
) -> (BoxStream, Option<zero_transport::grpc::GrpcPool>) {
    let socket = TcpStream::connect(endpoint).await.unwrap();
    let (mut io, pool): (BoxStream, _) = if grpc {
        let pool = zero_transport::grpc::GrpcPool::default();
        let profile = zero_transport::profile::OwnedGrpcProfile {
            service_names: vec!["zero.vless/Tun".into()],
            ..Default::default()
        };
        let io = pool
            .open(&profile, "localhost", || async {
                Ok::<_, zero_transport::RuntimeError>(socket)
            })
            .await
            .unwrap();
        (Box::pin(io), Some(pool))
    } else {
        (Box::pin(socket), None)
    };
    let mut header = vec![0];
    header.extend(vless::parse_uuid(USER).unwrap());
    header.extend([0, 3]);
    io.write_all(&header).await.unwrap();
    io.flush().await.unwrap();
    let mut response = [0; 2];
    io.read_exact(&mut response).await.unwrap();
    assert_eq!(response, [0, 0]);
    (io, pool)
}
fn packet(port: u16, payload: &[u8]) -> Vec<u8> {
    let mut metadata = vec![0, 7, 1, 1, 2];
    metadata.extend(port.to_be_bytes());
    metadata.extend([1, 127, 0, 0, 1]);
    metadata.extend([1, 2, 3, 4, 5, 6, 7, 8]);
    let mut frame = (metadata.len() as u16).to_be_bytes().to_vec();
    frame.extend(metadata);
    frame.extend((payload.len() as u16).to_be_bytes());
    frame.extend(payload);
    frame
}
async fn exchange(io: &mut BoxStream, target: &UdpSocket, payload: &[u8]) -> std::net::SocketAddr {
    io.write_all(&packet(target.local_addr().unwrap().port(), payload))
        .await
        .unwrap();
    io.flush().await.unwrap();
    let mut buffer = [0; 256];
    let (size, source) = target.recv_from(&mut buffer).await.unwrap();
    assert_eq!(&buffer[..size], payload);
    target.send_to(payload, source).await.unwrap();
    let len = io.read_u16().await.unwrap();
    let mut metadata = vec![0; len as usize];
    io.read_exact(&mut metadata).await.unwrap();
    assert_eq!(&metadata[..4], &[0, 7, 2, 1]);
    assert_eq!(io.read_u16().await.unwrap(), payload.len() as u16);
    let mut response = vec![0; payload.len()];
    io.read_exact(&mut response).await.unwrap();
    assert_eq!(response, payload);
    source
}
async fn wait(description: &str, mut condition: impl FnMut() -> bool) {
    timeout(Duration::from_secs(3), async {
        while !condition() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("pending: {description}"));
}

#[tokio::test]
async fn vless_continuity_survives_real_tcp_and_grpc_carrier_close_then_retires_original_udp() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("zero_proxy=debug,vless=debug")
        .with_test_writer()
        .try_init();
    timeout(Duration::from_secs(20),async{
        for grpc in [false,true]{
            let target=UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let mut protocol=serde_json::json!({"type":"vless","users":[{"id":USER,"principal_key":"continuity:user"}]});
            if grpc{protocol["grpc"]=serde_json::json!({"service_names":["zero.vless/Tun"]});}
            let config=RuntimeConfig::parse(&serde_json::json!({"inbounds":[{"tag":"first","listen":{"address":"127.0.0.1","port":0},"protocol":protocol}],"route":{"final":{"type":"direct"}}}).to_string()).unwrap();
            let proxy=Proxy::new(config).unwrap();let config=proxy.engine().config();
            proxy.protocols.start_components(config.clone(),Instant::now()+Duration::from_secs(3)).await.unwrap();proxy.protocols.commit_components(&config,||Ok(())).unwrap();
            let prepared=proxy.protocols.prepare_inbound_bind(config.inbounds[0].clone(),None).unwrap().expect("ordinary actual VLESS standard registration");
            let (shutdown,_)=watch::channel(false);
            let mut lease=proxy.inbound_owners.reserve_listener("first",&config,&proxy.protocols,prepared,shutdown.clone(),NonZeroUsize::new(1).unwrap()).unwrap();
            let grant=lease.original_grant();lease.bind_tcp().unwrap();
            let (definition,listener,installed)=promote_tcp(lease.finish().unwrap());let endpoint=listener.local_addr().unwrap();let owner=installed.owner();
            let recipe=definition;
            let (source,peers)=installed.build_inbound(recipe, NativeListenerView::Tcp(listener), None).unwrap();
            let factory=crate::runtime::route_runtime::InboundListenerRuntimeFactory::new(crate::runtime::route_runtime::SharedIngressRuntimeServices::new(proxy.tcp_runtime_services()),proxy.inbound_owners.clone());
            let runtime=factory.for_inbound("first".into()).with_inbound_instance(owner.identity()).with_readiness(owner.readiness()).with_datagram_source(peers);
            owner.spawn_driver(crate::runtime::inbound_operation::source::run(source,runtime,shutdown.subscribe(),NonZeroUsize::new(4096).unwrap(),"vless")).unwrap();owner.seal_listener_installation();
            let exercise=async{
                owner.wait_ready().await.unwrap();
                let (mut first,pool)=connected(endpoint,grpc).await;let original=exchange(&mut first,&target,b"one").await;
                drop(first);if let Some(pool)=pool{pool.retire();}
                let native=match grant.as_ref(){native::NativeBindGrant::Tcp(native)=>native,_=>panic!("actual TCP original")};
                wait("first original carrier",||native.accepted_evidence().is_some_and(|e|e.confirmed_native_closes==1)).await;
                assert_eq!(owner.peer_tasks_for_test().closed_io_receipts_count(),0,"old TCP original closed while association UDP remains genuinely live");
                let (mut second,pool)=connected(endpoint,grpc).await;assert_eq!(exchange(&mut second,&target,b"two").await,original,"same original UDP socket survives actual carrier change");
                assert_eq!(proxy.engine().active_sessions().len(),1,"same existing business flow across carriers");
                second.write_all(&[0,4,0,7,3,0]).await.unwrap();second.flush().await.unwrap();
                wait("explicit END original UDP close",||owner.peer_tasks_for_test().closed_io_receipts_count()==1).await;
                let records=proxy.engine().completed_sessions();assert_eq!(records.len(),1);assert_eq!((records[0].bytes_up,records[0].bytes_down),(6,6));assert_eq!(records[0].auth.as_ref().unwrap().principal_key.as_deref(),Some("continuity:user"));
                drop(second);if let Some(pool)=pool{pool.retire();}
                let (mut third,pool)=connected(endpoint,grpc).await;let _=exchange(&mut third,&target,b"new").await;
                drop(third);if let Some(pool)=pool{pool.retire();}
                wait("third original carrier",||native.accepted_evidence().is_some_and(|e|e.confirmed_native_closes==3)).await;
                // Pending detached business I/O is still owned; the configuration
                // transaction cannot release this listener before native cleanup.
                assert!(owner.terminal().is_none());shutdown.send_replace(true);
            };
            let (result,())=tokio::join!(owner.observe_driver(),exercise);result.unwrap();
            let receipt=proxy.inbound_owners.confirm_shutdown("first",Instant::now()+Duration::from_secs(3)).await.unwrap().unwrap();
            assert!(receipt.terminal().failure().is_none(),"{:?}",receipt.terminal().failure());
            assert_eq!(owner.peer_tasks_for_test().closed_io_receipts_count(),2,"two distinct original UDP providers closed");
            assert_eq!(receipt.terminal().closed_io_receipts(),4,"listener, aggregate receipt for three original TCP carriers and two original UDP receipts");
            let old_identity=receipt.terminal().identity();
            proxy.inbound_owners.release_closed(receipt,|_|Ok(())).unwrap();
            // Build/start a fresh real source only after consuming that exact
            // original listener/association receipt, with the same one Proxy.
            let prepared=proxy.protocols.prepare_inbound_bind(config.inbounds[0].clone(),None).unwrap().unwrap();
            let (next_stop,_)=watch::channel(false);
            let mut next=proxy.inbound_owners.reserve_listener("first",&config,&proxy.protocols,prepared,next_stop.clone(),NonZeroUsize::new(1).unwrap()).unwrap();
            next.bind_tcp().unwrap();
            let (definition,listener,installed)=promote_tcp(next.finish().unwrap());
            let replacement=installed.owner();assert_ne!(replacement.identity(),old_identity);
            let recipe=definition;
            let (source,peers)=installed.build_inbound(recipe, NativeListenerView::Tcp(listener), None).unwrap();
            let runtime=factory.for_inbound("first".into()).with_inbound_instance(replacement.identity()).with_readiness(replacement.readiness()).with_datagram_source(peers);
            replacement.spawn_driver(crate::runtime::inbound_operation::source::run(source,runtime,next_stop.subscribe(),NonZeroUsize::new(4096).unwrap(),"vless")).unwrap();replacement.seal_listener_installation();
            let (observed,())=tokio::join!(replacement.observe_driver(),async{replacement.wait_ready().await.unwrap();next_stop.send_replace(true);});observed.unwrap();
            let next_receipt=proxy.inbound_owners.confirm_shutdown("first",Instant::now()+Duration::from_secs(3)).await.unwrap().unwrap();
            assert_eq!(next_receipt.terminal().closed_io_receipts(),1,"the replacement closes only its own original listener");

        }
    }).await.unwrap();
}
