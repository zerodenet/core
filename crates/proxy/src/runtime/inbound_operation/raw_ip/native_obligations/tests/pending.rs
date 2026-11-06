//! A real raw caller finalizes Service preparation without inventing Business custody.
use super::*;
use crate::runtime::{inbound_owners::InboundOwnerRegistry, Proxy};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
use zero_core::{
    component::{ComponentId, ComponentInstanceId, NetworkGeneration, ResourceGeneration},
    request::Destination,
    Address,
};
use zero_platform_tokio::{
    contract::{
        component::ResourceDeclaration,
        network::{CarrierContext, UpstreamBinding, UpstreamTransport},
        scope::TaskScope,
    },
    controlled_carrier::{CarrierOwner, NativeCarrierOpener},
};
mod receiver;
#[tokio::test]
async fn actual_raw_caller_join_drives_pending_native_cleanup_without_erasing_uncovered_gateway() {
    actual(false).await;
}
#[tokio::test]
async fn actual_bound_receiver_retains_donor_after_pending_upgrade_and_closes_it_once() {
    actual(true).await;
}
async fn actual(bound_receiver: bool) {
    timeout(Duration::from_secs(8),async {
        let proxy=Proxy::new(zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap()).unwrap();
        let registry=Arc::new(InboundOwnerRegistry::default());
        let (stop,_)=watch::channel(false);
        let installed=registry.install("raw-pending",&proxy.protocols,1,stop).unwrap();
        let group=installed.child_tasks(NonZeroUsize::new(1).unwrap()).unwrap();
        let host=installed.tasks();let owner=installed.owner();
        let native=NativeObligations::new(1,0,0);installed.register(native.resource()).unwrap();
        let mut permit=native.reserve(ChildKind::Tcp).unwrap();let registration=permit.registration();
        let listener=TcpListener::bind("127.0.0.1:0").await.unwrap();let peer=listener.local_addr().unwrap();
        let server=tokio::spawn(async move {
            let (mut io,_)=listener.accept().await.unwrap();let mut request=[0;3];io.read_exact(&mut request).await.unwrap();assert_eq!(&request,b"raw");
            let mut byte=[0;1];match io.read(&mut byte).await {Ok(0)|Err(_)=>{},result=>panic!("same original must close: {result:?}")}
        });
        let cap=NonZeroUsize::new(1).unwrap();
        let mut scope=TaskScope::new(tokio::runtime::Handle::current(),cap);
        let cancel=Arc::new(zero_platform_tokio::contract::CancellationToken::new());
        let instance=ComponentInstanceId {component:ComponentId(974),generation:ResourceGeneration(1)};
        let mut carriers=CarrierOwner::new(instance,cap,&cancel);
        let endpoint=Destination::new(Address::Ipv4([127,0,0,1]),peer.port());
        let actual=carriers.registration();actual.bind_declared(&ResourceDeclaration::ControlledCarrierPool {endpoint:endpoint.clone(),dial:Default::default(),datagram:false,max_sockets:cap},&scope.registrar(),&cancel).unwrap();
        carriers.start(Instant::now()+Duration::from_secs(2)).await.unwrap();
        let egress=Arc::new(zero_platform_tokio::EgressInterfaceControl::default());
        let binding=UpstreamBinding {prefix:vec![],dial:Default::default(),network_generation:NetworkGeneration(egress.generation())};
        let upstream=UpstreamTransport::new(binding.clone(),Arc::new(NativeCarrierOpener::new(instance,binding.clone(),peer,egress,None).unwrap()));
        let mut grant=carriers.grants().reserve().unwrap();let view=grant.claim_transferred_view().unwrap();let adoption=grant.adoption();
        let source_adoption=adoption.clone();let source_registration=registration.clone();
        let mut receiver=if bound_receiver {Some(receiver::Receiver::new(endpoint.clone(),instance,binding.network_generation).await)} else {None};
        let receiver_setup=receiver.as_mut().map(|receiver|receiver.take());
        let engine=proxy.engine().clone();
        let (ready,started)=oneshot::channel();let (finish_parent,parent_finished)=oneshot::channel();
        owner.spawn_driver(async move {parent_finished.await.map_err(std::io::Error::other)?;Ok(())}).unwrap();
        let monitor={let owner=owner.clone();tokio::spawn(async move {owner.observe_driver().await})};
        group.spawn_bound(&host,move |caller|async move {
            caller.wait_bound().await.map_err(std::io::Error::other)?;
            permit.bind_caller(caller.clone()).map_err(std::io::Error::other)?;permit.enter().map_err(std::io::Error::other)?;
            source_registration.declare_gateway(false).map_err(std::io::Error::other)?;
            source_registration.observe_pending_carrier(source_adoption).map_err(std::io::Error::other)?;
            let stop=caller.cancellation();
            let io=upstream.acquire_controlled_stream(endpoint.clone(),CarrierContext {session:None,deadline:Some(Instant::now()+Duration::from_secs(3)),cancel:stop.clone()},grant).await.map_err(std::io::Error::other)?;
            let (mut io,_view)=if let Some(setup)=receiver_setup {
                setup.adopt(engine,source_registration.clone(),endpoint,io,view)?
            } else {(io,view)};
            io.write_all(b"raw").await?;
            let mut byte=[0;1];let reading=io.read(&mut byte);tokio::pin!(reading);
            std::future::poll_fn(|cx| {assert!(reading.as_mut().poll(cx).is_pending());Poll::Ready(())}).await;
            ready.send(caller).unwrap_or_else(|_|panic!("actual raw caller receiver"));
            tokio::select! {biased;_=stop.cancelled()=>{},result=&mut reading=>panic!("actual read must remain pending: {result:?}")};
            permit.consume_business_result(Ok(()))
        }).unwrap();
        let caller=started.await.unwrap();
        assert!(adoption.closed().is_none());assert_eq!(native.0.lock().unwrap().records.get(&registration.id).unwrap().pending_carriers.len(),1);
        caller.cancellation().cancel();
        let joined=caller.wait_joined(Instant::now()+Duration::from_secs(2)).await.unwrap();
        let witness=registration.caller_witness().unwrap();assert!(witness.try_joined().is_some_and(|actual|actual.id()==joined.task()));
        assert!(adoption.closed().is_none(),"actual caller join is not native closure");
        if let Some(receiver)=&mut receiver {
            {
                let state=native.0.lock().unwrap();let record=state.records.get(&registration.id).unwrap();
                assert!(record.carrier.as_ref().unwrap().same_slot(&receiver.original));
            }
            assert!(registration.caller_witness().is_some_and(|caller|caller.same_registration(&witness)));
            let held=receiver.original.retained_loan().expect("actual controller strongly installed in the bound receiver");
            assert!(held.retains(&adoption));assert!(receiver.original.retained_driver().is_none());
            assert!(registration.settle_pending(Instant::now()+Duration::from_secs(2)).await.unwrap());
            assert!(adoption.closed().is_none(),"pending donor must remain owned by the actual bound receiver");
            let state=native.0.lock().unwrap();let record=state.records.get(&registration.id).unwrap();
            assert!(record.pending_carriers.is_empty());assert!(record.carrier.as_ref().unwrap().same_slot(&receiver.original));drop(state);
            assert!(receiver.owner.shutdown(Instant::now()+Duration::from_secs(2)).await.unwrap().is_complete());
            assert!(receiver.scope.shutdown(Instant::now()+Duration::from_secs(2)).await.is_complete());
            assert!(receiver.original.closed().unwrap().matches(&receiver.original));
            assert!(receiver.original.users_closed().unwrap().matches_caller(&witness));
        }
        native.settle_joined(Instant::now()+Duration::from_secs(2)).await.unwrap();
        let closed=adoption.closed().unwrap();assert!(closed.matches(&adoption)&&closed.had_native_acquisition());
        let users=adoption.users_closed().unwrap();assert!(users.matches(&adoption));assert!(!users.matches_caller(&witness),"pending transferred Service was never a bound Business user");
        {
            let state=native.0.lock().unwrap();let record=state.records.get(&registration.id).unwrap();
            assert!(record.pending_carriers.is_empty());assert!(record.uncovered_gateway);assert_eq!(record.carrier.is_some(),bound_receiver);
        }
        assert_eq!(native.reserve(ChildKind::Tcp).err().unwrap().reason,Reason::Capacity,"pending cleanup cannot certify an uncovered gateway");
        assert!(carriers.shutdown(Instant::now()+Duration::from_secs(2)).await.unwrap().is_complete());
        assert!(scope.shutdown(Instant::now()+Duration::from_secs(2)).await.is_complete());
        assert!(actual.closed().unwrap().matches(&actual));server.await.unwrap();
        finish_parent.send(()).unwrap();let failure=monitor.await.unwrap().unwrap_err();
        assert!(matches!(failure,zero_engine::EngineError::Io(_)));
        assert!(registry.confirm_shutdown("raw-pending",Instant::now()+Duration::from_secs(2)).await.is_err());
    }).await.unwrap();
}
