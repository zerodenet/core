use super::*;
use std::{
    future::{poll_fn, Future},
    task::Poll,
};
#[tokio::test]
async fn ordinary_vmess_grpc_relay_pending_prefix_replacement_retires_actual_consumer_before_new_owner(
) {
    timeout(Duration::from_secs(25),async {
        let mut old_prefix=Prefix::new(Kind::Socks,true).await;let next=Prefix::new(Kind::Socks,false).await;
        let (proxy,server)=start_config(false,false,"aes-128-gcm","node",vec![b"owned".to_vec()],1,false,|config|configure(config,&[old_prefix.outbound("first")],false)).await;
        assert_standard_relay(&proxy);let config=proxy.engine().runtime_snapshot().config().clone();let running=proxy.spawn();
        let handle=crate::ProxyHandle::new(zero_engine::EngineHandle::new(proxy.engine().clone()),proxy.clone());
        handle.apply_runtime_config_and_wait((*config).clone(),Duration::from_secs(5)).await.unwrap();
        let plan=proxy.tcp_runtime_services().components.clone().unwrap();let prior_slot=plan.configured("node").unwrap();
        let registration=original(&proxy);let mut dispatch=BoundDispatch::new(&proxy, "incoming").await.unwrap();
        let pending = dispatch.operation_observation();
        let (replacing, native, consumer, donor) = {
        let opening=dispatch.dispatch(input(53,b"owned"));tokio::pin!(opening);
        tokio::select! {biased;_=&mut opening=>panic!("CONNECT response has not been sent"),_=old_prefix.request()=>{}}
        poll_fn(|cx|{assert!(opening.as_mut().poll(cx).is_pending(),"actual CONNECT response wait must still be pending");Poll::Ready(())}).await;
        pending.wait().await;
        let observed=registration.observe().unwrap();assert_eq!(observed.retained(),1);
        assert!(!observed.slots()[0].physical_view_live());assert!(observed.slots()[0].native().is_none());assert!(observed.slots()[0].driver().is_none());
        let consumer=observed.slots()[0].original().clone();
        let donor=donor_owner(&proxy,"first");let observed=donor.observe().unwrap();assert_eq!(observed.retained(),1);
        assert!(observed.slots()[0].physical_view_live());assert!(observed.slots()[0].native().is_some());
        let native=observed.slots()[0].original().clone();assert!(native.closed().is_none());
        let mut candidate=(*config).clone();candidate.outbounds[1]=next.outbound("first");
        let replacing=tokio::spawn(async move {handle.apply_runtime_config_and_wait(candidate,Duration::from_secs(10)).await});
        assert!(opening.await.is_err());
        (replacing, native, consumer, donor)
        };
        dispatch.finish_all().await;
        replacing.await.unwrap().unwrap();
        assert!(native.matches_closed(&native.closed().unwrap()));assert!(native.closed().unwrap().had_native_acquisition());
        assert!(consumer.matches_closed(&consumer.closed().unwrap()));assert!(!consumer.closed().unwrap().had_native_acquisition());
        assert!(donor.closed().unwrap().matches(&donor));assert!(donor.users_closed().unwrap().matches(&donor));
        assert!(registration.closed().unwrap().matches(&registration));
        assert_eq!(next.count.load(std::sync::atomic::Ordering::SeqCst),0);
        let fresh_plan=proxy.tcp_runtime_services().components.clone().unwrap();assert_ne!(prior_slot.instance_id(),fresh_plan.configured("node").unwrap().instance_id());
        assert!(prior_slot.datagram(Instant::now()+Duration::from_secs(3)).await.is_err());
        let mut fresh=BoundDispatch::new(&proxy, "fresh").await.unwrap();fresh.dispatch(input(53,b"owned")).await.unwrap();assert_eq!(response(&mut fresh).await.2.as_ref(),b"owned");
        close_flow(&proxy,fresh).await;server.finish().await;running.shutdown().await.unwrap();old_prefix.close().await;next.close().await;
    }).await.unwrap();
}
