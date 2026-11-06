//! Genuine DNS consumers of the existing physical VLESS gRPC pool.
use super::*;
use std::{sync::Arc, time::Duration};
use zero_platform_tokio::controlled_carrier::CarrierOrigin;
mod fixture;
use fixture::*;
fn deadline() -> tokio::time::Instant {
    tokio::time::Instant::now() + Duration::from_secs(3)
}
fn returned(child: &CarrierOrigin) {
    let closed = child.closed().expect("same actual DNS child logical close");
    assert!(closed.matches(child) && !closed.had_native_acquisition());
}
fn native_closed(original: &CarrierOrigin) {
    let native = original.native_origin();
    let closed = native
        .closed()
        .expect("same actual native gRPC original closed");
    assert!(closed.matches(native) && closed.had_native_acquisition());
    let driver = original.driver().expect("same original H2 driver");
    assert!(driver.is_joined());
    driver.try_joined().unwrap().result().unwrap();
}
#[tokio::test]
async fn ordinary_vless_grpc_dns_success_cancel_and_reuse_keep_the_same_physical_pool() {
    tokio::time::timeout(Duration::from_secs(25),async {
        for multi in [false,true] {
            let remote=Remote::new(multi).await;
            let mut dns=crate::dns_fixture::Dns::new().await;
            let (proxy,running)=start(&remote,multi,&dns).await;
            let mut physical:Option<CarrierOrigin>=None;
            for (index,cancel) in [false,true,false].into_iter().enumerate() {
                let mut query=query(&proxy,&format!("{multi}-{index}.vless.grpc.dns.test"));
                let request=tokio::select! {request=dns.request()=>request,result=&mut query=>panic!("ordinary VLESS gRPC failed before actual DNS delivery: {result:?}")};
                assert!(request.tcp);
                let caller=pending(&proxy).await;
                let child=caller.original.clone().unwrap();
                let donor=child.parent().expect("actual same-pool donor");
                let driver=donor.driver().expect("actual physical H2 driver");
                assert!(!driver.is_joined() && donor.closed().is_none());
                assert!(!caller.task.scope_identity().same_scope(&driver.scope_identity()));
                if let Some(prior)=&physical {
                    assert!(donor.same_original(prior));
                    assert!(driver.same_registration(prior.driver().unwrap()));
                } else {physical=Some(donor.clone());}
                if cancel {query.abort();assert!(query.await.unwrap_err().is_cancelled());drop(request);}
                else {request.answer(false,false);assert_eq!(query.await.unwrap().unwrap(),vec![IpAddress::V4([127,0,0,1])]);}
                tokio::time::timeout_at(deadline(),caller.task.wait()).await.unwrap().result().unwrap();
                returned(&child);
                assert!(!driver.is_joined() && donor.closed().is_none(),"query logical return cannot close the shared physical original");
            }
            assert!(proxy.engine().active_sessions().is_empty() && proxy.engine().completed_sessions().is_empty());
            running.shutdown().await.unwrap();
            let original=physical.unwrap();native_closed(&original);
            assert!(original.owner().closed().unwrap().matches(original.owner()));
            assert!(original.owner().users_closed().unwrap().matches(original.owner()));
            remote.close().await;dns.close().await;
        }
    }).await.unwrap();
}

#[tokio::test]
async fn ordinary_vless_grpc_dns_network_replacement_retires_the_same_physical_pool_before_new_io()
{
    tokio::time::timeout(Duration::from_secs(25),async {
        let remote=Remote::new(true).await;
        let mut dns=crate::dns_fixture::Dns::new().await;
        let (proxy,running)=start(&remote,true,&dns).await;
        let mut warm=query(&proxy,"before.network.vless.grpc.test");
        let request=tokio::select! {request=dns.request()=>request,result=&mut warm=>panic!("warm query failed before actual DNS delivery: {result:?}")};
        let prior=pending(&proxy).await;
        let old=prior.original.as_ref().unwrap().parent().unwrap().clone();
        request.answer(false,false);warm.await.unwrap().unwrap();
        tokio::time::timeout_at(deadline(),prior.task.wait()).await.unwrap().result().unwrap();
        returned(prior.original.as_ref().unwrap());
        assert!(old.closed().is_none() && !old.driver().unwrap().is_joined());
        proxy.egress_interface.invalidate_network();
        let mut next=query(&proxy,"after.network.vless.grpc.test");
        let request=tokio::select! {request=dns.request()=>request,result=&mut next=>panic!("network replacement failed before actual DNS delivery: {result:?}")};
        native_closed(&old);
        let caller=pending(&proxy).await;
        let child=caller.original.clone().unwrap();
        let fresh=child.parent().unwrap().clone();
        assert!(!fresh.same_original(&old));
        assert!(!fresh.driver().unwrap().same_registration(old.driver().unwrap()));
        request.answer(false,false);next.await.unwrap().unwrap();
        tokio::time::timeout_at(deadline(),caller.task.wait()).await.unwrap().result().unwrap();
        returned(&child);
        assert!(proxy.engine().active_sessions().is_empty() && proxy.engine().completed_sessions().is_empty());
        running.shutdown().await.unwrap();native_closed(&fresh);
        remote.close().await;dns.close().await;
    }).await.unwrap();
}

mod wire_pending;
