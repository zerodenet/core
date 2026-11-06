//! The related TCP control is not physical ancestry of the borrowed UDP socket.
use super::*;
use zero_platform_tokio::controlled_carrier::CarrierOrigin;
mod fixture;
mod pending_ack;
use fixture::Prefix;

#[tokio::test]
async fn cancelling_first_dns_query_preserves_both_actual_socks_originals_used_by_a_concurrent_mieru_udp_pool(
) {
    tokio::time::timeout(Duration::from_secs(25),async {
        let prefix=Prefix::new().await;
        let server=Server::packet(true).await;
        let mut dns=Dns::new().await;
        let mut settings=detoured(&dns,&server);
        let zero_config::OutboundProtocolConfig::Mieru { transport,.. }=&mut settings.outbounds[0].protocol else {unreachable!()};
        *transport=zero_config::MieruTransport::Udp;
        settings.outbounds.push(prefix.node.clone());
        settings.outbound_groups=zero_config::RuntimeConfig::parse(r#"{"outbounds":[{"tag":"socks","protocol":{"type":"direct"}},{"tag":"a","protocol":{"type":"direct"}}],"outbound_groups":[{"tag":"packet-chain","type":"relay","proxies":["socks","a"]}],"route":{"final":{"type":"direct"}}}"#).unwrap().outbound_groups;
        let zero_config::DnsServerConfig::Udp { detour,.. }=settings.runtime.dns.as_mut().unwrap().servers.get_mut("query").unwrap() else {unreachable!()};
        *detour=Some("packet-chain".into());
        let (proxy,running,_)=start(settings).await;
        let resolver=proxy.resolver.clone();
        let mut first=tokio::spawn(async move {resolver.resolve_real_type("first.related-control.test",1).await});
        stage("first query: awaiting real DNS request",&proxy,None);
        let first_request=tokio::select! {request=dns.request()=>request,result=&mut first=>panic!("first actual SOCKS/Mieru UDP DNS query failed: {result:?}")};
        stage("first query: real DNS request received",&proxy,None);
        assert!(first_request.tcp);
        let first_caller=pending(&proxy).await;
        let child=first_caller.original.clone().unwrap();
        let pool=child.parent().unwrap().clone();
        let udp=pool.native_origin().clone();
        let control=udp.related_control().expect("same actual UDP association retains its negotiated TCP original");
        assert!(udp.parent().is_none(),"related TCP control must not become false physical UDP encapsulation");
        assert!(!udp.same_original(&control));
        let resolver=proxy.resolver.clone();
        let mut second=tokio::spawn(async move {resolver.resolve_real_type("second.related-control.test",1).await});
        stage("second query: awaiting concurrent real DNS request",&proxy,Some([&pool,&udp,&control]));
        let second_request=tokio::select! {request=dns.request()=>request,result=&mut second=>panic!("same actual pool must serve another DNS query: {result:?}")};
        stage("second query: real DNS request received",&proxy,Some([&pool,&udp,&control]));
        assert!(second_request.tcp);
        let second_caller=tokio::time::timeout_at(deadline(),async {
            loop {
                if let Some(caller)=proxy.resolver.native_query_observations().into_iter().find(|caller| caller.io_pending && caller.original.is_some() && !caller.task.is_joined() && !caller.task.same_registration(&first_caller.task)) {break caller;}
                tokio::task::yield_now().await;
            }
        }).await.unwrap();
        assert!(second_caller.original.as_ref().unwrap().parent().unwrap().same_original(&pool));
        assert!(!first.is_finished() && !second.is_finished());
        stage("first query: cancel original caller",&proxy,Some([&pool,&udp,&control]));
        first.abort();assert!(first.await.unwrap_err().is_cancelled());drop(first_request);
        tokio::time::timeout_at(deadline(),first_caller.task.wait()).await.unwrap().result().unwrap();
        stage("first query: original caller joined",&proxy,Some([&pool,&udp,&control]));
        assert!(child.closed().unwrap().matches(&child));
        assert!(!child.closed().unwrap().had_native_acquisition());
        assert!(pool.closed().is_none() && udp.closed().is_none() && control.closed().is_none());
        assert!(!pool.driver().unwrap().is_joined());
        assert!(!second.is_finished(),"cancelling the original waiter cannot retire a live sibling's physical association");
        stage("second query: deliver real DNS response",&proxy,Some([&pool,&udp,&control]));
        second_request.answer(false,false);second.await.unwrap().unwrap();
        tokio::time::timeout_at(deadline(),second_caller.task.wait()).await.unwrap().result().unwrap();
        stage("second query: original caller joined",&proxy,Some([&pool,&udp,&control]));
        let resolver=proxy.resolver.clone();
        let mut third=tokio::spawn(async move {resolver.resolve_real_type("third.related-control.test",1).await});
        stage("third query: awaiting same-pool reuse real DNS request",&proxy,Some([&pool,&udp,&control]));
        let request=tokio::select! {request=dns.request()=>request,result=&mut third=>panic!("both original association handles must remain reusable: {result:?}")};
        stage("third query: real DNS request received",&proxy,Some([&pool,&udp,&control]));
        let third_caller=pending(&proxy).await;
        let same_pool=third_caller.original.as_ref().unwrap().parent().unwrap();
        assert!(same_pool.same_original(&pool));
        assert!(same_pool.native_origin().same_original(&udp));
        assert!(same_pool.native_origin().related_control().unwrap().same_original(&control));
        assert_eq!(server.accepts.load(Ordering::SeqCst),1);
        request.answer(false,false);third.await.unwrap().unwrap();
        assert!(proxy.engine().active_sessions().is_empty() && proxy.engine().completed_sessions().is_empty());
        stage("host shutdown: before actual original retirement",&proxy,Some([&pool,&udp,&control]));
        running.shutdown().await.unwrap();
        stage("host shutdown: originals retired",&proxy,Some([&pool,&udp,&control]));
        assert!(pool.driver().unwrap().is_joined());
        for original in [&udp,&control] { native_closed(original); }
        prefix.close().await;server.close().await;dns.close().await;
    }).await.unwrap();
}
fn native_closed(original: &CarrierOrigin) {
    let closed = original
        .closed()
        .expect("same original native controller closed");
    assert!(closed.matches(original));
    assert!(closed.had_native_acquisition());
    let owner = original.owner();
    assert!(owner.closed().unwrap().matches(owner));
    assert!(owner.users_closed().unwrap().matches(owner));
}

// Test diagnostics name each existing four-second request wait. Every value is
// an observation of the original caller/controller, never a readiness marker.
fn stage(name: &str, proxy: &Proxy, originals: Option<[&CarrierOrigin; 3]>) {
    eprintln!("SOCKS UDP / Mieru DNS phase: {name}");
    for caller in proxy.resolver.native_query_observations() {
        eprintln!(
            "  actual caller {:?}: io_pending={} joined={} original={}",
            caller.task.task_id(),
            caller.io_pending,
            caller.task.is_joined(),
            caller.original.is_some()
        );
    }
    if let Some(originals) = originals {
        for (label, original) in ["pool", "native UDP", "related TCP"]
            .into_iter()
            .zip(originals)
        {
            eprintln!(
                "  {label} {:?}: closed={} driver_joined={:?} owner_closed={} users_closed={}",
                original.instance(),
                original.closed().is_some(),
                original.driver().map(|driver| driver.is_joined()),
                original.owner().closed().is_some(),
                original.owner().users_closed().is_some()
            );
        }
    }
}
