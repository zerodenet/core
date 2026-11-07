//! A not-yet-delivered logical Stream cannot retire a published packet pool.
use super::*;
mod pending_packet;
use pending_packet::PacketPeer;

#[tokio::test]
async fn cancelling_udp_underlay_connect_ack_waiter_preserves_the_actual_socks_pair_used_by_a_sibling(
) {
    tokio::time::timeout(Duration::from_secs(25),async {
        let prefix=Prefix::new().await;
        let mut server=PacketPeer::new().await;
        let mut dns=Dns::new().await;
        let mut settings=config([server.address;3]);
        settings.outbounds.truncate(1);
        let zero_config::OutboundProtocolConfig::Mieru { transport,.. }=&mut settings.outbounds[0].protocol else {unreachable!()};
        *transport=zero_config::MieruTransport::Udp;
        settings.outbounds.push(prefix.node.clone());
        settings.outbound_groups=zero_config::RuntimeConfig::parse(r#"{"outbounds":[{"tag":"socks","protocol":{"type":"direct"}},{"tag":"a","protocol":{"type":"direct"}}],"outbound_groups":[{"tag":"packet-chain","type":"relay","proxies":["socks","a"]}],"route":{"final":{"type":"direct"}}}"#).unwrap().outbound_groups;
        let mut policy=dns.config();let mut backend=dns.server();
        let zero_config::DnsServerConfig::Udp { detour,.. }=&mut backend else {unreachable!()};
        *detour=Some("packet-chain".into());policy.servers.insert("query".into(),backend);policy.default_server="query".into();policy.policy.node_server=Some("native".into());settings.runtime.dns=Some(policy);
        let (proxy,running,_)=start(settings).await;
        let resolver=proxy.resolver.clone();
        let mut first=tokio::spawn(async move {resolver.resolve_real_type("first.packet-ack.test",1).await});
        tokio::select! {waiting=&mut server.waiting=>waiting.unwrap(),result=&mut first=>panic!("first query must remain suspended at its actual CONNECT ACK: {result:?}")};
        let first_caller=proxy.resolver.native_query_observations().into_iter().find(|caller| caller.io_pending && !caller.task.is_joined()).expect("the original query is awaiting the withheld real ACK");
        assert!(!first.is_finished());
        let resolver=proxy.resolver.clone();
        let mut second=tokio::spawn(async move {resolver.resolve_real_type("second.packet-ack.test",1).await});
        let request=tokio::select! {request=dns.request()=>request,result=&mut second=>panic!("a concurrent query must borrow the same published packet pool: {result:?}")};
        assert!(request.tcp);
        let second_caller=tokio::time::timeout_at(deadline(),async {
            loop {
                if let Some(caller)=proxy.resolver.native_query_observations().into_iter().find(|caller| caller.io_pending && caller.original.is_some() && !caller.task.is_joined() && !caller.task.same_registration(&first_caller.task)) {break caller;}
                tokio::task::yield_now().await;
            }
        }).await.unwrap();
        let child=second_caller.original.as_ref().unwrap();let pool=child.parent().unwrap().clone();let udp=pool.native_origin().clone();let control=udp.related_control().unwrap();
        first.abort();assert!(first.await.unwrap_err().is_cancelled());
        tokio::time::timeout_at(deadline(),first_caller.task.wait()).await.unwrap().result().unwrap();
        assert!(!second.is_finished());
        assert!(!pool.driver().unwrap().is_joined());
        assert!(pool.closed().is_none() && udp.closed().is_none() && control.closed().is_none());
        request.answer(false,false);second.await.unwrap().unwrap();
        tokio::time::timeout_at(deadline(),second_caller.task.wait()).await.unwrap().result().unwrap();
        let resolver=proxy.resolver.clone();
        let mut third=tokio::spawn(async move {resolver.resolve_real_type("third.packet-ack.test",1).await});
        let request=tokio::select! {request=dns.request()=>request,result=&mut third=>panic!("cancelled opening must not destroy shared association reuse: {result:?}")};
        let caller=pending(&proxy).await;let actual=caller.original.as_ref().unwrap().parent().unwrap();
        assert!(actual.same_original(&pool));assert!(actual.native_origin().same_original(&udp));assert!(actual.native_origin().related_control().unwrap().same_original(&control));
        assert_eq!(server.accepts.load(Ordering::SeqCst),1);
        request.answer(false,false);third.await.unwrap().unwrap();
        assert!(proxy.engine().active_sessions().is_empty() && proxy.engine().completed_sessions().is_empty());
        running.shutdown().await.unwrap();assert!(pool.driver().unwrap().is_joined());native_closed(&udp);native_closed(&control);
        server.close().await;prefix.close().await;dns.close().await;
    }).await.unwrap();
}
