use super::*;
use zero_traits::DnsResolver;
fn end() -> tokio::time::Instant {
    tokio::time::Instant::now() + std::time::Duration::from_secs(3)
}
#[tokio::test]
async fn pure_local_resolution_preserves_both_families_without_inventing_a_query_original() {
    let dns = DnsSystem::build(None).unwrap();
    let call = dns.resolution_call();
    assert!(call.closed().is_none());
    let actual = call
        .scope(dns.resolve("app.localhost"))
        .await
        .unwrap()
        .unwrap();
    assert!(actual.contains(&zero_traits::IpAddress::V4([127, 0, 0, 1])));
    assert!(actual.contains(&zero_traits::IpAddress::V6(
        std::net::Ipv6Addr::LOCALHOST.octets()
    )));
    let closed = call.wait_closed(end()).await.unwrap();
    assert!(closed.matches(&call));
    for query in closed.queries() {
        assert!(query.query().is_none() && query.joined().is_none() && query.failure().is_none());
    }
    assert!(!closed.matches(&dns.resolution_call()));
    let polled = AtomicBool::new(false);
    assert_eq!(
        call.scope(async { polled.store(true, Ordering::Release) })
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidInput
    );
    assert!(!polled.load(Ordering::Acquire));
}
#[cfg(feature = "udp")]
#[tokio::test]
async fn both_actual_unowned_udp_family_originals_remain_unknown_after_caller_cancellation() {
    tokio::time::timeout(std::time::Duration::from_secs(3),async{
 let peer=tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
 let config=zero_config::DnsConfig{servers:std::collections::BTreeMap::from([("wire".into(),zero_config::DnsServerConfig::Udp{host:"127.0.0.1".into(),port:peer.local_addr().unwrap().port(),bootstrap:vec![],detour:None})]),default_server:"wire".into(),dispatch:vec![],cache:None,reverse_mapping:None,answer:Default::default(),policy:Default::default()};
 let dns=DnsSystem::build(Some(&config)).unwrap();let call=dns.resolution_call();
 let work=call.scope(dns.resolve("actual.example"));tokio::pin!(work);
 let mut types=Vec::new();
 for _ in 0..2{
 let mut bytes=[0;512];
 let (size,_)=tokio::select!{result=peer.recv_from(&mut bytes)=>result.unwrap(),result=&mut work=>panic!("resolution returned before original family IO: {result:?}")};
 types.push(u16::from_be_bytes([bytes[size-4],bytes[size-3]]));
 }
 types.sort_unstable();assert_eq!(types,[crate::message::TYPE_A,crate::message::TYPE_AAAA]);
 call.cancel();assert_eq!(work.await.unwrap_err().kind(),io::ErrorKind::Interrupted);
 let error=call.wait_closed(end()).await.unwrap_err();
 let retained=error.get_ref().unwrap().downcast_ref::<DnsResolutionUnsettled>().unwrap();
 assert!(retained.call().same_call(&call));assert_eq!(retained.faults().count(),2);
 assert!(call.closed().is_none(),"unowned library tasks cannot supply installed Host/native receipts");
 dns.shutdown_queries(end()).await.unwrap();
 }).await.unwrap();
}
