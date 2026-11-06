//! One actual Node lookup spans failed originals in the same DNS Service call.
use super::*;
#[tokio::test]
async fn ordinary_vless_h2_and_upgrade_domain_dns_consumes_each_failed_original_before_the_next_candidate()
 {
    tokio::time::timeout(Duration::from_secs(20),async {
        for kind in ["h2","http_upgrade"] {
            let remote=Remote::new(kind).await;
            let refused=tokio::net::TcpSocket::new_v4().unwrap();
            refused.bind(std::net::SocketAddr::from(([127,0,0,2],remote.address.port()))).unwrap();
            let mut dns=crate::dns_fixture::Dns::new().await;
            let mut node=crate::dns_fixture::Dns::new().await;
            let (proxy,running)=start_with_node(&remote,kind,dns.server(),&node).await;
            let mut query=query(&proxy,&format!("domain.{kind}.native.dns.test"));
            let bootstrap=tokio::select! {
                request=node.request()=>request,
                result=&mut query=>panic!("ordinary {kind} failed before actual Node bootstrap: {result:?}"),
            };
            let question=zero_dns::udp::parse_dns_question(&bootstrap.query).unwrap();
            assert_eq!(question.domain,"transport.actual.node.test");assert_eq!(question.query_type,1);assert!(!bootstrap.tcp);
            let bootstrap_call=pending(&proxy).await;
            let bootstrap_native=bootstrap_call.original.as_ref().unwrap();
            assert_eq!(bootstrap_native.endpoints().unwrap().peer(),node.address);
            bootstrap.answer_ips(false,false,&[IpAddress::V4([127,0,0,2]),IpAddress::V4([127,0,0,1])]);
            tokio::time::timeout_at(deadline(),bootstrap_call.task.wait()).await.unwrap().result().unwrap();
            native_closed(bootstrap_native);
            let request=tokio::select! {
                request=dns.request()=>request,
                again=node.request()=>panic!("one original caller performed another Node lookup instead of advancing its actual candidates: {:?}",zero_dns::udp::parse_dns_question(&again.query).unwrap()),
                result=&mut query=>panic!("ordinary {kind} did not retire failed candidate and acquire the next original: {result:?}"),
            };
            assert!(request.tcp);
            let actual=pending(&proxy).await;let child=actual.original.as_ref().unwrap();let original=donor(&proxy,child);
            assert_eq!(original.selected_binding().unwrap().0.address,zero_core::Address::Domain("transport.actual.node.test".into()));
            assert_eq!(original.endpoints().unwrap().peer(),remote.address);
            let plan=proxy.tcp_runtime_services().components.clone().unwrap();let slot=plan.configured("node").unwrap();
            let owner=(0..slot.recipe.spec().resources.len()).filter_map(|ordinal|slot.original_carrier_owner(ordinal))
                .find(|owner|owner.observe().is_some_and(|held|held.retained()==1)).unwrap();
            let held=owner.observe().unwrap();assert_eq!(held.retained(),1);
            assert_eq!(held.slots()[0].ordinal(),2,"same actual issuer fully returned its first failed original before this N=1 successor");
            assert!(held.slots()[0].driver().is_none());
            let auxiliary=held.slots()[0].auxiliary().unwrap();
            assert!(auxiliary.scope_identity().same_scope(&owner.bound().unwrap().scope_identity()));
            let joined=auxiliary.try_joined().unwrap();
            assert!(auxiliary.matches_joined(&joined)&&joined.registration_released());joined.result().unwrap();
            request.answer(false,false);assert_eq!(query.await.unwrap().unwrap(),vec![IpAddress::V4([127,0,0,1])]);
            tokio::time::timeout_at(deadline(),actual.task.wait()).await.unwrap().result().unwrap();
            returned(child);native_closed(&original);node.no_new_request();
            running.shutdown().await.unwrap();
            assert!(owner.closed().unwrap().matches(&owner)&&owner.users_closed().unwrap().matches(&owner));
            assert!(proxy.engine().active_sessions().is_empty()&&proxy.engine().completed_sessions().is_empty());
            remote.close().await;dns.close().await;node.close().await;drop(refused);
        }
    }).await.unwrap();
}
