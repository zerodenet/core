//! Cancelling a real UDP receive returns the same Direct original, then N1 reopens.
use super::*;
use zero_platform_tokio::contract::{
    dns::{DnsQueryContext, DnsQueryRole, DnsRequest, DnsService},
    CancellationToken, Reason,
};
#[tokio::test]
async fn actual_direct_datagram_cancel_keeps_native_and_query_ack_separate_then_reuses_n1() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let listener=tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint=listener.local_addr().unwrap();
        let socket=tokio::net::UdpSocket::bind(endpoint).await.unwrap();
        let config=zero_config::RuntimeConfig::parse(&serde_json::json!({
            "outbounds":[{"tag":"dns-direct","protocol":{"type":"direct"}}],
            "runtime":{"direct_max_originals":1,"direct_max_tasks":1,"native_dns_max_originals":1,
                "dns":{"servers":{"node":{"type":"system"},"query":{"type":"udp","host":"127.0.0.1","port":endpoint.port(),"detour":"dns-direct"}},
                "default_server":"query","policy":{"node_server":"node","timeout_ms":3000}}},
            "route":{"final":{"type":"direct"}}
        }).to_string()).unwrap();
        let proxy=Proxy::new(config.clone()).unwrap();
        proxy.resolver.set_outbound_connector(Arc::new(NoLegacy));
        let running=proxy.spawn();
        let handle=crate::ProxyHandle::new(zero_engine::EngineHandle::new(proxy.engine().clone()),proxy.clone());
        handle.apply_runtime_config_and_wait(config,Duration::from_secs(3)).await.unwrap();
        for attempt in 0..2 {
            let cancel=CancellationToken::new();
            let resolver=proxy.resolver.clone(); let token=cancel.clone();
            let mut query=tokio::spawn(async move { DnsService::resolve(resolver.as_ref(),DnsRequest {
                name:format!("cancel-direct-{attempt}.test"),role:DnsQueryRole::Default,family:zero_traits::AddressFamily::OnlyIpv4,
            },DnsQueryContext {deadline:Some(tokio::time::Instant::now()+Duration::from_secs(3)),cancel:token,expected_generation:None}).await });
            let mut bytes=vec![0;65536];
            let (size,peer)=tokio::select! {
                tcp=listener.accept()=>panic!("native UDP must not use the TCP peer: {tcp:?}"),
                received=socket.recv_from(&mut bytes)=>received.unwrap(),
                result=&mut query=>panic!("actual UDP receive must be pending first: {result:?}"),
            };
            bytes.truncate(size);
            let observed=proxy.resolver.native_query_observations().into_iter().find(|entry|entry.io_pending&&entry.original.is_some()).expect("same UDP receive-pending actual Query");
            let original=observed.original.as_ref().unwrap();
            let donor=original.parent().expect("selected Direct native parent").clone();
            let plan=proxy.tcp_runtime_services().components.clone().unwrap();
            let slot=plan.component(donor.instance().component).unwrap();
            let owner=slot.original_carrier_owner(0).unwrap();
            let actual=owner.observe().unwrap(); assert_eq!(actual.retained(),1);
            assert_eq!(actual.slots()[0].ordinal(),attempt+1);
            assert_eq!(actual.slots()[0].native().unwrap().kind(),zero_platform_tokio::controlled_carrier::CarrierNativeKind::Datagram);
            let setup=actual.slots()[0].auxiliary().unwrap().clone();
            let setup_joined=setup.try_joined().unwrap();
            assert!(setup.matches_joined(&setup_joined)&&setup_joined.registration_released());
            setup_joined.result().unwrap();
            assert!(!query.is_finished() && donor.closed().is_none());
            if attempt==0 {
                cancel.cancel();
                let error=proxy.protocols.drive_component_supervision(Box::pin(query)).await.unwrap().unwrap_err();
                assert_eq!(error.reason,Reason::Cancelled);
            } else {
                let answer=zero_dns::udp::build_dns_response(&bytes,&[IpAddress::V4([198,51,100,27])]);
                assert_eq!(socket.send_to(&answer,peer).await.unwrap(),answer.len());
                let answer=proxy.protocols.drive_component_supervision(Box::pin(query)).await.unwrap().unwrap();
                assert_eq!(answer.addresses,vec![IpAddress::V4([198,51,100,27])]);
            }
            let joined=proxy.protocols.drive_component_supervision(Box::pin(observed.task.wait_registration_released())).await;
            assert!(observed.task.matches_joined(&joined)&&joined.registration_released());
            joined.result().unwrap();
            let closed=donor.closed().expect("same actual UDP native original positively closed");
            assert!(closed.matches(&donor)&&closed.had_native_acquisition());
            let logical=original.closed().expect("same DNS logical original returned");
            assert!(logical.matches(original)&&!logical.had_native_acquisition());
            assert_eq!(owner.observe().unwrap().retained(),0);
        }
        std::future::poll_fn(|cx|{assert!(listener.poll_accept(cx).is_pending());std::task::Poll::Ready(())}).await;
        assert!(proxy.engine().active_sessions().is_empty());
        assert!(proxy.engine().completed_sessions().is_empty());
        running.shutdown().await.unwrap();
        assert_eq!(proxy.protocols.runtime_host().original_budgets().carrier.snapshot().charged,0);
    }).await.unwrap();
}
