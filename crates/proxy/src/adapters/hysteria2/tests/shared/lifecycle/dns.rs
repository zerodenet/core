use super::*;
use zero_core::{
    component::AssociationId,
    request::{DatagramTargets, Destination},
};
use zero_platform_tokio::{
    contract::{datagram::DatagramRequest, dns::DnsService},
    controlled_carrier::CarrierOriginOwner,
};

#[tokio::test]
async fn hysteria2_actual_node_dns_replacement_waits_for_historical_view_and_original_native_receipt(
) {
    use std::{
        future::{poll_fn, Future},
        task::Poll,
    };
    tokio::time::timeout(Duration::from_secs(20),async {
        let server=Server::new().await;
        let mut previous=crate::dns_fixture::Dns::new().await;
        let mut next_dns=crate::dns_fixture::Dns::new().await;
        let mut config=zero_config::RuntimeConfig::parse(&serde_json::json!({
            "outbounds":[{"tag":"hy","protocol":{"type":"hysteria2","server":"dns-generation.test","server_name":"localhost","port":server.port,"password":"p","insecure":true}}],
            "route":{"final":{"type":"route","outbound":"hy"}}
        }).to_string()).unwrap();
        // This test advances only DnsSystem's generation inside the existing
        // host. Both real endpoints must already belong to that host's cold
        // declaration; a newly introduced endpoint needs a host transaction.
        let mut declared=previous.config();
        declared.servers.insert("next".into(),next_dns.server());
        config.runtime.dns=Some(declared);
        let proxy=Proxy::new(config).unwrap();start(&proxy).await;
        let mut first=Box::pin(connect(&proxy,"hy"));
        let request=tokio::select! {request=previous.request()=>request,_=&mut first=>panic!("Node DNS was bypassed")};
        request.answer(false,false);
        let held=first.await;
        let services=proxy.tcp_runtime_services().execution();let slot=services.components.as_ref().unwrap().configured("hy").unwrap();
        let role=slot.datagram(end()).await.unwrap();
        let loan=proxy.protocols.drive_component_supervision(role.open_carrier(DatagramRequest {
            association:AssociationId(810),targets:DatagramTargets::Fixed(Destination::new(Address::Ipv4([203,0,113,2]),53)),
        },OpenContext {
            identity:RequestIdentity::Service(Arc::new(zero_core::request::ServiceRequestFacts {origin:zero_core::request::ServiceOrigin::Runtime,purpose:zero_core::request::ServicePurpose::BackgroundControl})),
            deadline:Some(end()),cancel:CancellationToken::new(),observer:None,
            upstream:UpstreamTransport::new(UpstreamBinding {prefix:vec![],dial:slot.dial.clone(),network_generation:zero_core::component::NetworkGeneration(proxy.egress_interface.generation())},Arc::new(Denied)),
        })).await.unwrap();
        let root=loan.owner.original().clone();
        let original=match root.owner() {CarrierOriginOwner::Metadata(owner)=>owner.current_original().unwrap(),_=>panic!("actual metadata provider")};
        let zero_platform_tokio::controlled_carrier::loan::DatagramLoan {io,mut owner}=loan;
        let network=proxy.egress_interface.generation();let generation=proxy.resolver.generation();
        proxy.resolver.reload(Some(&next_dns.config())).unwrap();
        assert_eq!(proxy.egress_interface.generation(),network,"this is a real DNS replacement, without an egress invalidation");
        assert_ne!(proxy.resolver.generation(),generation);
        let stale=zero_engine::EngineError::Io(io.send(b"historical DNS view").await.unwrap_err());
        assert_eq!(crate::transport::channel_failure(&stale).unwrap().reason,zero_platform_tokio::contract::Reason::StalePlan);
        assert_eq!(server.authenticated.load(Ordering::SeqCst),1,"the historical association cannot acquire a new DNS authentication");
        let mut reopening=Box::pin(connect(&proxy,"hy"));
        tokio::select! {biased;_=server.wait_closed(1)=>{},request=next_dns.request()=>{drop(request);panic!("replacement DNS was acquired before the old transferred view retired")},_=&mut reopening=>panic!("replacement bypassed the old view")}
        poll_fn(|cx|{assert!(reopening.as_mut().poll(cx).is_pending());Poll::Ready(())}).await;
        next_dns.no_new_request();assert!(original.closed().is_none(),"held original endpoint user still blocks original native closure");
        drop(held);
        poll_fn(|cx|{assert!(reopening.as_mut().poll(cx).is_pending(),"the retained UDP view still holds original endpoint custody");Poll::Ready(())}).await;
        assert!(original.closed().is_none());next_dns.no_new_request();
        let unfinished=owner.close(tokio::time::Instant::now()).await.unwrap();assert!(!unfinished.is_complete());assert!(owner.closed().is_none());
        drop(io);
        poll_fn(|cx|{assert!(reopening.as_mut().poll(cx).is_pending(),"returned views cannot replace the original private control report");Poll::Ready(())}).await;
        assert!(original.closed().is_none());next_dns.no_new_request();
        let report=owner.close(end()).await.unwrap();assert!(report.is_complete());
        assert!(report.closed_io.is_empty() && report.exited_tasks.is_empty(),"logical control does not claim native or task closure");
        assert!(owner.closed().is_some());drop(owner);
        let request=tokio::select! {request=next_dns.request()=>request,_=&mut reopening=>panic!("fresh DNS provider was bypassed")};
        assert!(original.closed().unwrap().had_native_acquisition(),"the actual old raw controller closed before the new Node query");
        request.answer(false,false);
        let fresh=reopening.await;assert_eq!(server.authenticated.load(Ordering::SeqCst),2);
        assert!(root.is_retired(),"the new DNS original cannot impersonate a historical loan root with equal address/network facts");
        drop(fresh);proxy.protocols.shutdown_components(end()).await.unwrap();
        proxy.resolver.shutdown_queries(end()).await.unwrap();previous.close().await;next_dns.close().await;server.close().await;
    }).await.unwrap();
}
