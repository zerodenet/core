//! Actual default-dial DNS consumes the two original cached HTTP halves.
use super::*;
#[path = "dns/fixture.rs"]
mod fixture;
use fixture::*;
use zero_traits::IpAddress;

#[tokio::test]
async fn two_independent_h3_dns_halves_use_udp_and_return_query_before_cached_native() {
    timeout(Duration::from_secs(30), async {
        let directory = tempfile::tempdir().unwrap();
        let certificate = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let ca = directory.path().join("client.pem");
        std::fs::write(&ca, certificate.cert.pem()).unwrap();
        // Two real endpoints share only the independent peer's HTTP session
        // registry. Distinct endpoints forbid coalescing their actual halves.
        let registry = zero_transport::split_http::SplitHttpRegistry::new();
        let upload = peer::Peer::with_registry(&certificate, registry.clone()).await;
        let download = peer::Peer::with_registry(&certificate, registry).await;
        let mut dns = crate::dns_fixture::Dns::new().await;
        let client = start((3, upload.address(3)), (3, download.address(3)), &ca, dns.server()).await;
        let plan = client.proxy.tcp_runtime_services().components.clone().unwrap();
        let slot = plan.configured("node").unwrap();
        let declared = slot.recipe.spec().resources.iter().filter(|resource| matches!(resource,
            zero_platform_tokio::contract::component::ResourceDeclaration::ControlledCarrierPool {datagram:true,..})).count();
        assert_eq!(declared, 2, "both actual H3 half issuers exist before DNS selection");
        let mut first = None::<originals::Originals>;
        for (index, cancel) in [false, true, false].into_iter().enumerate() {
            let mut work = query(&client, &format!("{index}.two-half.actual.dns.test"));
            let request = tokio::select! {
                request = dns.request() => request,
                result = &mut work => panic!("actual mixed DNS failed before UDP delivery: {result:?}"),
            };
            assert!(!request.tcp, "a second actual H3 issuer cannot silently select TCP DNS framing");
            let actual = pending(&client).await;
            let child = actual.original.as_ref().unwrap();
            let donor = child.parent().expect("actual cached original retained by the DNS child");
            assert_eq!(donor.instance(), slot.instance_id());
            assert!(child.closed().is_none() && donor.closed().is_none());
            let current = originals::Originals::capture(&client, false).await;
            assert_eq!(current.actual_halves, 2);
            if let Some(first) = &first {
                assert!(first.same(&current) && current.same(first));
            }
            assert!(!work.is_finished());
            if cancel {
                work.abort(); assert!(work.await.unwrap_err().is_cancelled()); drop(request);
            } else {
                request.answer(false, false);
                assert_eq!(work.await.unwrap().unwrap(), vec![IpAddress::V4([127,0,0,1])]);
            }
            let joined = tokio::time::timeout_at(end(), actual.task.wait_registration_released()).await.unwrap();
            assert!(actual.task.matches_joined(&joined) && joined.registration_released());
            joined.result().unwrap(); returned(child);
            assert!(donor.closed().is_none(), "Query ACK is not either cached half's native close");
            assert!(client.proxy.protocols.runtime_host().original_budgets().carrier.snapshot().charged > 0);
            no_business(&client); dns.no_new_request();
            if first.is_none() { first = Some(current); }
        }
        let root = client.proxy.protocols.runtime_host().original_budgets().carrier.clone();
        client.close().await;
        first.unwrap().closed().await;
        assert_eq!(root.snapshot().charged, 0);
        assert_eq!(root.buffer_budget().used_bytes(), 0);
        upload.close().await; download.close().await; dns.close().await;
    }).await.unwrap();
}

#[tokio::test]
async fn mixed_tcp_h3_doh_keeps_cached_halves_through_query_cancel_and_source_replacement() {
    timeout(Duration::from_secs(30), async {
        let directory = tempfile::tempdir().unwrap();
        let certificate = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let ca = directory.path().join("client.pem");
        std::fs::write(&ca, certificate.cert.pem()).unwrap();
        let peer = peer::Peer::new(&certificate).await;
        let mut tls = crate::dns_fixture::TlsDns::new(true).await;
        let client = start((2, peer.address(2)), (3, peer.address(3)), &ca, tls.server(true)).await;
        zero_dns::test_support::install_doh_ca(&client.proxy.resolver,"query",tls.certificate.clone()).unwrap();
        let mut first = query(&client,"first.mixed.actual.doh.test");
        let (_, first_answer) = tokio::select! {
            request=tokio::time::timeout_at(end(),tls.requests.recv())=>request.unwrap().unwrap(),
            result=&mut first=>panic!("actual mixed DoH failed before its first TLS request: {result:?}"),
        };
        let actual = pending(&client).await;
        let child = actual.original.as_ref().unwrap();
        let donor = child.parent().unwrap();
        let originals = originals::Originals::capture(&client,false).await;
        assert_eq!(originals.actual_halves, 2);
        let mut second = query(&client,"second.mixed.actual.doh.test");
        let (bytes, answer) = tokio::select! {
            request=tokio::time::timeout_at(end(),tls.requests.recv())=>request.unwrap().unwrap(),
            result=&mut second=>panic!("actual mixed DoH lost its shared pool: {result:?}"),
        };
        leases(&client,2).await;
        let mut target = (*client.proxy.engine().config()).clone();
        target.runtime.native_dns_max_h2_drivers = Some(1);
        assert!(client.handle().apply_runtime_config_and_wait(target.clone(),Duration::from_secs(5)).await.is_err(), "active requests cannot be displaced by a DNS limit transaction");
        assert!(!actual.task.is_joined() && donor.closed().is_none());
        first.abort(); assert!(first.await.unwrap_err().is_cancelled()); drop(first_answer);
        leases(&client,1).await;
        answer.send(zero_dns::udp::build_dns_response(&bytes,&[IpAddress::V4([127,0,0,1])])).unwrap();
        assert_eq!(second.await.unwrap().unwrap(),vec![IpAddress::V4([127,0,0,1])]);
        leases(&client,0).await;
        assert!(!actual.task.is_joined() && child.closed().is_none());
        assert_eq!(tls.connections.load(std::sync::atomic::Ordering::SeqCst),1);
        let same = originals::Originals::capture(&client,false).await;
        assert!(originals.same(&same) && same.same(&originals));
        // Only the DNS Source budget changes. The configured XHTTP owner and
        // both cached physical halves remain the same actual registrations.
        client.handle().apply_runtime_config_and_wait(target,Duration::from_secs(5)).await.unwrap();
        let joined = actual.task.try_joined().expect("retired original DoH driver joined");
        assert!(actual.task.matches_joined(&joined) && joined.registration_released());
        joined.result().unwrap(); returned(child);
        assert!(donor.closed().is_none());
        zero_dns::test_support::install_doh_ca(&client.proxy.resolver,"query",tls.certificate.clone()).unwrap();
        let mut fresh = query(&client,"fresh-source.mixed.actual.doh.test");
        let (bytes, answer) = tokio::select! {
            request=tokio::time::timeout_at(end(),tls.requests.recv())=>request.unwrap().unwrap(),
            result=&mut fresh=>panic!("mixed cached carrier failed after actual old DNS retirement: {result:?}"),
        };
        let next = pending(&client).await;
        assert!(!next.task.same_registration(&actual.task));
        let next_child = next.original.as_ref().unwrap();
        assert!(!next_child.same_original(child));
        assert!(next_child.parent().unwrap().same_original(donor));
        let current = originals::Originals::capture(&client,false).await;
        assert!(originals.same(&current) && current.same(&originals));
        assert_eq!(tls.connections.load(std::sync::atomic::Ordering::SeqCst),2);
        answer.send(zero_dns::udp::build_dns_response(&bytes,&[IpAddress::V4([127,0,0,1])])).unwrap();
        assert_eq!(fresh.await.unwrap().unwrap(),vec![IpAddress::V4([127,0,0,1])]);
        leases(&client,0).await; no_business(&client);
        let root = client.proxy.protocols.runtime_host().original_budgets().carrier.clone();
        client.close().await;
        let joined = next.task.try_joined().unwrap();
        assert!(next.task.matches_joined(&joined) && joined.registration_released());
        joined.result().unwrap(); returned(next_child);
        originals.closed().await; same.closed().await; current.closed().await;
        assert_eq!(root.snapshot().charged,0);
        assert_eq!(root.buffer_budget().used_bytes(),0);
        peer.close().await; tls.close().await;
    }).await.unwrap();
}
