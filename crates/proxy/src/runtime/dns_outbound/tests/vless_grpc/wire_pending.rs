//! Native pending preparation keeps its actual same-pool original before a loan.
use super::*;
#[tokio::test]
async fn ordinary_vless_grpc_dns_cancel_during_real_wire_preparation_closes_the_undelivered_original(
) {
    tokio::time::timeout(Duration::from_secs(15), async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut dns = crate::dns_fixture::Dns::new().await;
        let (proxy, running) = start_with_tls(listener.local_addr().unwrap(), &dns).await;
        let mut query = query(&proxy, "preloan.pending.vless.grpc.test");
        let (mut wire, _) = tokio::select! {
            accepted = listener.accept() => accepted.unwrap(),
            result = &mut query => panic!("ordinary VLESS gRPC returned before native preparation: {result:?}"),
        };
        let mut header = [0; 5];
        wire.read_exact(&mut header).await.unwrap();
        assert_eq!(&header[..2], &[22, 3], "actual TLS handshake record");
        let mut handshake = vec![0; u16::from_be_bytes([header[3], header[4]]) as usize];
        wire.read_exact(&mut handshake).await.unwrap();
        assert_eq!(handshake[0], 1, "actual ClientHello precedes H2 driver installation");
        assert!(!query.is_finished(), "peer withholds the actual TLS ServerHello");
        let caller = proxy.resolver.native_query_observations().into_iter().find(|record| !record.task.is_joined()).expect("actual original DNS query waiting for its physical pool");
        let plan = proxy.tcp_runtime_services().components.clone().unwrap();
        let mut pending = vec![plan.configured("grpc").unwrap()];
        let mut seen = std::collections::BTreeSet::new();
        let mut actual = None;
        while let Some(slot) = pending.pop() {
            if !seen.insert(slot.instance_id()) { continue; }
            for ordinal in 0..slot.recipe.spec().resources.len() {
                if let Some(owner) = slot.original_carrier_owner(ordinal) {
                    if owner.observe().is_some_and(|state| state.slots().iter().any(|original| original.native().is_some_and(|native| !native.is_closed()))) { actual = Some(owner); break; }
                }
            }
            if actual.is_some() { break; }
            pending.extend(slot.dependency_instances().iter().filter_map(|instance| plan.component(instance.component).filter(|slot| slot.instance_id() == *instance)));
        }
        let owner = actual.expect("actual canonical gRPC pool's original provider, not a fabricated DNS socket");
        let held = owner.observe().unwrap();
        assert_eq!(held.retained(), 1);
        let original = held.slots()[0].original().clone();
        let auxiliary = held.slots()[0].auxiliary().cloned();
        assert!(held.slots()[0].driver().is_none(), "TLS preparation cannot already be a cached H2 driver");
        let auxiliary = auxiliary.expect("same actual physical setup witness installed before ClientHello");
        assert!(caller.original.is_none(), "no DNS child has yet received a logical stream loan");
        assert!(original.closed().is_none());
        query.abort(); assert!(query.await.unwrap_err().is_cancelled());
        tokio::time::timeout_at(deadline(), caller.task.wait()).await.unwrap().result().unwrap();
        assert!(auxiliary.is_joined());
        auxiliary.try_joined().unwrap().result().unwrap();
        let closed = original.closed().expect("same actual pre-delivery native original closed without another dial");
        assert!(original.matches_closed(&closed) && closed.had_native_acquisition());
        assert_eq!(owner.observe().unwrap().retained(), 0);
        let mut remaining = Vec::new(); wire.read_to_end(&mut remaining).await.unwrap();
        dns.no_new_request();
        assert!(proxy.engine().active_sessions().is_empty() && proxy.engine().completed_sessions().is_empty());
        running.shutdown().await.unwrap();
        assert!(owner.closed().is_some_and(|closed| closed.matches(&owner)));
        assert!(owner.users_closed().is_some_and(|closed| closed.matches(&owner)));
        dns.close().await;
    }).await.unwrap();
}
