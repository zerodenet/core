//! Native pending preparation keeps its actual same-pool original before a loan.
use super::*;
#[tokio::test]
async fn ordinary_vless_grpc_dns_cancel_during_real_wire_preparation_closes_the_undelivered_original(
) {
    tokio::time::timeout(Duration::from_secs(15), async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut dns = crate::dns_fixture::Dns::new().await;
        let (proxy, running) = start_at(listener.local_addr().unwrap(), false, &dns).await;
        let mut query = query(&proxy, "preloan.pending.vless.grpc.test");
        let (mut wire, _) = tokio::select! {
            accepted = listener.accept() => accepted.unwrap(),
            result = &mut query => panic!("ordinary VLESS gRPC returned before native preparation: {result:?}"),
        };
        let mut first = [0]; wire.read_exact(&mut first).await.unwrap();
        assert_eq!(first, [b'P'], "actual HTTP/2 client preface");
        assert!(!query.is_finished(), "peer withholds actual H2 SETTINGS");
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
        let driver = held.slots()[0].driver().map(|(_, witness)| witness.clone());
        assert!(auxiliary.is_some() || driver.is_some(), "actual setup/driver witness is installed before first wire I/O");
        assert!(original.closed().is_none());
        query.abort(); assert!(query.await.unwrap_err().is_cancelled());
        tokio::time::timeout_at(deadline(), caller.task.wait()).await.unwrap().result().unwrap();
        for task in auxiliary.into_iter().chain(driver) { assert!(task.is_joined()); task.try_joined().unwrap().result().unwrap(); }
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
