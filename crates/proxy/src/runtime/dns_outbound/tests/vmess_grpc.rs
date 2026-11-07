//! Ordinary single-leaf gRPC DNS uses the existing per-connection H2 owner.
use super::*;
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use zero_platform_tokio::contract::CancellationToken;
mod candidates;
mod fixture;
mod replacement;
use fixture::*;

#[tokio::test]
async fn ordinary_vmess_grpc_dns_returns_real_h2_custody_on_success_and_pending_cancel() {
    tokio::time::timeout(Duration::from_secs(25), async {
        for multi in [false, true] {
            let peer = Peer::new(multi).await;
            let mut dns = crate::dns_fixture::Dns::new().await;
            let config = config(peer.address, multi, &dns, None);
            let (proxy, running, _) = start(config).await;
            let mut previous = None;
            for (number, cancel) in [false, true].into_iter().enumerate() {
                let mut query = query(&proxy, &format!("{multi}-{number}.grpc.dns.test"));
                let request = tokio::select! {
                    request = dns.request() => request,
                    result = &mut query => panic!("gRPC DNS failed before actual DNS I/O: {result:?}"),
                };
                assert!(request.tcp);
                let caller = pending(&proxy).await;
                let (child, original) = originals(&caller);
                assert!(!caller.task.scope_identity().same_scope(&original.driver().unwrap().scope_identity()));
                assert!(original.closed().is_none());
                assert!(!original.driver().unwrap().is_joined());
                if cancel {
                    query.abort();
                    assert!(query.await.unwrap_err().is_cancelled());
                    drop(request);
                } else {
                    request.answer(false, false);
                    assert_eq!(query.await.unwrap().unwrap(), vec![IpAddress::V4([127, 0, 0, 1])]);
                }
                tokio::time::timeout_at(deadline(), caller.task.wait()).await.unwrap();
                closed(&child, &original);
                if let Some(prior) = &previous {
                    assert!(!original.same_original(prior), "per-connection H2 must not fabricate a shared cache");
                    assert!(!original.driver().unwrap().same_registration(prior.driver().unwrap()));
                }
                previous = Some(original);
            }
            assert_eq!(peer.accepted(), 2);
            assert!(proxy.engine().active_sessions().is_empty());
            assert!(proxy.engine().completed_sessions().is_empty());
            running.shutdown().await.unwrap();
            let original = previous.unwrap();
            assert!(original.owner().closed().unwrap().matches(original.owner()));
            assert!(original.owner().users_closed().unwrap().matches(original.owner()));
            peer.close().await;
            dns.close().await;
        }
    }).await.unwrap();
}

#[tokio::test]
async fn cancelled_vmess_grpc_dns_wire_prepare_closes_the_actual_undelivered_native_original() {
    tokio::time::timeout(Duration::from_secs(15), async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut dns = crate::dns_fixture::Dns::new().await;
        let config = config(listener.local_addr().unwrap(), false, &dns, None);
        let (proxy, running, _) = start(config).await;
        let mut query = query(&proxy, "wire.pending.grpc.dns.test");
        let (mut socket, _) = tokio::select! {
            socket = listener.accept() => socket.unwrap(),
            result = &mut query => panic!("gRPC DNS failed before actual native wire preparation: {result:?}"),
        };
        let mut first = [0u8; 1];
        socket.read_exact(&mut first).await.unwrap();
        // Withhold peer H2 SETTINGS after consuming an actual first wire byte.
        // This witnesses the original handshake suspension, before loan return.
        assert!(!query.is_finished());
        let caller = proxy.resolver.native_query_observations().into_iter().find(|query| !query.task.is_joined()).expect("actual DNS caller");
        let services = proxy.tcp_runtime_services();
        let plan = services.components.as_ref().unwrap();
        let role = plan.configured("grpc").unwrap();
        let owner = role.recipe.spec().resources.iter().enumerate().filter_map(|(ordinal, _)| {
            plan.original_carrier_owner(role.instance_id(), ordinal)
        }).find(|owner| owner.observe().is_some_and(|held| held.retained() == 1)).expect("same actual service controlled original");
        let held = owner.observe().unwrap();
        let original = held.slots()[0].original().clone();
        let driver = held.slots()[0].driver().map(|(_, witness)| witness.clone());
        let auxiliary = held.slots()[0].auxiliary().expect("actual same-original preparation").clone();
        assert!(auxiliary.scope_identity().same_scope(&owner.bound().unwrap().scope_identity()));
        assert!(original.closed().is_none());
        query.abort();
        assert!(query.await.unwrap_err().is_cancelled());
        tokio::time::timeout_at(deadline(), caller.task.wait()).await.unwrap();
        if let Some(driver) = driver {
            let joined = tokio::time::timeout_at(deadline(), driver.wait_registration_released()).await.unwrap();
            assert!(driver.matches_joined(&joined) && joined.registration_released());
        }
        let joined = auxiliary.try_joined().expect("same actual preparation finished after cancellation");
        assert!(auxiliary.matches_joined(&joined) && joined.registration_released());
        let closed = original.closed().expect("same pre-handoff original native close");
        assert!(original.matches_closed(&closed));
        assert!(closed.had_native_acquisition());
        assert_eq!(owner.observe().unwrap().retained(), 0);
        let mut rest = Vec::new();
        tokio::time::timeout_at(deadline(), socket.read_to_end(&mut rest)).await.unwrap().unwrap();
        dns.no_new_request();
        running.shutdown().await.unwrap();
        dns.close().await;
    }).await.unwrap();
}
