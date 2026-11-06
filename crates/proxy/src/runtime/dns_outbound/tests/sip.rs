//! Ordinary DNS consumes the actual SIP Process dependency and derived original.
use super::*;
use std::{sync::Arc, time::Duration};
use zero_platform_tokio::controlled_carrier::CarrierOrigin;
#[derive(Debug)]
struct NoLegacy;
impl zero_dns::DnsOutboundConnector for NoLegacy {
    fn connect(&self, _: String, _: std::net::SocketAddr) -> zero_dns::DnsOutboundConnectFuture {
        Box::pin(async {
            panic!("SIP DNS must use its actual Process dependency, not a dummy Session")
        })
    }
}
fn deadline() -> tokio::time::Instant {
    tokio::time::Instant::now() + Duration::from_secs(3)
}
async fn start(config: zero_config::RuntimeConfig) -> (Proxy, crate::RunningProxy) {
    let proxy = Proxy::new(config.clone()).unwrap();
    proxy.resolver.set_outbound_connector(Arc::new(NoLegacy));
    let running = proxy.spawn();
    crate::ProxyHandle::new(
        zero_engine::EngineHandle::new(proxy.engine().clone()),
        proxy.clone(),
    )
    .apply_runtime_config_and_wait(config, Duration::from_secs(5))
    .await
    .unwrap();
    (proxy, running)
}
async fn pending(proxy: &Proxy) -> zero_dns::OwnedDnsQueryObservation {
    tokio::time::timeout_at(deadline(), async {
        loop {
            if let Some(actual) = proxy
                .resolver
                .native_query_observations()
                .into_iter()
                .find(|actual| actual.io_pending && actual.original.is_some())
            {
                break actual;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap()
}
fn query(proxy: &Proxy, name: &str) -> tokio::task::JoinHandle<std::io::Result<Vec<IpAddress>>> {
    let resolver = proxy.resolver.clone();
    let name = name.to_owned();
    tokio::spawn(async move { resolver.resolve_real_type(&name, 1).await })
}
fn original(proxy: &Proxy, child: &CarrierOrigin) -> CarrierOrigin {
    let original = child.parent().unwrap().clone();
    assert!(
        original.parent().is_none() && original.driver().is_none() && original.closed().is_none()
    );
    let peer = original.endpoints().unwrap().peer();
    assert!(peer.ip().is_loopback());
    let plan = proxy.tcp_runtime_services().components.clone().unwrap();
    let slot = plan.configured("sip").unwrap();
    assert_eq!(slot.instance_id(), original.instance());
    let owner = (0..slot.recipe.spec().resources.len())
        .filter_map(|ordinal| slot.original_carrier_owner(ordinal))
        .find(|owner| owner.instance() == original.instance())
        .unwrap();
    let held = owner.observe().unwrap();
    assert_eq!(held.retained(), 1);
    let auxiliary = held.slots()[0].auxiliary().unwrap();
    assert!(auxiliary
        .scope_identity()
        .same_scope(&owner.bound().unwrap().scope_identity()));
    let joined = auxiliary.try_joined().unwrap();
    assert!(auxiliary.matches_joined(&joined) && joined.registration_released());
    joined.result().unwrap();
    original
}
fn closed(child: &CarrierOrigin, original: &CarrierOrigin) {
    let logical = child.closed().unwrap();
    assert!(logical.matches(child) && !logical.had_native_acquisition());
    let native = original.closed().unwrap();
    assert!(native.matches(original) && native.had_native_acquisition());
}
#[tokio::test]
async fn ordinary_sip_dns_pending_cancel_and_warm_doh_retire_the_actual_process_before_limit_replacement(
) {
    tokio::time::timeout(Duration::from_secs(25), async {
        let remote_config = zero_config::RuntimeConfig::parse(&serde_json::json!({
            "inbounds":[{"tag":"ss","listen":{"address":"127.0.0.1","port":0},"protocol":{"type":"shadowsocks","cipher":"aes-128-gcm","password":"actual-sip-dns"}}],
            "route":{"final":{"type":"direct"}}
        }).to_string()).unwrap();
        let remote = Proxy::new(remote_config.clone()).unwrap(); let registry = remote.system_tun_source_registry();
        let serving = remote.spawn();
        let source = tokio::time::timeout_at(deadline(), async {
            loop { if let Some(source) = registry.owner("ss") { break source; } tokio::task::yield_now().await; }
        }).await.unwrap();
        source.wait_ready().await.unwrap();
        crate::ProxyHandle::new(zero_engine::EngineHandle::new(remote.engine().clone()), remote.clone())
            .apply_runtime_config_and_wait(remote_config, Duration::from_secs(5)).await.unwrap();
        let endpoint = *source.original_listener_address.get().unwrap();
        let directory = tempfile::tempdir().unwrap();
        let helper = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../protocols/shadowsocks/tests/support/sip003.py");
        let mut dns = crate::dns_fixture::Dns::new().await;
        let mut tls = crate::dns_fixture::TlsDns::new(true).await;
        let config = |backend: zero_config::DnsServerConfig| {
            let mut backend = serde_json::to_value(backend).unwrap(); backend["detour"] = "sip".into();
            zero_config::RuntimeConfig::parse(&serde_json::json!({
                "runtime":{"native_stream_max_originals":1,"dns":{"servers":{"query":backend,"node":{"type":"system"}},"default_server":"query","policy":{"node_server":"node"}}},
                "outbounds":[{"tag":"sip","protocol":{"type":"shadowsocks","server":"127.0.0.1","port":endpoint.port(),"cipher":"aes-128-gcm","password":"actual-sip-dns","plugin":{"command":"python3","args":[helper,"client","tcp_only",directory.path().join("actual-child.json")],"options":"test=options;escaped=value","mode":"tcp_only"}}}],
                "route":{"final":{"type":"direct"}}
            }).to_string()).unwrap()
        };
        let (proxy, running) = start(config(dns.server())).await;
        let process = proxy.protocols.runtime_host().original_budgets().process.clone();
        assert_eq!(process.snapshot().charged_jobs, 0, "cold DNS does not eagerly spawn a plugin");
        let mut previous = Vec::new();
        for (index, cancel) in [false, true, false].into_iter().enumerate() {
            let mut work = query(&proxy, &format!("{index}.actual.sip.dns.test"));
            let request = tokio::select! {
                request = dns.request() => request,
                result = &mut work => panic!("actual SIP DNS failed before real framed query: {result:?}"),
            };
            assert!(request.tcp); let caller = pending(&proxy).await; let child = caller.original.as_ref().unwrap();
            let native = original(&proxy, child);
            assert_ne!(native.endpoints().unwrap().peer(), endpoint, "actual derived loopback is not the configured remote SS endpoint");
            assert_eq!(process.snapshot().charged_jobs, 1);
            if cancel { work.abort(); assert!(work.await.unwrap_err().is_cancelled()); drop(request); }
            else { request.answer(false, false); assert_eq!(work.await.unwrap().unwrap(), vec![IpAddress::V4([127,0,0,1])]); }
            tokio::time::timeout_at(deadline(), caller.task.wait()).await.unwrap().result().unwrap(); closed(child, &native);
            for old in &previous { assert!(!native.same_original(old)); } previous.push(native);
            assert_eq!(process.snapshot().charged_jobs, 1, "query return does not claim cached process/actor/native retirement");
        }
        running.shutdown().await.unwrap(); assert_eq!(process.snapshot().charged_jobs, 0);
        for native in previous {
            assert!(native.owner().closed().unwrap().matches(native.owner()));
            assert!(native.owner().users_closed().unwrap().matches(native.owner()));
        }
        assert!(proxy.engine().active_sessions().is_empty() && proxy.engine().completed_sessions().is_empty());

        let (proxy, running) = start(config(tls.server(true))).await;
        zero_dns::test_support::install_doh_ca(&proxy.resolver, "query", tls.certificate.clone()).unwrap();
        let process = proxy.protocols.runtime_host().original_budgets().process.clone();
        let mut work = query(&proxy, "warm.actual.sip.doh.test");
        let (bytes, answer) = tokio::select! {
            request = tokio::time::timeout_at(deadline(), tls.requests.recv()) => request.unwrap().unwrap(),
            result = &mut work => panic!("actual SIP DoH failed before HTTP wire: {result:?}"),
        };
        let caller = pending(&proxy).await; let child = caller.original.as_ref().unwrap(); let old = original(&proxy, child);
        assert_ne!(old.endpoints().unwrap().peer(), endpoint);
        assert_eq!(process.snapshot().charged_jobs, 1);
        answer.send(zero_dns::udp::build_dns_response(&bytes, &[IpAddress::V4([127,0,0,1])])).unwrap(); work.await.unwrap().unwrap();
        tokio::time::timeout_at(deadline(), async {
            loop { if zero_dns::test_support::doh_pool_witness(&proxy.resolver, "query").unwrap().active_leases == 0 { break; } tokio::task::yield_now().await; }
        }).await.unwrap();
        assert!(!caller.task.is_joined() && old.closed().is_none());
        let mut candidate = (*proxy.engine().config()).clone(); candidate.runtime.process_max_jobs = Some(1);
        crate::ProxyHandle::new(zero_engine::EngineHandle::new(proxy.engine().clone()), proxy.clone())
            .apply_runtime_config_and_wait(candidate, Duration::from_secs(5)).await.unwrap();
        closed(child, &old); let joined = caller.task.try_joined().unwrap();
        assert!(caller.task.matches_joined(&joined) && joined.registration_released()); joined.result().unwrap();
        assert!(old.owner().closed().unwrap().matches(old.owner()));
        assert!(old.owner().users_closed().unwrap().matches(old.owner()));
        assert_eq!(process.snapshot().charged_jobs, 0, "same original process tree, derived views, actor join and ACK precede installing the new job limit");
        zero_dns::test_support::install_doh_ca(&proxy.resolver, "query", tls.certificate.clone()).unwrap();
        let mut work = query(&proxy, "new.actual.sip.doh.test");
        let (bytes, answer) = tokio::select! {
            request = tokio::time::timeout_at(deadline(), tls.requests.recv()) => request.unwrap().unwrap(),
            result = &mut work => panic!("SIP successor failed after actual old retirement: {result:?}"),
        };
        let fresh = pending(&proxy).await; let successor = fresh.original.as_ref().unwrap(); let native = original(&proxy, successor);
        assert!(!old.same_original(&native) && !caller.task.scope_identity().same_scope(&fresh.task.scope_identity()));
        assert_eq!(process.snapshot().charged_jobs, 1);
        answer.send(zero_dns::udp::build_dns_response(&bytes, &[IpAddress::V4([127,0,0,1])])).unwrap(); work.await.unwrap().unwrap();
        running.shutdown().await.unwrap(); closed(successor, &native);
        assert!(fresh.task.try_joined().unwrap().registration_released()); assert_eq!(process.snapshot().charged_jobs, 0);
        assert!(proxy.engine().active_sessions().is_empty() && proxy.engine().completed_sessions().is_empty());
        serving.shutdown().await.unwrap(); let terminal = source.terminal_for_test().unwrap();
        assert_eq!(terminal.identity(), source.identity()); assert!(terminal.failure().is_none() && terminal.task_failures().is_empty() && terminal.native_bound_failures().is_empty());
        assert!(terminal.joined_tasks() >= 2 && terminal.closed_io_receipts() > 0);
        assert!(remote.engine().active_sessions().is_empty()); dns.close().await; tls.close().await;
    }).await.unwrap();
}
