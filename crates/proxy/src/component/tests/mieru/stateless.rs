//! Real DNSRoot consumes four taskless prefixes and the same two-hop Mieru pool.
use super::*;
use crate::dns_fixture::{Dns, Request};
use zero_platform_tokio::controlled_carrier::CarrierOrigin;

type Query = tokio::task::JoinHandle<std::io::Result<Vec<zero_traits::IpAddress>>>;

async fn querying(
    proxy: &Proxy,
    dns: &mut Dns,
    name: String,
) -> (Query, Request, zero_dns::OwnedDnsQueryObservation) {
    let resolver = proxy.resolver.clone();
    let mut query = tokio::spawn(async move { resolver.resolve_real_type(&name, 1).await });
    let request = tokio::select! {
        request = dns.request() => request,
        result = &mut query => panic!("actual DNSRoot query failed before fixture I/O: {result:?}"),
    };
    assert!(
        request.tcp,
        "this prefix borrows a real Stream, not root UDP"
    );
    let observed = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if let Some(observed) =
                proxy
                    .resolver
                    .native_query_observations()
                    .into_iter()
                    .find(|query| {
                        query.io_pending && query.original.is_some() && !query.task.is_joined()
                    })
            {
                break observed;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the real query must retain its suspended original");
    (query, request, observed)
}

fn lineage(
    observed: &zero_dns::OwnedDnsQueryObservation,
) -> (CarrierOrigin, CarrierOrigin, CarrierOrigin, CarrierOrigin) {
    let child = observed.original.clone().unwrap();
    let last = child.parent().expect("actual final Mieru donor").clone();
    let middle = last.parent().expect("actual first Mieru donor").clone();
    let root = middle.parent().expect("actual taskless donor").clone();
    assert!(root.parent().is_none());
    assert_eq!(
        root.owner().limit().get(),
        1,
        "configured actual Stream original cap"
    );
    assert!(
        root.driver().is_none(),
        "a taskless donor cannot invent a driver"
    );
    assert!(
        child.driver().is_none(),
        "DNS logical custody is not the Mieru driver"
    );
    assert_eq!(
        middle.instance(),
        last.instance(),
        "both hops belong to the same actual Mieru pool"
    );
    assert!(middle
        .driver()
        .unwrap()
        .scope_identity()
        .same_scope(&last.driver().unwrap().scope_identity()));
    assert!(
        !middle.same_original(&last),
        "two carriers retain their distinct original slots"
    );
    assert_ne!(root.instance(), last.instance());
    (child, last, middle, root)
}

#[tokio::test]
async fn ordinary_taskless_protocol_prefixes_transfer_their_actual_original_to_the_shared_mieru_pool(
) {
    tokio::time::timeout(Duration::from_secs(45), async {
        for protocol in [
            "vless",
            "vmess",
            "trojan",
            #[cfg(feature = "shadowsocks")]
            "shadowsocks",
        ] {
            let mut prefix = if protocol == "shadowsocks" {
                crate::runtime::MuxPrefix::shadowsocks("aes-128-gcm", "a").await
            } else {
                crate::runtime::MuxPrefix::start(protocol, "a").await
            };
            match &mut prefix.node.protocol {
                zero_config::OutboundProtocolConfig::Vless {
                    mux_concurrency,
                    xudp_concurrency,
                    ..
                } => {
                    *mux_concurrency = None;
                    *xudp_concurrency = None;
                }
                zero_config::OutboundProtocolConfig::Vmess {
                    mux_concurrency, ..
                }
                | zero_config::OutboundProtocolConfig::Trojan {
                    mux_concurrency, ..
                } => *mux_concurrency = None,
                _ => {}
            }
            let mut dns = Dns::new().await;
            let b = Server::new(true).await;
            let c = Server::new(true).await;
            let mut config = config([b.address, b.address, c.address]);
            config.outbounds[0] = prefix.node.clone();
            config.runtime.native_stream_max_originals = Some(1);
            let mut settings = dns.config();
            let mut server = dns.server();
            let zero_config::DnsServerConfig::Udp { detour, .. } = &mut server else {
                unreachable!()
            };
            *detour = Some("chain".into());
            settings.servers.insert("query".into(), server);
            settings.default_server = "query".into();
            settings.policy.node_server = Some("native".into());
            config.runtime.dns = Some(settings);
            let proxy = Proxy::new(config.clone()).unwrap();
            let running = proxy.spawn();
            let handle = ProxyHandle::new(
                zero_engine::EngineHandle::new(proxy.engine().clone()),
                proxy.clone(),
            );
            handle
                .apply_runtime_config_and_wait(config.clone(), Duration::from_secs(5))
                .await
                .unwrap();
            let mut prior = None;
            for (number, cancel) in [true, false].into_iter().enumerate() {
                let (query, request, observed) =
                    querying(&proxy, &mut dns, format!("{protocol}-{number}.nested.test")).await;
                let (child, last, middle, root) = lineage(&observed);
                assert!(root.closed().is_none());
                assert!(!last.driver().unwrap().is_joined());
                assert!(!middle.driver().unwrap().is_joined());
                if let Some((old_last, old_middle, old_root)) = &prior {
                    assert!(last.same_original(old_last));
                    assert!(middle.same_original(old_middle));
                    assert!(root.same_original(old_root));
                }
                if cancel {
                    query.abort();
                    assert!(query.await.unwrap_err().is_cancelled());
                    drop(request);
                } else {
                    request.answer(false, false);
                    assert_eq!(
                        query.await.unwrap().unwrap(),
                        vec![zero_traits::IpAddress::V4([127, 0, 0, 1])]
                    );
                }
                tokio::time::timeout_at(deadline(), observed.task.wait())
                    .await
                    .unwrap();
                let returned = child.closed().expect("same actual query child returned");
                assert!(returned.matches(&child) && !returned.had_native_acquisition());
                for donor in [&last, &middle, &root] {
                    assert!(donor.closed().is_none());
                }
                assert!(!last.driver().unwrap().is_joined());
                assert!(!middle.driver().unwrap().is_joined());
                assert_eq!(b.accepts.load(Ordering::SeqCst), 1);
                assert_eq!(c.accepts.load(Ordering::SeqCst), 1);
                prior = Some((last, middle, root));
            }
            // A third real query retains a child while the original prefix's
            // credentials change. The host must settle that actual query,
            // both actual drivers and all original controls before ACK.
            let (query, request, observed) =
                querying(&proxy, &mut dns, format!("{protocol}.replacement.test")).await;
            let (child, last, middle, root) = lineage(&observed);
            let (old_last, old_middle, old_root) = prior.unwrap();
            assert!(
                last.same_original(&old_last)
                    && middle.same_original(&old_middle)
                    && root.same_original(&old_root)
            );
            assert!(child.closed().is_none());
            assert!(!observed.task.is_joined());
            match &mut config.outbounds[0].protocol {
                zero_config::OutboundProtocolConfig::Vless { id, .. }
                | zero_config::OutboundProtocolConfig::Vmess { id, .. } => {
                    *id = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee".into()
                }
                zero_config::OutboundProtocolConfig::Trojan { password, .. }
                | zero_config::OutboundProtocolConfig::Shadowsocks { password, .. } => {
                    *password = "replacement-password".into()
                }
                _ => unreachable!(),
            }
            handle
                .apply_runtime_config_and_wait(config, Duration::from_secs(10))
                .await
                .unwrap();
            assert!(
                observed.task.is_joined(),
                "replacement ACK follows actual waiting query cleanup"
            );
            assert!(query.await.unwrap().is_err());
            let returned = child
                .closed()
                .expect("replacement settled the actual query child");
            assert!(returned.matches(&child) && !returned.had_native_acquisition());
            for donor in [&last, &middle] {
                assert!(donor.driver().unwrap().is_joined());
                assert!(donor.closed().unwrap().matches(donor));
                assert!(donor.owner().closed().unwrap().matches(donor.owner()));
            }
            let closed = root.closed().unwrap();
            assert!(closed.matches(&root) && closed.had_native_acquisition());
            assert!(root.owner().closed().unwrap().matches(root.owner()));
            assert!(proxy.engine().active_sessions().is_empty());
            assert!(proxy.engine().completed_sessions().is_empty());
            assert_eq!(
                b.accepts.load(Ordering::SeqCst),
                1,
                "replacement never eagerly opens a second pool"
            );
            assert_eq!(c.accepts.load(Ordering::SeqCst), 1);
            drop(request);
            running.shutdown().await.unwrap();
            b.close().await;
            c.close().await;
            dns.close().await;
            prefix.close().await;
        }
    })
    .await
    .unwrap();
}

mod no_deadline;

#[cfg(all(feature = "socks5", feature = "shadowsocks"))]
mod business_relay;
