//! Tagged Direct DNS uses its declared original and the actual dial policy.
use super::*;
use std::{sync::Arc, time::Duration};
use zero_dns::{DnsOutboundConnectFuture, DnsOutboundConnector};

#[derive(Debug)]
struct NoLegacy;
impl DnsOutboundConnector for NoLegacy {
    fn connect(&self, _: String, _: std::net::SocketAddr) -> DnsOutboundConnectFuture {
        Box::pin(async { panic!("ordinary Direct DNS must use its host-owned original") })
    }
}

#[tokio::test]
async fn constrained_direct_dns_uses_its_declared_original_and_live_source_policy() {
    direct(false).await;
}

#[tokio::test]
async fn constrained_direct_dns_family_rejection_cannot_retry_a_bare_carrier() {
    direct(true).await;
}

async fn direct(reject: bool) {
    tokio::time::timeout(Duration::from_secs(10), async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = listener.local_addr().unwrap();
        let dial = if reject {
            serde_json::json!({"address_family":"only_ipv6"})
        } else {
            serde_json::json!({"address_family":"only_ipv4","source_ip":"127.0.0.1"})
        };
        let config = zero_config::RuntimeConfig::parse(&serde_json::json!({
            "outbounds":[{"tag":"dns-direct","protocol":{"type":"direct"},"dial":dial}],
            "runtime":{"dns":{"servers":{
                "node":{"type":"system"},
                "query":{"type":"udp","host":"127.0.0.1","port":endpoint.port(),"detour":"dns-direct"}
            },"default_server":"query","policy":{"node_server":"node","timeout_ms":3000}}},
            "route":{"final":{"type":"direct"}}
        }).to_string()).unwrap();
        let proxy = Proxy::new(config.clone()).unwrap();
        proxy.resolver.set_outbound_connector(Arc::new(NoLegacy));
        let end = || tokio::time::Instant::now()+Duration::from_secs(3);
        let running = proxy.spawn();
        let handle = crate::ProxyHandle::new(zero_engine::EngineHandle::new(proxy.engine().clone()), proxy.clone());
        handle.apply_runtime_config_and_wait(config,Duration::from_secs(3)).await.unwrap();
        let mut query = tokio::spawn({
            let resolver=proxy.resolver.clone();
            async move {resolver.resolve_real_type("constrained.direct.dns.test",1).await}
        });
        if reject {
            assert!(query.await.unwrap().is_err());
            std::future::poll_fn(|cx| {
                assert!(listener.poll_accept(cx).is_pending());
                std::task::Poll::Ready(())
            }).await;
        } else {
            let (mut stream,source)=tokio::select! {
                accepted = listener.accept() => accepted.unwrap(),
                result = &mut query => panic!("Direct DNS returned before actual native acceptance: {result:?}"),
            };
            assert_eq!(source.ip(),endpoint.ip());
            let size=stream.read_u16().await.unwrap() as usize;
            let mut bytes=vec![0;size];
            stream.read_exact(&mut bytes).await.unwrap();
            let observed=tokio::time::timeout_at(end(),async {
                loop {
                    if let Some(query)=proxy.resolver.native_query_observations().into_iter().find(|query|query.io_pending&&query.original.is_some()) {break query;}
                    tokio::task::yield_now().await;
                }
            }).await.unwrap();
            let original=observed.original.as_ref().unwrap();
            assert!(original.parent().is_none());
            assert!(original.closed().is_none());
            let answer=zero_dns::udp::build_dns_response(&bytes,&[IpAddress::V4([198,51,100,27])]);
            stream.write_u16(answer.len() as u16).await.unwrap();
            stream.write_all(&answer).await.unwrap();
            assert_eq!(proxy.protocols.drive_component_supervision(Box::pin(query)).await.unwrap().unwrap(),vec![IpAddress::V4([198,51,100,27])]);
            proxy.protocols.drive_component_supervision(Box::pin(observed.task.wait())).await;
            let closed=original.closed().expect("same original native controller receipt");
            assert!(closed.matches(original)&&closed.had_native_acquisition());
        }
        assert!(proxy.engine().active_sessions().is_empty());
        assert!(proxy.engine().completed_sessions().is_empty());
        running.shutdown().await.unwrap();
    }).await.unwrap();
}
