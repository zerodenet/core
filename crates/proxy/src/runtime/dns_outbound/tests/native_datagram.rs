//! Independent SS UDP and SOCKS control/BND use their actual original grants.
use super::*;
use futures_util::FutureExt;
use std::{sync::Arc, time::Duration};
use zero_platform_tokio::controlled_carrier::CarrierOrigin;
#[path = "native_datagram/fixture.rs"]
mod fixture;
use fixture::*;
fn deadline() -> tokio::time::Instant {
    tokio::time::Instant::now() + Duration::from_secs(3)
}
#[tokio::test]
async fn independent_dns_datagrams_return_selected_native_and_related_control_before_retry() {
    tokio::time::timeout(Duration::from_secs(30), async {
        for (kind, tcp_only_plugin) in [
            #[cfg(feature="socks5")] ("socks5", false),
            #[cfg(feature="shadowsocks")] ("shadowsocks", false),
            #[cfg(all(feature="shadowsocks",target_os="linux"))] ("shadowsocks", true),
        ] {
            eprintln!("native Datagram DNS family starts: kind={kind}, tcp_only_plugin={tcp_only_plugin}");
            let remote = Remote::new(kind).await;
            let mut dns = crate::dns_fixture::Dns::new().await;
            let (proxy, running, _material) = client(&remote, &dns, tcp_only_plugin).await;
            let mut previous = Vec::<CarrierOrigin>::new();
            for (index, cancel) in [false, true, false].into_iter().enumerate() {
                let resolver = proxy.resolver.clone();
                let name = format!("{index}.{kind}.independent.udp.dns.test");
                let mut query = tokio::spawn(async move { resolver.resolve_real_type(&name, 1).await });
                eprintln!("native Datagram DNS request starts: kind={kind}, tcp_only_plugin={tcp_only_plugin}, attempt={index}, cancel={cancel}, remote={:?}, target={:?}", remote.address, dns.address);
                let request = tokio::select! {
                    request = std::panic::AssertUnwindSafe(dns.request()).catch_unwind() => match request {
                        Ok(request)=>request,
                        Err(original)=>{
                            eprintln!("native Datagram DNS original request deadline failed: kind={kind}, tcp_only_plugin={tcp_only_plugin}, attempt={index}");
                            remote.diagnose();
                            eprintln!("actual Query observations: {:?}",proxy.resolver.native_query_observations());
                            eprintln!("actual Query Root: {:?}",proxy.protocols.runtime_host().original_budgets().carrier.snapshot());
                            std::panic::resume_unwind(original)
                        }
                    },
                    result = &mut query => panic!("{kind} Datagram DNS failed before actual UDP: {result:?}"),
                };
                assert!(!request.tcp, "the actual standard Datagram role must reach UDP DNS");
                remote.assert_original_process_lookup(index + 1).await;
                let actual = pending(&proxy).await;
                let child = actual.original.as_ref().unwrap();
                let native = child.parent().expect("actual native Datagram donor").clone();
                assert!(child.closed().is_none() && native.closed().is_none());
                assert!(native.driver().is_none() && native.parent().is_none());
                let plan = proxy.tcp_runtime_services().components.clone().unwrap();
                let slot = plan.configured("node").unwrap();
                assert_eq!(native.instance(), slot.instance_id());
                let related = if kind == "socks5" {
                    let control = native.related_control().expect("negotiated UDP keeps its exact TCP original");
                    assert!(!control.same_original(&native));
                    assert_eq!(control.endpoints().unwrap().peer(), remote.address);
                    assert!(control.closed().is_none() && control.driver().is_none());
                    Some(control)
                } else {
                    assert_eq!(native.endpoints().unwrap().peer(), remote.address);
                    assert!(native.related_control().is_none());
                    None
                };
                if cancel {
                    query.abort(); assert!(query.await.unwrap_err().is_cancelled()); drop(request);
                } else {
                    request.answer(false, false);
                    assert_eq!(query.await.unwrap().unwrap(), vec![IpAddress::V4([127,0,0,1])]);
                }
                let joined = tokio::time::timeout_at(deadline(), actual.task.wait_registration_released()).await.unwrap();
                assert!(actual.task.matches_joined(&joined) && joined.registration_released()); joined.result().unwrap();
                let returned = child.closed().unwrap(); assert!(returned.matches(child) && !returned.had_native_acquisition());
                closed(&native);
                if let Some(control) = related { closed(&control); }
                for old in &previous { assert!(!old.same_original(&native)); closed(old); }
                previous.push(native);
                dns.no_new_request();
                #[cfg(target_os="linux")]
                if tcp_only_plugin {
                    assert_eq!(proxy.protocols.runtime_host().original_budgets().process.snapshot().charged_jobs,0,
                        "UDP must not start the configured TCP-only plugin");
                }
            }
            assert!(proxy.engine().active_sessions().is_empty() && proxy.engine().completed_sessions().is_empty());
            running.shutdown().await.unwrap();
            for original in previous {
                assert!(original.owner().closed().unwrap().matches(original.owner()));
                assert!(original.owner().users_closed().unwrap().matches(original.owner()));
            }
            assert_eq!(proxy.protocols.runtime_host().original_budgets().carrier.snapshot().charged,0);
            remote.close().await; dns.close().await;
        }
    }).await.unwrap();
}
fn closed(original: &CarrierOrigin) {
    let closed = original
        .closed()
        .expect("the original native provider's positive close");
    assert!(closed.matches(original) && closed.had_native_acquisition());
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
                return actual;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap()
}
