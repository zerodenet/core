use super::*;
use zero_dns::native::NativeDnsOpenFailure;

fn settings(dns: &Dns, fallback: bool) -> zero_config::DnsConfig {
    let mut settings = dns.config();
    settings.servers.insert(
        "refusing".into(),
        zero_config::DnsServerConfig::Dot {
            // A TCP connect to a broadcast destination is rejected by the real
            // kernel. It does not depend on an unreserved port or a bound but
            // non-listening endpoint (which can silently time out on macOS).
            host: "255.255.255.255".into(),
            port: 53,
            bootstrap: vec![],
            server_name: Some("refusing.native.test".into()),
            detour: None,
        },
    );
    settings.policy.node_server = Some("refusing".into());
    if fallback {
        settings.policy.node_fallback_servers = vec!["native".into()];
    }
    settings
}
fn failure<'a>(error: &'a (dyn std::error::Error + 'static)) -> Option<&'a NativeDnsOpenFailure> {
    if let Some(failure) = error.downcast_ref::<NativeDnsOpenFailure>() {
        return Some(failure);
    }
    if let Some(inner) = error
        .downcast_ref::<std::io::Error>()
        .and_then(std::io::Error::get_ref)
    {
        if let Some(found) = failure(inner) {
            return Some(found);
        }
    }
    error.source().and_then(failure)
}
fn native_open_error(error: &(dyn std::error::Error + 'static)) -> bool {
    if error
        .downcast_ref::<std::io::Error>()
        .is_some_and(|error| error.raw_os_error().is_some())
    {
        return true;
    }
    error.source().is_some_and(native_open_error)
}

#[tokio::test]
async fn actual_failed_dns_tcp_open_retains_original_close_and_native_os_error_without_fallback() {
    let mut dns = Dns::new().await;
    let mut config = config(["127.0.0.1:1".parse().unwrap(); 3]);
    config.runtime.dns = Some(settings(&dns, false));
    let (proxy, running, _) = start(config).await;
    let error = proxy
        .resolver
        .resolve_node("refused.node.test")
        .await
        .unwrap_err();
    let actual = failure(&error)
        .unwrap_or_else(|| panic!("actual failed opening lost its typed close proof: {error:?}"));
    assert!(actual.original_close().had_native_acquisition());
    assert!(actual.original_close().borrowed_origin().is_none());
    assert!(actual.original_close().driver_exit().is_none());
    assert!(
        native_open_error(actual.opening_error()),
        "retain the actual OS connect error"
    );
    dns.no_new_request();
    running.shutdown().await.unwrap();
    dns.close().await;
}

#[tokio::test]
async fn actual_failed_dns_tcp_open_closes_before_existing_fallback_with_the_native_budget_full() {
    let mut dns = Dns::new().await;
    let mut config = config(["127.0.0.1:1".parse().unwrap(); 3]);
    config.runtime.dns = Some(settings(&dns, true));
    let (proxy, running, _) = start(config).await;
    let mut held_requests = Vec::new();
    let mut held_jobs = Vec::new();
    for index in 0..crate::component::native_dns::LIMIT - 1 {
        let resolver = proxy.resolver.clone();
        held_jobs.push(tokio::spawn(async move {
            resolver
                .resolve_real_type(&format!("held-{index}.node.test"), 1)
                .await
        }));
        held_requests.push(dns.request().await);
    }
    let held = proxy.resolver.native_query_observations();
    assert_eq!(held.len(), crate::component::native_dns::LIMIT - 1);
    assert!(held
        .iter()
        .all(|query| query.io_pending && !query.task.is_joined()));
    let resolver = proxy.resolver.clone();
    let fallback = tokio::spawn(async move { resolver.resolve_node("fallback.node.test").await });
    // Fifteen originals stay pending. The failed TCP original fills slot 16;
    // fallback can open only after its actual original close refunds that slot.
    let response = dns.request().await;
    assert!(!response.tcp);
    assert!(held_jobs.iter().all(|job| !job.is_finished()));
    response.answer(false, false);
    assert_eq!(
        fallback.await.unwrap().unwrap(),
        vec![zero_traits::IpAddress::V4([127, 0, 0, 1])]
    );
    running.shutdown().await.unwrap();
    for job in held_jobs {
        assert!(job.await.unwrap().is_err());
    }
    for query in held {
        assert!(query.task.is_joined());
        closed(&original(&query));
    }
    drop(held_requests);
    dns.close().await;
}
