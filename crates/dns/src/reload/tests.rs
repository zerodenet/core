use super::*;

fn config() -> DnsConfig {
    serde_json::from_value(serde_json::json!({
        "servers":{"local":{"type":"system"}}, "default_server":"local"
    }))
    .unwrap()
}
fn backend(dns: &DnsSystem) -> Arc<ResolverBackend> {
    match &*dns.inner.read().unwrap() {
        DnsSystemInner::Configured { servers, .. } => servers["local"].backend.clone(),
        _ => panic!("configured DNS required"),
    }
}
fn prepare(dns: &DnsSystem, config: &DnsConfig) -> DnsReloadResources {
    dns.prepare_reload_resources_with_dispatch(
        Some(config),
        compile_standalone_dispatch(Some(config)).unwrap(),
        &BTreeSet::new(),
    )
    .unwrap()
}

#[test]
fn rule_policy_changes_publish_the_exact_prepared_backend_without_rebuilding_it() {
    let mut config = config();
    let dns = DnsSystem::build(Some(&config)).unwrap();
    let original = backend(&dns);
    config.policy.timeout_ms += 1;
    let candidate = prepare(&dns, &config);
    assert!(Arc::ptr_eq(
        &original,
        &candidate.0.servers["local"].backend
    ));
    let prepared = candidate.0.servers["local"].backend.clone();
    dns.commit_reload_resources(&candidate).unwrap();
    assert!(Arc::ptr_eq(&prepared, &backend(&dns)));
}

#[test]
fn a_token_cannot_publish_or_retire_another_candidate_or_another_dns_system() {
    let config = config();
    let dns = DnsSystem::build(Some(&config)).unwrap();
    let other = DnsSystem::build(Some(&config)).unwrap();
    let stale = prepare(&dns, &config);
    dns.discard_prepared_reload();
    let current = prepare(&dns, &config);
    assert_eq!(
        dns.commit_reload_resources(&stale).unwrap_err().kind(),
        io::ErrorKind::Interrupted
    );
    assert_eq!(
        other.commit_reload_resources(&current).unwrap_err().kind(),
        io::ErrorKind::Interrupted
    );
    dns.commit_reload_resources(&current).unwrap();
}

#[cfg(feature = "doh")]
#[tokio::test]
async fn cold_prepare_leaves_current_pool_open_and_requires_retirement_before_replacement() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let mut config: DnsConfig = serde_json::from_value(serde_json::json!({
        "servers":{"local":{"type":"doh","host":"127.0.0.1","port":listener.local_addr().unwrap().port()}},
        "default_server":"local"
    })).unwrap();
    let dns = DnsSystem::build(Some(&config)).unwrap();
    let original = backend(&dns);
    if let zero_config::DnsServerConfig::Doh { path, .. } = config.servers.get_mut("local").unwrap()
    {
        *path = "/replacement".to_owned();
    }
    let candidate = prepare(&dns, &config);
    assert!(original.resource_admission_open());
    assert!(!Arc::ptr_eq(
        &original,
        &candidate.0.servers["local"].backend
    ));
    assert_eq!(
        dns.commit_reload_resources(&candidate).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    assert!(
        original.resource_admission_open(),
        "commit rejection does not implicitly retire current"
    );
    assert_eq!(
        listener.accept().unwrap_err().kind(),
        io::ErrorKind::WouldBlock,
        "prepare and rejected commit did no network I/O"
    );
    dns.retire_reload_resources(
        &candidate,
        tokio::time::Instant::now() + std::time::Duration::from_secs(1),
    )
    .await
    .unwrap();
    assert!(!original.resource_admission_open());
    let actual = candidate.0.servers["local"].backend.clone();
    dns.commit_reload_resources(&candidate).unwrap();
    assert!(Arc::ptr_eq(&actual, &backend(&dns)));
}
