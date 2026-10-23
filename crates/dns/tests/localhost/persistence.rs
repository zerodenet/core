use super::*;

#[tokio::test]
async fn legacy_localhost_mapping_does_not_override_loopback_or_refresh_persistence() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("fake-ip.jsonl");
    let expires = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
        + 3_600_000;
    let records = [
        serde_json::json!({"record":"header","schema":"zero.dns.fake-ip.v3","cidr":"198.18.0.0/24","ipv6_cidr":null,"ttl_seconds":3600,"max_entries":16,"exclusions":[]}),
        serde_json::json!({"record":"upsert","domain":"localhost","ip":"198.18.0.1","expires_at_unix_ms":expires}),
        serde_json::json!({"record":"upsert","domain":"keep.example","ip":"198.18.0.2","expires_at_unix_ms":expires}),
    ];
    let data = records
        .iter()
        .map(|record| serde_json::to_string(record).unwrap() + "\n")
        .collect::<String>();
    std::fs::write(&path, data).unwrap();
    let config = config(true, DnsAddressFamilyPolicy::PreferIpv4);
    let dispatch = config.compile_dispatch(&[], None).unwrap();
    let dns = DnsSystem::build_with_egress_dispatch_and_state(
        Some(&config),
        Some(dispatch),
        Default::default(),
        Some(path.clone()),
    )
    .unwrap();
    dns.set_outbound_connector(std::sync::Arc::new(NoUpstream));
    let before = std::fs::read(&path).unwrap();
    assert_eq!(
        dns.lookup_fake_ip_domain("localhost").await.as_deref(),
        Some("198.18.0.1")
    );
    assert_eq!(dns.resolve("localhost").await.unwrap(), vec![V4, V6]);
    let answer = dns.answer_udp_query(&query("localhost", 1)).await.unwrap();
    assert!(answer.ends_with(&[127, 0, 0, 1]));
    assert_eq!(std::fs::read(&path).unwrap(), before);
    assert_eq!(
        dns.lookup_fake_ip_domain("keep.example").await.as_deref(),
        Some("198.18.0.2")
    );
    // Old clients may still hold the old address until their DNS TTL expires.
    assert_eq!(
        dns.lookup_fake_ip(&IpAddress::V4([198, 18, 0, 1]))
            .await
            .as_deref(),
        Some("localhost")
    );
}
