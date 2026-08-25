use std::collections::BTreeMap;
use std::path::Path;

use zero_config::{DnsAnswerConfig, DnsConfig, DnsServerConfig};

fn config(cidr: &str, ttl_seconds: u64) -> DnsConfig {
    DnsConfig {
        servers: BTreeMap::from([("system".to_owned(), DnsServerConfig::System)]),
        default_server: "system".to_owned(),
        dispatch: Vec::new(),
        cache: None,
        answer: DnsAnswerConfig::FakeIp {
            cidr: cidr.to_owned(),
            ipv6_cidr: None,
            ttl_seconds,
            max_entries: Some(16),
            exclude_domains: Vec::new(),
        },
        policy: Default::default(),
    }
}

fn build(config: &DnsConfig, state_path: &Path) -> zero_dns::DnsSystem {
    let dispatch = config
        .compile_dispatch(&[], None)
        .expect("compile DNS dispatch");
    zero_dns::DnsSystem::build_with_egress_dispatch_and_state(
        Some(config),
        Some(dispatch),
        zero_platform_tokio::EgressInterfaceControl::default(),
        Some(state_path.to_path_buf()),
    )
    .expect("build persistent DNS")
}

#[tokio::test]
async fn restores_live_mapping_after_process_rebuild() {
    let directory = tempfile::tempdir().expect("state directory");
    let path = directory.path().join("fake-ip.jsonl");
    let config = config("198.18.0.0/24", 3_600);

    let first = build(&config, &path);
    let assigned = zero_traits::DnsResolver::resolve(&first, "Restart.Example.")
        .await
        .expect("allocate Fake-IP");
    assert_eq!(assigned, vec![zero_traits::IpAddress::V4([198, 18, 0, 1])]);
    drop(first);

    let restored = build(&config, &path);
    assert_eq!(
        restored
            .lookup_fake_ip_domain("restart.example")
            .await
            .as_deref(),
        Some("198.18.0.1")
    );
    assert_eq!(
        restored
            .lookup_fake_ip(&zero_traits::IpAddress::V4([198, 18, 0, 1]))
            .await
            .as_deref(),
        Some("restart.example")
    );
}

#[tokio::test]
async fn incompatible_pool_starts_with_an_empty_mapping_set() {
    let directory = tempfile::tempdir().expect("state directory");
    let path = directory.path().join("fake-ip.jsonl");
    let original = config("198.18.0.0/24", 3_600);
    let changed = config("198.19.0.0/24", 3_600);

    let first = build(&original, &path);
    zero_traits::DnsResolver::resolve(&first, "pool.example")
        .await
        .expect("allocate original Fake-IP");
    drop(first);

    let rebuilt = build(&changed, &path);
    assert!(rebuilt
        .lookup_fake_ip_domain("pool.example")
        .await
        .is_none());
    assert_eq!(
        zero_traits::DnsResolver::resolve(&rebuilt, "pool.example")
            .await
            .expect("allocate changed Fake-IP"),
        vec![zero_traits::IpAddress::V4([198, 19, 0, 1])]
    );
}

#[tokio::test]
async fn incompatible_hot_reload_replaces_the_persistent_allocator() {
    let directory = tempfile::tempdir().expect("state directory");
    let path = directory.path().join("fake-ip.jsonl");
    let original = config("198.18.0.0/24", 3_600);
    let changed = config("198.19.0.0/24", 3_600);
    let dns = build(&original, &path);

    zero_traits::DnsResolver::resolve(&dns, "reload-pool.example")
        .await
        .expect("allocate original Fake-IP");
    dns.reload(Some(&changed))
        .expect("replace allocator while old journal is open");
    assert_eq!(
        zero_traits::DnsResolver::resolve(&dns, "reload-pool.example")
            .await
            .expect("allocate reloaded Fake-IP"),
        vec![zero_traits::IpAddress::V4([198, 19, 0, 1])]
    );
    drop(dns);

    let restored = build(&changed, &path);
    assert_eq!(
        restored
            .lookup_fake_ip_domain("reload-pool.example")
            .await
            .as_deref(),
        Some("198.19.0.1")
    );
}

#[test]
fn rejects_a_second_live_owner_of_the_same_state() {
    let directory = tempfile::tempdir().expect("state directory");
    let path = directory.path().join("fake-ip.jsonl");
    let config = config("198.18.0.0/24", 3_600);
    let _first = build(&config, &path);
    let dispatch = config
        .compile_dispatch(&[], None)
        .expect("compile DNS dispatch");

    let error = zero_dns::DnsSystem::build_with_egress_dispatch_and_state(
        Some(&config),
        Some(dispatch),
        zero_platform_tokio::EgressInterfaceControl::default(),
        Some(path),
    )
    .expect_err("second owner must fail before serving Fake-IP answers");
    assert_eq!(error.kind(), std::io::ErrorKind::AlreadyExists);
    assert!(error.to_string().contains("already owned"));
}

#[tokio::test]
async fn corrupt_state_is_quarantined_and_reinitialized() {
    let directory = tempfile::tempdir().expect("state directory");
    let path = directory.path().join("fake-ip.jsonl");
    std::fs::write(&path, b"not-json\n").expect("write corrupt state");

    let dns = build(&config("198.18.0.0/24", 3_600), &path);
    assert_eq!(
        zero_traits::DnsResolver::resolve(&dns, "healthy.example")
            .await
            .expect("allocate after recovery"),
        vec![zero_traits::IpAddress::V4([198, 18, 0, 1])]
    );
    let quarantined = std::fs::read_dir(directory.path())
        .expect("list state directory")
        .filter_map(Result::ok)
        .any(|entry| {
            entry
                .file_name()
                .to_string_lossy()
                .starts_with("fake-ip.jsonl.corrupt-")
        });
    assert!(
        quarantined,
        "corrupt state should remain available for diagnosis"
    );
}

#[tokio::test]
async fn expired_mapping_is_not_restored() {
    let directory = tempfile::tempdir().expect("state directory");
    let path = directory.path().join("fake-ip.jsonl");
    let config = config("198.18.0.0/24", 1);

    let first = build(&config, &path);
    zero_traits::DnsResolver::resolve(&first, "expired.example")
        .await
        .expect("allocate Fake-IP");
    drop(first);
    tokio::time::sleep(std::time::Duration::from_millis(1_100)).await;

    let rebuilt = build(&config, &path);
    assert!(rebuilt
        .lookup_fake_ip_domain("expired.example")
        .await
        .is_none());
}

#[tokio::test]
async fn migrates_v1_ipv4_state_into_dual_stack_v2_journal() {
    let directory = tempfile::tempdir().expect("state directory");
    let path = directory.path().join("fake-ip.jsonl");
    let expires_at_unix_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
        + 3_600_000;
    std::fs::write(
        &path,
        format!(
            "{{\"record\":\"header\",\"schema\":\"zero.dns.fake-ip.v1\",\"cidr\":\"198.18.0.0/24\",\"ttl_seconds\":3600,\"max_entries\":16,\"exclusions\":[]}}\n{{\"record\":\"upsert\",\"domain\":\"migrate.example\",\"ip\":[198,18,0,7],\"expires_at_unix_ms\":{expires_at_unix_ms}}}\n"
        ),
    )
    .expect("write legacy state");
    let mut config = config("198.18.0.0/24", 3_600);
    let DnsAnswerConfig::FakeIp { ipv6_cidr, .. } = &mut config.answer else {
        unreachable!()
    };
    *ipv6_cidr = Some("fd00::/120".to_owned());

    let dns = build(&config, &path);
    assert_eq!(
        dns.lookup_fake_ip(&zero_traits::IpAddress::V4([198, 18, 0, 7]))
            .await
            .as_deref(),
        Some("migrate.example")
    );
    let response = dns
        .answer_udp_query(&dns_query("migrate.example", 28))
        .await
        .expect("allocate migrated IPv6 mapping");
    assert_eq!(u16::from_be_bytes([response[6], response[7]]), 1);
    drop(dns);

    let journal = std::fs::read_to_string(path).expect("read migrated journal");
    assert!(journal.lines().next().unwrap().contains("zero.dns.fake-ip.v2"));
    assert!(journal.contains("198.18.0.7"));
    assert!(journal.contains("fd00::"));
}

fn dns_query(domain: &str, query_type: u16) -> Vec<u8> {
    let mut query = vec![
        0x12, 0x34, 0x01, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    ];
    for label in domain.split('.') {
        query.push(label.len() as u8);
        query.extend_from_slice(label.as_bytes());
    }
    query.push(0);
    query.extend_from_slice(&query_type.to_be_bytes());
    query.extend_from_slice(&1_u16.to_be_bytes());
    query
}
