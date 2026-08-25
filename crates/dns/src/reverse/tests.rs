use std::time::Duration;

use zero_config::DnsReverseMappingConfig;
use zero_traits::IpAddress;

use super::{RealIpReverseIndex, RealIpReverseLookup};

fn index(max_entries: usize, max_ttl_seconds: u64) -> RealIpReverseIndex {
    RealIpReverseIndex::new(&DnsReverseMappingConfig {
        max_entries,
        max_domains_per_address: 4,
        max_ttl_seconds,
    })
}

#[tokio::test]
async fn resolves_only_unambiguous_live_domain() {
    let index = index(8, 60);
    let address = IpAddress::V4([192, 0, 2, 1]);
    index
        .record("Example.COM.", &[address], 60)
        .await;
    assert_eq!(
        index.lookup(address).await,
        RealIpReverseLookup::Resolved("example.com".to_owned())
    );

    index.record("shared.example", &[address], 60).await;
    assert_eq!(
        index.lookup(address).await,
        RealIpReverseLookup::Ambiguous
    );
}

#[tokio::test]
async fn expires_candidates_and_evicts_addresses_by_lru() {
    let index = index(2, 1);
    let first = IpAddress::V4([192, 0, 2, 1]);
    let second = IpAddress::V4([192, 0, 2, 2]);
    let third = IpAddress::V4([192, 0, 2, 3]);
    index.record("first.example", &[first], 60).await;
    index.record("second.example", &[second], 60).await;
    let _ = index.lookup(first).await;
    index.record("third.example", &[third], 60).await;
    assert_eq!(index.lookup(second).await, RealIpReverseLookup::Missing);

    tokio::time::sleep(Duration::from_millis(1_050)).await;
    assert_eq!(index.lookup(first).await, RealIpReverseLookup::Missing);
}
