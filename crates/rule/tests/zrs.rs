use std::fs;
use std::net::{IpAddr, Ipv4Addr};

use ipnet::Ipv4Net;
use sha2::{Digest, Sha256};
use zero_rule::zrs::{
    encode, verify, MappedRuleSet, PrewarmPolicy, VerifiedRuleSet, VerifyMode, ZrsError,
};
use zero_rule::{PreparedRuleQuery, Rule, RuleMatch, RuleMatcher, RuleSet, RuleSetCompiler};

fn compiled() -> zero_rule::CompiledRuleSet {
    RuleSetCompiler
        .compile(
            RuleSet::new(vec![
                Rule::DomainExact("api.example.com".to_owned()),
                Rule::DomainSuffix("service.example".to_owned()),
                Rule::DomainKeyword("keyword".to_owned()),
                Rule::Ipv4Cidr(Ipv4Net::new(Ipv4Addr::new(10, 0, 0, 0), 8).unwrap()),
            ])
            .with_display_name("test rules"),
        )
        .expect("compile")
        .0
}

#[test]
fn encoded_zrs_round_trips_through_verified_view() {
    let bytes = encode(&compiled()).expect("encode");
    assert_eq!(bytes.len(), 416);
    assert_eq!(crc32fast::hash(&bytes), 0xb61f_c5a3);
    assert_eq!(
        format!("{:x}", Sha256::digest(&bytes)),
        "6c4af864f13631c75168a701897f2c91441e92128aa23d6f589dcefcb5587be6"
    );
    let metadata = verify(&bytes, VerifyMode::FullChecksum).expect("inspect metadata");
    assert_eq!(metadata.body_checksum, 0xb752_9134);
    assert_eq!(metadata.display_name.as_deref(), Some("test rules"));
    assert_eq!(metadata.counts.domain_exact, 1);
    assert_eq!(metadata.counts.domain_suffix, 1);
    assert_eq!(metadata.counts.domain_keyword, 1);
    assert_eq!(metadata.counts.ipv4_ranges, 1);
    assert_eq!(metadata.entry_count(), 4);
    assert_eq!(metadata.index_bytes(), 161);
    assert_eq!(metadata.section_bytes.ipv4_ranges, 16);
    assert_eq!(&bytes[0..4], b"ZRS!");
    let view = VerifiedRuleSet::from_bytes(&bytes, VerifyMode::FullChecksum).expect("verify");
    assert_eq!(view.display_name(), Some("test rules"));

    for query in [
        PreparedRuleQuery::new(Some("api.example.com"), None).unwrap(),
        PreparedRuleQuery::new(Some("a.service.example"), None).unwrap(),
        PreparedRuleQuery::new(Some("has-keyword.example"), None).unwrap(),
        PreparedRuleQuery::new(None, Some(IpAddr::V4(Ipv4Addr::new(10, 1, 2, 3)))).unwrap(),
    ] {
        assert!(view.matches(&query));
    }

    let exact = PreparedRuleQuery::new(Some("api.example.com"), None).unwrap();
    assert_eq!(view.lookup(&exact), Some(RuleMatch::DomainExact));
}

#[test]
fn verifier_rejects_corruption_and_invalid_structure() {
    let bytes = encode(&compiled()).expect("encode");
    let mut bad_magic = bytes.clone();
    bad_magic[0] = 0;
    assert!(matches!(
        VerifiedRuleSet::from_bytes(&bad_magic, VerifyMode::Structure),
        Err(ZrsError::InvalidMagic)
    ));

    let mut bad_checksum = bytes;
    *bad_checksum.last_mut().unwrap() ^= 1;
    assert!(matches!(
        VerifiedRuleSet::from_bytes(&bad_checksum, VerifyMode::FullChecksum),
        Err(ZrsError::ChecksumMismatch)
    ));

    let mut unknown_required = encode(&compiled()).unwrap();
    unknown_required[128..130].copy_from_slice(&99_u16.to_le_bytes());
    assert!(matches!(
        VerifiedRuleSet::from_bytes(&unknown_required, VerifyMode::Structure),
        Err(ZrsError::InvalidSection { kind: 99, .. })
    ));

    let mut invalid_range = encode(&compiled()).unwrap();
    let ipv4_entry = 128 + 3 * 24;
    let offset = u64::from_le_bytes(
        invalid_range[ipv4_entry + 8..ipv4_entry + 16]
            .try_into()
            .unwrap(),
    ) as usize;
    invalid_range[offset + 8..offset + 12].copy_from_slice(&20_u32.to_le_bytes());
    invalid_range[offset + 12..offset + 16].copy_from_slice(&10_u32.to_le_bytes());
    assert!(matches!(
        VerifiedRuleSet::from_bytes(&invalid_range, VerifyMode::Semantic),
        Err(ZrsError::InvalidSection { kind: 4, .. })
    ));

    let mut reserved_range = encode(&compiled()).unwrap();
    let ipv4_entry = 128 + 3 * 24;
    let offset = u64::from_le_bytes(
        reserved_range[ipv4_entry + 8..ipv4_entry + 16]
            .try_into()
            .unwrap(),
    ) as usize;
    reserved_range[offset + 4] = 1;
    assert!(matches!(
        VerifiedRuleSet::from_bytes(&reserved_range, VerifyMode::Structure),
        Err(ZrsError::InvalidSection { kind: 4, .. })
    ));

    let mut reserved_strings = encode(&compiled()).unwrap();
    let keyword_entry = 128 + 2 * 24;
    let offset = u64::from_le_bytes(
        reserved_strings[keyword_entry + 8..keyword_entry + 16]
            .try_into()
            .unwrap(),
    ) as usize;
    reserved_strings[offset + 4] = 1;
    assert!(matches!(
        VerifiedRuleSet::from_bytes(&reserved_strings, VerifyMode::Structure),
        Err(ZrsError::InvalidSection { kind: 3, .. })
    ));

    let mut non_canonical_keyword = encode(&compiled()).unwrap();
    let keyword_entry = 128 + 2 * 24;
    let offset = u64::from_le_bytes(
        non_canonical_keyword[keyword_entry + 8..keyword_entry + 16]
            .try_into()
            .unwrap(),
    ) as usize;
    let keyword_position = non_canonical_keyword[offset..]
        .windows(b"keyword".len())
        .position(|window| window == b"keyword")
        .unwrap()
        + offset;
    non_canonical_keyword[keyword_position] = b'K';
    assert!(matches!(
        VerifiedRuleSet::from_bytes(&non_canonical_keyword, VerifyMode::Semantic),
        Err(ZrsError::InvalidSection { kind: 3, .. })
    ));
}

#[test]
fn mmap_view_owns_mapping_for_queries_and_is_thread_safe() {
    let path = std::env::temp_dir().join(format!("zero-rule-{}.zrs", std::process::id()));
    fs::write(&path, encode(&compiled()).unwrap()).expect("write fixture");
    let mapped = MappedRuleSet::open(&path, VerifyMode::FullChecksum).expect("map fixture");
    assert_eq!(mapped.prewarm(PrewarmPolicy::Roots).touched_pages, 1);
    assert_eq!(mapped.prewarm(PrewarmPolicy::FullFile).touched_pages, 1);
    let query = PreparedRuleQuery::new(Some("api.example.com"), None).unwrap();
    std::thread::scope(|scope| {
        for _ in 0..4 {
            scope.spawn(|| assert!(mapped.matches(&query)));
        }
    });
    drop(mapped);
    fs::remove_file(path).expect("remove fixture");
}

#[test]
fn domain_only_zrs_does_not_require_destination_ip() {
    let domain_only = RuleSetCompiler
        .compile(RuleSet::new(vec![Rule::DomainSuffix(
            "example.com".to_owned(),
        )]))
        .expect("compile domain-only rules")
        .0;
    let bytes = encode(&domain_only).expect("encode domain-only rules");
    let verified =
        VerifiedRuleSet::from_bytes(&bytes, VerifyMode::FullChecksum).expect("verify rules");
    assert!(!verified.requires_destination_ip());

    let path =
        std::env::temp_dir().join(format!("zero-rule-domain-only-{}.zrs", std::process::id()));
    fs::write(&path, bytes).expect("write fixture");
    let mapped = MappedRuleSet::open(&path, VerifyMode::FullChecksum).expect("map fixture");
    assert!(!mapped.requires_destination_ip());
    drop(mapped);
    fs::remove_file(path).expect("remove fixture");
}

#[test]
fn verifier_enforces_section_resource_limits_before_access() {
    let mut excessive_sections = encode(&compiled()).unwrap();
    excessive_sections[10..12].copy_from_slice(&65_u16.to_le_bytes());
    assert!(matches!(
        VerifiedRuleSet::from_bytes(&excessive_sections, VerifyMode::Structure),
        Err(ZrsError::ResourceLimit {
            resource: "section count",
            ..
        })
    ));

    let mut excessive_section_size = encode(&compiled()).unwrap();
    excessive_section_size[144..152].copy_from_slice(&(512_u64 * 1024 * 1024 + 1).to_le_bytes());
    assert!(matches!(
        VerifiedRuleSet::from_bytes(&excessive_section_size, VerifyMode::Structure),
        Err(ZrsError::ResourceLimit {
            resource: "section size",
            ..
        })
    ));
}

#[test]
fn verifier_never_panics_on_truncation_or_single_byte_corruption() {
    let bytes = encode(&compiled()).unwrap();
    for length in 0..bytes.len() {
        assert!(std::panic::catch_unwind(|| {
            let _ = VerifiedRuleSet::from_bytes(&bytes[..length], VerifyMode::Structure);
        })
        .is_ok());
    }
    for index in 0..bytes.len() {
        let mut corrupted = bytes.clone();
        corrupted[index] ^= 0x80;
        assert!(std::panic::catch_unwind(|| {
            let _ = VerifiedRuleSet::from_bytes(&corrupted, VerifyMode::FullChecksum);
        })
        .is_ok());
    }
}

#[test]
fn verifier_never_panics_on_deterministic_arbitrary_bytes() {
    let mut state = 0x6a09_e667_f3bc_c909_u64;
    for length in 0..2048 {
        let mut bytes = vec![0_u8; length];
        for byte in &mut bytes {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            *byte = state as u8;
        }
        assert!(std::panic::catch_unwind(|| {
            let _ = verify(&bytes, VerifyMode::FullChecksum);
        })
        .is_ok());
    }
}
