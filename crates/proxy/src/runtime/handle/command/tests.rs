use std::io;

use zero_api::{ApiErrorCode, CommandRequest, CommandService, FakeIpClearCommand};
use zero_config::RuntimeConfig;
use zero_engine::EngineError;
use zero_engine::EngineHandle;
use zero_traits::DnsResolver;

use crate::runtime::Proxy;

use super::super::ProxyHandle;

use super::tun::{map_tun_start_error, TUN_PRIVILEGE_MESSAGE};

#[test]
fn tun_start_maps_host_permission_failures_to_a_stable_api_error() {
    let error = map_tun_start_error(EngineError::Io(io::Error::new(
        io::ErrorKind::PermissionDenied,
        "platform-specific permission diagnostic",
    )));

    assert_eq!(error.code, ApiErrorCode::InsufficientOsPrivilege);
    assert_eq!(error.message, TUN_PRIVILEGE_MESSAGE);
    assert_eq!(
        error.cause.as_deref(),
        Some("platform-specific permission diagnostic")
    );
}

#[test]
fn tun_start_keeps_unclassified_runtime_failures_internal() {
    let error = map_tun_start_error(EngineError::Io(io::Error::other("device failed")));

    assert_eq!(error.code, ApiErrorCode::Internal);
    assert_eq!(error.message, "device failed");
}

#[tokio::test(flavor = "multi_thread")]
async fn fake_ip_clear_command_supports_address_and_all_scopes() {
    let config = RuntimeConfig::parse(
        r#"{
            "runtime": {
                "dns": {
                    "servers": { "system": { "type": "system" } },
                    "default_server": "system",
                    "answer": {
                        "type": "fake_ip",
                        "cidr": "198.18.0.0/24",
                        "ttl_seconds": 60,
                        "max_entries": 16
                    }
                }
            },
            "route": { "rules": [], "final": { "type": "direct" } }
        }"#,
    )
    .expect("parse Fake-IP config");
    let proxy = Proxy::new(config).expect("build proxy");
    let resolver = proxy.resolver.clone();
    DnsResolver::resolve(resolver.as_ref(), "first.example")
        .await
        .expect("allocate first mapping");
    DnsResolver::resolve(resolver.as_ref(), "second.example")
        .await
        .expect("allocate second mapping");
    let handle = ProxyHandle::new(EngineHandle::new(proxy.engine().clone()), proxy);

    let response = tokio::task::block_in_place(|| {
        handle.execute(CommandRequest::FakeIpClear(FakeIpClearCommand {
            domain: None,
            ip: Some("198.18.0.1".to_owned()),
        }))
    })
    .expect("clear mapping by address");
    let result = response.result.expect("clear result");
    assert_eq!(result["scope"], "ip");
    assert_eq!(result["removed_mappings"], 1);
    assert_eq!(result["live_mappings"], 1);
    assert!(resolver
        .lookup_fake_ip_domain("first.example")
        .await
        .is_none());

    let response = tokio::task::block_in_place(|| {
        handle.execute(CommandRequest::FakeIpClear(FakeIpClearCommand::default()))
    })
    .expect("clear all mappings");
    let result = response.result.expect("clear-all result");
    assert_eq!(result["scope"], "all");
    assert_eq!(result["removed_mappings"], 1);
    assert_eq!(result["live_mappings"], 0);
}

#[test]
fn fake_ip_clear_rejects_multiple_selectors_before_execution() {
    let config = RuntimeConfig::parse(r#"{"route":{"rules":[],"final":{"type":"direct"}}}"#)
        .expect("parse minimal config");
    let proxy = Proxy::new(config).expect("build proxy");
    let handle = ProxyHandle::new(EngineHandle::new(proxy.engine().clone()), proxy);

    let error = handle
        .execute(CommandRequest::FakeIpClear(FakeIpClearCommand {
            domain: Some("example.com".to_owned()),
            ip: Some("198.18.0.1".to_owned()),
        }))
        .expect_err("multiple selectors must fail");
    assert_eq!(error.code, ApiErrorCode::InvalidArgument);
}
