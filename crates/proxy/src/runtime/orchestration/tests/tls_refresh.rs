//! Real ordinary factories: cold files, TLS rotation and native provider closure.
use super::inbound_receipts::{closed, owner};
use serde_json::json;
use std::{sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    time::timeout,
};
mod fixture;
mod ocsp;
mod quic;

#[test]
fn all_standard_tls_factories_declare_cold_original_files_and_finite_workers() {
    for protocol in [
        json!({"type":"vless","users":[{"id":"11111111-2222-3333-4444-555555555555"}],"tls":{"cert_path":"missing-cert.pem","key_path":"missing-key.pem"}}),
        json!({"type":"trojan","password":"11111111-2222-3333-4444-555555555555","tls":{"cert_path":"missing-cert.pem","key_path":"missing-key.pem"}}),
        json!({"type":"vmess","users":[{"id":"11111111-2222-3333-4444-555555555555"}],"tls":{"cert_path":"missing-cert.pem","key_path":"missing-key.pem"}}),
        json!({"type":"vless","users":[{"id":"11111111-2222-3333-4444-555555555555"}],"quic":{"cert_path":"missing-cert.pem","key_path":"missing-key.pem"}}),
        json!({"type":"hysteria2","password":"11111111-2222-3333-4444-555555555555","cert_path":"missing-cert.pem","key_path":"missing-key.pem"}),
    ] {
        let config=zero_config::RuntimeConfig::parse(&json!({"runtime":{"file_read_max_bytes":4096},"inbounds":[{"tag":"entry","listen":{"address":"127.0.0.1","port":0},"protocol":protocol}],"route":{"final":{"type":"direct"}}}).to_string()).unwrap();
        let proxy = crate::Proxy::new(config.clone()).unwrap();
        let cold = proxy
            .protocols
            .prepare_inbound_bind_in_config(config.inbounds[0].clone(), &config)
            .unwrap()
            .unwrap();
        assert_eq!(
            cold.resources()
                .iter()
                .filter(|resource| matches!(
                    resource,
                    zero_platform_tokio::contract::component::ResourceDeclaration::File { .. }
                ))
                .count(),
            2
        );
        for resource in cold.resources() {
            if let zero_platform_tokio::contract::component::ResourceDeclaration::File {
                max_bytes,
                ..
            } = resource
            {
                assert_eq!(
                    *max_bytes, 4096,
                    "actual Source declares the runtime File policy"
                );
            }
        }
        assert!(cold.into_recipe().spec().task_limit.is_some());
        assert_eq!(
            proxy
                .protocols
                .runtime_host()
                .original_budgets()
                .carrier
                .snapshot()
                .charged,
            0,
            "cold TLS has acquired no original"
        );
    }
}
#[tokio::test]
async fn real_vless_trojan_vmess_sources_rotate_certificate_and_keep_proxy_wire() {
    timeout(Duration::from_secs(60),async {
        for backend in ["rustls","openssl"] {
            for kind in ["vless","trojan","vmess"] {
                let first=rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();let second=rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
                let directory=tempfile::tempdir().unwrap();let cert=directory.path().join("cert.pem");let key=directory.path().join("key.pem");let roots=directory.path().join("roots.pem");
                std::fs::write(&cert,first.cert.pem()).unwrap();std::fs::write(&key,first.signing_key.serialize_pem()).unwrap();std::fs::write(&roots,first.cert.pem()+&second.cert.pem()).unwrap();
                let mut inbound=json!({"type":kind,"tls":{"cert_path":cert,"key_path":key,"options":{"backend":backend,"reload_interval_secs":1}}});
                if kind=="trojan" {inbound["password"]=json!("11111111-2222-3333-4444-555555555555");}else{inbound["users"]=json!([{"id":"11111111-2222-3333-4444-555555555555"}]);}
                let (remote,running,endpoint)=fixture::start(json!({"inbounds":[{"tag":"entry","listen":{"address":"127.0.0.1","port":0},"protocol":inbound}],"route":{"final":{"type":"direct"}}})).await;
                let actual=owner(&remote,"entry").await;let original_scope=actual.original_scope_for_test();
                let (verifier,_)=fixture::client(vec![first.cert.der().clone(),second.cert.der().clone()]);
                assert_eq!(fixture::peek(endpoint,&verifier,"localhost").await,first.cert.der().as_ref());
                let mut outbound=json!({"type":kind,"server":"127.0.0.1","port":endpoint.port(),"tls":{"server_name":"localhost","ca_cert_path":roots}});
                outbound[if kind=="trojan" {"password"}else{"id"}]=json!("11111111-2222-3333-4444-555555555555");
                let (client,local_running,socks)=fixture::start(json!({"inbounds":[{"tag":"entry","listen":{"address":"127.0.0.1","port":0},"protocol":{"type":"socks5"}}],"outbounds":[{"tag":"tls","protocol":outbound}],"route":{"final":{"type":"route","outbound":"tls"}}})).await;
                fixture::exercise(socks).await;
                std::fs::write(&cert,second.cert.pem()).unwrap();std::fs::write(&key,second.signing_key.serialize_pem()).unwrap();
                timeout(Duration::from_secs(5),async {loop {if fixture::peek(endpoint,&verifier,"localhost").await==second.cert.der().as_ref(){break;}tokio::time::sleep(Duration::from_millis(20)).await;}}).await.unwrap();
                fixture::exercise(socks).await;
                let local_owner=owner(&client,"entry").await;local_running.shutdown().await.unwrap();running.shutdown().await.unwrap();
                assert!(!original_scope.resource_open());closed(&actual);closed(&local_owner);
            }
        }
    }).await.unwrap();
}

#[test]
fn tls_file_limit_changes_rebuild_only_actual_tls_sources_and_preserve_legacy_default() {
    let base = zero_config::RuntimeOptionsConfig::default();
    assert_eq!(base.effective_file_read_max_bytes(), 8 * 1024 * 1024);
    assert_eq!(
        base.effective_tls_material_max_file_bytes(),
        16 * 1024 * 1024
    );
    for protocol in [
        json!({"type":"http"}),
        json!({"type":"vmess","users":[{"id":"11111111-2222-3333-4444-555555555555"}]}),
        json!({"type":"vmess","users":[{"id":"11111111-2222-3333-4444-555555555555"}],"tls":{"cert_path":"cold-cert.pem","key_path":"cold-key.pem"}}),
        json!({"type":"trojan","password":"secret","tls":{"cert_path":"cold-cert.pem","key_path":"cold-key.pem"}}),
    ] {
        let old = zero_config::RuntimeConfig::parse(&json!({"inbounds":[{"tag":"entry","listen":{"address":"127.0.0.1","port":0},"protocol":protocol}],"route":{"final":{"type":"direct"}}}).to_string()).unwrap();
        let mut next = old.clone();
        next.runtime.file_read_max_bytes = Some(8 * 1024 * 1024);
        let expected = protocol.get("tls").is_some();
        assert_eq!(
            crate::adapters::inbound_work_budgets::inbound_work_source_changed(
                &old.inbounds[0],
                &old,
                &next
            ),
            expected
        );
    }
}
