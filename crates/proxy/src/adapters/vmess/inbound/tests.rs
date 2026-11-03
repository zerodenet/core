//! Standard cold material never publishes credentials until the real host Build.
use crate::adapters::vmess::{inbound_user_refs, VmessAdapter};
use zero_config::InboundProtocolConfig;
use zero_platform_tokio::contract::{component::*, scope::TaskScope, CancellationToken};
fn candidate() -> (tempfile::TempDir, zero_config::RuntimeConfig) {
    let directory = tempfile::tempdir().unwrap();
    let certified = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let cert = directory.path().join("cert.pem");
    let key = directory.path().join("key.pem");
    std::fs::write(&cert, certified.cert.pem()).unwrap();
    std::fs::write(&key, certified.signing_key.serialize_pem()).unwrap();
    let config = zero_config::RuntimeConfig::parse(
        &serde_json::json!({
            "inbounds":[{"tag":"vmess-staged", "listen":{"address":"127.0.0.1","port":0},
                "protocol":{"type":"vmess","users":[
                    {"id":"51515151-5151-5151-5151-515151515151","cipher":"aes-128-gcm"},
                    {"id":"52525252-5252-5252-5252-525252525252","cipher":"aes-128-gcm"}
                ],"tls":{"cert_path":cert,"key_path":key}}}],
            "route":{"final":{"type":"direct"}}
        })
        .to_string(),
    )
    .unwrap();
    (directory, config)
}
#[tokio::test]
async fn candidate_prepare_drop_and_tls_failure_never_publish_live_users_but_build_does() {
    let (directory, config) = candidate();
    let adapter = VmessAdapter::default();
    let InboundProtocolConfig::Vmess { users, .. } = &config.inbounds[0].protocol else {
        unreachable!()
    };
    let old = adapter
        .runtime
        .replace_inbound_profile("vmess-staged", &inbound_user_refs(&users[..1]))
        .unwrap();
    let discarded = adapter.inbound_options(&config.inbounds[0], None).unwrap();
    assert_eq!(old.user_count(), 1);
    drop(discarded);
    assert_eq!(old.user_count(), 1);
    let mut invalid = config.inbounds[0].clone();
    let InboundProtocolConfig::Vmess { tls, .. } = &mut invalid.protocol else {
        unreachable!()
    };
    tls.as_mut().unwrap().key_path = "does-not-exist.key".into();
    assert!(adapter.inbound_options(&invalid, None).is_err());
    assert_eq!(old.user_count(), 1);
    let options = std::sync::Arc::new(adapter.inbound_options(&config.inbounds[0], None).unwrap());
    assert_eq!(old.user_count(), 1);
    std::fs::remove_file(directory.path().join("cert.pem")).unwrap();
    std::fs::remove_file(directory.path().join("key.pem")).unwrap();
    let scope = TaskScope::empty(tokio::runtime::Handle::current());
    let context = || BuildContext {
        component: zero_core::component::ComponentId(1),
        generation: zero_core::component::ResourceGeneration(1),
        roles: zero_core::component::ComponentRoleIds {
            inbound: Some(zero_core::component::InboundId(1)),
            ..Default::default()
        },
        dependencies: std::sync::Arc::from([]),
        resources: std::sync::Arc::new(NoIo),
        tasks: scope.registrar(),
        cancel: CancellationToken::new(),
        background_carrier: None,
    };
    let instance = ::vmess::component::inbound::listener::Factory
        .build(options.clone(), context())
        .unwrap();
    assert_eq!(
        old.user_count(),
        2,
        "publish into the same original profile"
    );
    adapter
        .runtime
        .replace_inbound_profile("vmess-staged", &[])
        .unwrap();
    assert!(::vmess::component::inbound::listener::Factory
        .build(options, context())
        .is_err());
    assert_eq!(
        old.user_count(),
        0,
        "consumed cold material cannot overwrite a later hot update"
    );
    drop(instance); // Cold Build starts no sockets or tasks.
}
#[test]
fn post_reconcile_hot_callback_still_publishes_to_the_existing_actual_profile() {
    let (_directory, mut config) = candidate();
    let adapter = VmessAdapter::default();
    let InboundProtocolConfig::Vmess { users, .. } = &config.inbounds[0].protocol else {
        unreachable!()
    };
    let profile = adapter
        .runtime
        .replace_inbound_profile("vmess-staged", &inbound_user_refs(&users[..1]))
        .unwrap();
    crate::protocol_registry::ProtocolSupportCapability::on_config_reloaded(&adapter, &config);
    assert_eq!(profile.user_count(), 2);
    let InboundProtocolConfig::Vmess { users, .. } = &mut config.inbounds[0].protocol else {
        unreachable!()
    };
    users.clear();
    crate::protocol_registry::ProtocolSupportCapability::on_config_reloaded(&adapter, &config);
    assert_eq!(profile.user_count(), 0);
}

struct NoIo;
impl zero_platform_tokio::contract::component::ResourceAccess for NoIo {
    fn read_file(
        &self,
        _: std::path::PathBuf,
        _: usize,
    ) -> zero_platform_tokio::contract::BoxFuture<
        '_,
        Result<Vec<u8>, zero_platform_tokio::contract::ChannelError>,
    > {
        panic!("build grants no file I/O")
    }
    fn listen_tcp(
        &self,
        _: zero_core::request::Destination,
    ) -> zero_platform_tokio::contract::BoxFuture<
        '_,
        Result<
            zero_platform_tokio::ControlledTcpListener,
            zero_platform_tokio::contract::ChannelError,
        >,
    > {
        panic!("build grants no native TCP")
    }
    fn listen_datagram(
        &self,
        _: zero_core::request::Destination,
    ) -> zero_platform_tokio::contract::BoxFuture<
        '_,
        Result<
            zero_platform_tokio::contract::component::GrantedDatagramListener,
            zero_platform_tokio::contract::ChannelError,
        >,
    > {
        panic!("build grants no native UDP")
    }
    fn open_device(
        &self,
        _: std::sync::Arc<str>,
    ) -> zero_platform_tokio::contract::BoxFuture<
        '_,
        Result<
            zero_platform_tokio::contract::packet::BoxPacketSession,
            zero_platform_tokio::contract::ChannelError,
        >,
    > {
        panic!("build grants no native device")
    }
}
