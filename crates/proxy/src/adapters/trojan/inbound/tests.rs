use crate::adapters::trojan::{inbound_user_refs, TrojanAdapter};
use zero_config::{InboundProtocolConfig, RuntimeConfig};

#[tokio::test]
async fn cold_prepare_drop_and_transport_failure_leave_live_users_until_install() {
    let directory = tempfile::tempdir().unwrap();
    let certificate = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let cert = directory.path().join("cert.pem");
    let key = directory.path().join("key.pem");
    std::fs::write(&cert, certificate.cert.pem()).unwrap();
    std::fs::write(&key, certificate.signing_key.serialize_pem()).unwrap();
    let config = RuntimeConfig::parse(
        &serde_json::json!({
            "inbounds":[{"tag":"trojan-in","listen":{"address":"127.0.0.1","port":18443},
                "protocol":{"type":"trojan","users":[{"password":"new"},{"password":"second"}],
                    "tls":{"cert_path":cert,"key_path":key}}}],
            "route":{"final":{"type":"direct"}}
        })
        .to_string(),
    )
    .unwrap();
    let adapter = TrojanAdapter::default();
    let live = adapter
        .inbound_profiles
        .replace("trojan-in", &inbound_user_refs("old", &[]));
    let candidate = config.inbounds[0].clone();
    let discarded = adapter.inbound_options(&candidate, None).unwrap();
    assert_eq!(
        live.user_count(),
        1,
        "cold preparation must not publish users"
    );
    drop(discarded);
    assert_eq!(live.user_count(), 1);
    let mut invalid = candidate.clone();
    let InboundProtocolConfig::Trojan { tls, .. } = &mut invalid.protocol else {
        panic!("Trojan");
    };
    *tls = None;
    assert!(adapter.inbound_options(&invalid, None).is_err());
    assert_eq!(
        live.user_count(),
        1,
        "transport validation cannot publish users"
    );

    let options = adapter.inbound_options(&candidate, None).unwrap();
    assert_eq!(live.user_count(), 1);
    use zero_platform_tokio::contract::{
        component::{BuildContext, ComponentFactory},
        scope::TaskScope,
    };
    let scope = TaskScope::empty(tokio::runtime::Handle::current());
    let instance = ::trojan::component::inbound::listener::Factory
        .build(
            std::sync::Arc::new(options),
            BuildContext {
                component: zero_core::component::ComponentId(1),
                generation: zero_core::component::ResourceGeneration(1),
                roles: zero_core::component::ComponentRoleIds {
                    inbound: Some(zero_core::component::InboundId(1)),
                    ..Default::default()
                },
                dependencies: std::sync::Arc::from([]),
                resources: std::sync::Arc::new(NoIo),
                tasks: scope.registrar(),
                cancel: zero_platform_tokio::contract::CancellationToken::new(),
                background_carrier: None,
            },
        )
        .unwrap();
    assert_eq!(
        live.user_count(),
        2,
        "host build publishes to the same original profile clone"
    );
    drop(instance); // Pure source build started no native I/O or tasks.
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
