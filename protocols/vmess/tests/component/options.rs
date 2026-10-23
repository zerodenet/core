use super::*;
use zero_platform_tokio::contract::component::ComponentFactory;
fn input(
    tls: &OwnedClientTlsProfile,
) -> VmessOutboundBuildOptionsRef<'_, OwnedClientTlsProfile, OwnedWebSocketProfile, OwnedGrpcProfile>
{
    VmessOutboundBuildOptionsRef {
        tag: "explicit-ca",
        server: "localhost",
        port: 443,
        protocol: crate::transport::VmessOutboundOptionsRef {
            id: "11111111-2222-3333-4444-555555555555",
            cipher: "aes-128-gcm",
            mux_concurrency: None,
            mux_idle_timeout_secs: None,
            mux_response_backlog_frames: None,
            mux_response_backlog_bytes: None,
        },
        tls: Some(tls),
        ws: None,
        grpc: None,
    }
}
fn profile(path: &str) -> OwnedClientTlsProfile {
    OwnedClientTlsProfile {
        options: Default::default(),
        server_name: Some("localhost".into()),
        disable_sni: false,
        ca_cert_path: Some(path.into()),
        insecure: false,
        alpn: vec![],
        client_fingerprint: None,
    }
}
#[test]
fn explicit_ca_bound_and_lexical_directory_are_part_of_the_cold_resource_contract() {
    let tls = profile("../authority.pem");
    assert!(Options::from_options_refs(input(&tls), Default::default()).is_err());
    let bound = NonZeroUsize::new(1234);
    let options = Options::from_options_refs_at(
        input(&tls),
        Default::default(),
        Some(Path::new("/chosen/config")),
        bound,
    )
    .unwrap();
    let spec = crate::component::Factory.describe(&options);
    assert_eq!(
        spec.resources,
        vec![
            zero_platform_tokio::contract::component::ResourceDeclaration::File {
                path: PathBuf::from("/chosen/config/../authority.pem"),
                max_bytes: 1234
            }
        ]
    );
    let different = Options::from_options_refs_at(
        input(&tls),
        Default::default(),
        Some(Path::new("/chosen/config")),
        NonZeroUsize::new(1235),
    )
    .unwrap();
    assert_ne!(options.identity, different.identity);
    let default_base =
        Options::from_options_refs_at(input(&tls), Default::default(), None, bound).unwrap();
    assert_eq!(
        default_base.ca_file.unwrap().path,
        Path::new("../authority.pem")
    );
    let absolute = profile("/external/authority.pem");
    let options = Options::from_options_refs_at(
        input(&absolute),
        Default::default(),
        Some(Path::new("/chosen/config")),
        bound,
    )
    .unwrap();
    assert_eq!(
        options.ca_file.unwrap().path,
        Path::new("/external/authority.pem")
    );
}
#[test]
fn incompatible_backend_and_invalid_file_bounds_fail_before_any_resource_acquisition() {
    let mut tls = profile("not-opened.pem");
    assert!(Options::from_options_refs_at(
        input(&tls),
        Default::default(),
        None,
        NonZeroUsize::new(usize::MAX)
    )
    .is_err());
    tls.options.backend = zero_traits::TlsBackend::OpenSsl;
    assert!(matches!(
        Options::from_options_refs_at(
            input(&tls),
            Default::default(),
            None,
            NonZeroUsize::new(1024)
        ),
        Err(Error::Unsupported(_))
    ));
}
