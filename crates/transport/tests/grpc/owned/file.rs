//! Cold authorized File identity stays in the actual immutable pool recipe.
use super::*;
use zero_platform_tokio::contract::component::ResourceDeclaration;
#[test]
fn actual_ca_file_recipe_is_cold_idempotent_bounded_and_distinct_from_the_fixed_library() {
    for backend in [
        zero_traits::TlsBackend::Rustls,
        zero_traits::TlsBackend::OpenSsl,
    ] {
        let pool = GrpcPool::default();
        let settings = NativePoolSettings {
            server: "127.0.0.1".into(),
            port: 443,
            grpc: crate::profile::OwnedGrpcProfile {
                service_names: vec!["owned".into()],
                ..Default::default()
            },
            tls: Some(OwnedClientTlsProfile {
                ca_cert_path: Some("ca.pem".into()),
                options: zero_traits::ClientTlsOptions {
                    backend,
                    ..Default::default()
                },
                server_name: None,
                disable_sni: false,
                insecure: false,
                alpn: vec![],
                client_fingerprint: None,
            }),
            dial: Default::default(),
        };
        let prepare = || {
            NativePoolOptions::for_background_pool(
                &pool,
                settings.clone(),
                NonZeroUsize::new(1).unwrap(),
                NonZeroUsize::new(2).unwrap(),
            )
            .unwrap()
        };
        let first = prepare()
            .with_file_owner(
                Some(std::path::Path::new("/declared")),
                NonZeroUsize::new(4096).unwrap(),
            )
            .unwrap();
        let again = prepare()
            .with_file_owner(
                Some(std::path::Path::new("/declared")),
                NonZeroUsize::new(4096).unwrap(),
            )
            .unwrap();
        // These are two fresh candidate recipes, not the same retained Options.
        // Their File descriptors are identical, while their authorized snapshot
        // stamps force a new candidate to read changed CA material.
        assert_ne!(first.identity, again.identity);
        assert_ne!(first.file_stamp, again.file_stamp);
        assert_eq!(
            NativePoolFactory.describe(&first).resources,
            NativePoolFactory.describe(&again).resources
        );
        assert!(
            !Arc::ptr_eq(first.ca.as_ref().unwrap(), again.ca.as_ref().unwrap()),
            "a newly prepared candidate must read its own actual File material"
        );
        let idempotent = first
            .clone()
            .with_file_owner(
                Some(std::path::Path::new("/declared")),
                NonZeroUsize::new(4096).unwrap(),
            )
            .unwrap();
        assert_eq!(first.identity, idempotent.identity);
        assert!(
            Arc::ptr_eq(first.ca.as_ref().unwrap(), idempotent.ca.as_ref().unwrap()),
            "only the exact retained Options clone preserves its recovery snapshot"
        );
        let next = prepare()
            .with_file_owner(
                Some(std::path::Path::new("/declared")),
                NonZeroUsize::new(8192).unwrap(),
            )
            .unwrap();
        assert_ne!(first.identity, next.identity);
        assert!(!Arc::ptr_eq(
            first.ca.as_ref().unwrap(),
            next.ca.as_ref().unwrap()
        ));
        let spec = NativePoolFactory.describe(&first);
        assert!(
            matches!(&spec.resources[..],[ResourceDeclaration::File{path,max_bytes:4096},ResourceDeclaration::ControlledCarrierPool{..}] if path==std::path::Path::new("/declared/ca.pem"))
        );
        assert!(NativePoolOptions::from_pool(
            &pool,
            settings.clone(),
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(2).unwrap()
        )
        .is_err());
        assert!(prepare()
            .with_file_owner(None, NonZeroUsize::new(usize::MAX).unwrap())
            .is_err());
        assert!(matches!(
            *pool.0.ownership.lock().unwrap(),
            super::super::state::Ownership::Pristine
        ));
        assert!(pool.0.cached.lock().unwrap().is_none());
    }
}
