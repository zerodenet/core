use super::*;
#[tokio::test]
async fn mask_started_before_endpoint_registration_refusal_keeps_original_owner_until_real_join() {
    timeout(Duration::from_secs(8), async {
        let mut scope = TaskScope::new(tokio::runtime::Handle::current(), n(1));
        let tasks = scope.registrar();
        let limits = zero_platform_tokio::controlled_datagram::metadata::MetadataLimits {
            main_sockets: n(1),
            probe_sockets: n(2),
            probe_attempts_per_initialization: n(8),
            concurrent_send_calls: n(2),
            write_pollers: n(8),
        };
        let native = crate::component::instance::HostResources::new_for_instance(
            ComponentInstanceId {
                component: ComponentId(948),
                generation: ResourceGeneration(1),
            },
            &[ResourceDeclaration::MetadataDatagram {
                source: "127.0.0.1:0".parse().unwrap(),
                limits,
                max_bind_attempts: n(1),
            }],
            tasks.cancellation(),
            tasks.scope_identity(),
        )
        .unwrap();
        let certificate = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let server = quinn::ServerConfig::with_single_cert(
            vec![certificate.cert.der().clone()],
            rustls::pki_types::PrivatePkcs8KeyDer::from(certificate.signing_key.serialize_der())
                .into(),
        )
        .unwrap();
        let mask = Mask::Noise(zero_transport::finalmask::noise::Settings::default());
        let (mut lifecycle, source) = zero_transport::quic::source::source(
            MetadataDatagramRequest {
                ordinal: 0,
                source: "127.0.0.1:0".parse().unwrap(),
                limits,
            },
            server,
            vec![mask],
            zero_transport::quic::owned::OwnedEndpointLimits {
                drivers: n(2),
                users: n(1),
            },
            native.clone(),
            tasks.clone(),
            tasks.cancellation(),
            |_| -> PendingInbound { panic!("failed original start cannot deliver a request") },
        );
        let access = native.activate(deadline()).unwrap();
        assert!(
            lifecycle.start(deadline()).await.is_err(),
            "actual Scope admits mask but refuses endpoint; no fallback"
        );
        drop(access);
        drop(source);
        scope.begin_shutdown();
        let (closed, count) = tokio::join!(lifecycle.shutdown(deadline()), async {
            let mut count = 0;
            while let Some(exit) = scope.next_exit().await {
                assert!(exit.result.is_ok());
                count += 1;
            }
            count
        });
        assert!(closed.unwrap().is_complete());
        assert_eq!(
            count, 1,
            "original registered mask driver joined exactly once"
        );
        let report = native.shutdown_io(deadline()).await.unwrap();
        assert!(report.is_complete(), "{report:?}");
        assert!(
            report
                .closed_io
                .iter()
                .any(|line| line.contains("metadata declaration 0 main")),
            "{report:?}"
        );
    })
    .await
    .unwrap();
}
