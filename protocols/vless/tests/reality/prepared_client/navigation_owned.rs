//! Verified TLS decoy uses the actual rejected original and its real Aux/ACK.
use super::*;
use std::task::Poll;
use tokio::time::{timeout, Instant};
use zero_core::{
    component::{ComponentId, ComponentInstanceId, NetworkGeneration, ResourceGeneration},
    request::Destination,
    Address,
};
use zero_platform_tokio::{
    contract::{
        component::ResourceDeclaration,
        network::{CarrierContext, UpstreamBinding, UpstreamTransport},
        ChannelError, Impact, Reason, Stage,
    },
    controlled_carrier::{CarrierOriginalTotals, CarrierOwner, NativeCarrierOpener},
    EgressInterfaceControl,
};

#[tokio::test]
async fn verified_decoy_stays_on_the_original_until_same_scope_job_and_native_ack() {
    timeout(Duration::from_secs(5), async {
        let cert = rcgen::generate_simple_self_signed(vec!["decoy.test".into()]).unwrap();
        let trust =
            ztls::certificate::ServerTrust::certificates(&[cert.cert.der().to_vec()]).unwrap();
        let tls = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(
            vec![cert.cert.der().clone()],
            rustls::pki_types::PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der()).into(),
        )
        .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = listener.local_addr().unwrap();
        let destination = Destination::new(Address::Ipv4([127, 0, 0, 1]), endpoint.port());
        let instance = ComponentInstanceId {
            component: ComponentId(919),
            generation: ResourceGeneration(3),
        };
        let egress = Arc::new(EgressInterfaceControl::default());
        let binding = UpstreamBinding {
            prefix: vec![],
            dial: Default::default(),
            network_generation: NetworkGeneration(egress.generation()),
        };
        let transport = UpstreamTransport::new(
            binding.clone(),
            Arc::new(
                NativeCarrierOpener::new(instance, binding.clone(), endpoint, egress, None)
                    .unwrap(),
            ),
        );
        let totals = CarrierOriginalTotals::new(NonZeroUsize::new(1).unwrap());
        totals
            .install_preparation_limits_after_retirement(
                NonZeroUsize::new(1).unwrap(),
                NonZeroUsize::new(1).unwrap(),
            )
            .unwrap();
        let mut scope =
            TaskScope::suspended_hosted(tokio::runtime::Handle::current(), NonZeroUsize::new(1));
        scope.enable_registration().unwrap();
        let tasks = scope.registrar();
        let host = Arc::new(CancellationToken::new());
        let mut owner = CarrierOwner::new(instance, NonZeroUsize::new(1).unwrap(), &host);
        let registration = owner.registration();
        registration
            .bind_declared_with_totals(
                &ResourceDeclaration::ControlledNodeCarrier {
                    endpoint: destination.clone(),
                    dial: binding.dial,
                    max_sockets: NonZeroUsize::new(1).unwrap(),
                },
                &tasks,
                &host,
                &totals,
            )
            .unwrap();
        let end = Instant::now() + Duration::from_secs(4);
        owner.start(end).await.unwrap();
        let grants = owner.grants();
        let mut grant = grants.reserve().unwrap();
        let original = grant.adoption();
        let (view, preparation) = grant.claim_preparing_transferred_view().unwrap();
        let call = CarrierContext {
            session: None,
            deadline: Some(end),
            cancel: CancellationToken::new(),
        };
        let first_caller = call.cancel.clone();
        let prepared = PreparedRealityClient {
            config: RealityClientConfig {
                public_key: *PublicKey::from(&StaticSecret::from([21; 32])).as_bytes(),
                decoy_trust: Some(trust),
                server_name: "decoy.test".into(),
                hybrid_key_exchange: false,
                ..Default::default()
            },
            spider: crate::reality_spider::Profile::parse("/").unwrap(),
        };
        let (setup_ready, setup) = tokio::sync::oneshot::channel();
        let (_, auxiliary) = original
            .spawn_auxiliary(&tasks, async move {
                let carrier = transport
                    .prepare_controlled_stream(destination, call, grant)?
                    .open(end)
                    .await?;
                let (io, _) = carrier.into_parts();
                let (io, input) = io.into_driver().split(|io| (io, view));
                let result = prepared
                    .connect_with_navigation(io, |stream, host, plan| async move {
                        let job = preparation.reserve_job().unwrap();
                        crate::component::navigation::stage(stream, host, plan, job);
                    })
                    .await;
                let rejection = result.err().expect("a verified decoy never admits a proxy");
                assert_eq!(rejection.kind(), io::ErrorKind::PermissionDenied);
                let rejection = Arc::new(
                    ChannelError::new(Stage::Handshake, Reason::Io, Impact::Component)
                        .with_source(rejection),
                );
                assert!(setup_ready.send((input, rejection.clone())).is_ok());
                Err(ChannelError::retained(rejection))
            })
            .unwrap();
        let (peer, _) = listener.accept().await.unwrap();
        let (seen_preface, preface) = tokio::sync::oneshot::channel();
        let peer = tokio::spawn(async move {
            let mut io = tokio_rustls::TlsAcceptor::from(Arc::new(tls))
                .accept(peer)
                .await
                .unwrap();
            let mut header = [0; 24];
            io.read_exact(&mut header).await.unwrap();
            assert_eq!(&header, b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n");
            seen_preface.send(()).unwrap();
            // Withhold SETTINGS: the original job is actually pending in H2.
            let mut tail = Vec::new();
            if let Err(cause) = io.get_mut().0.read_to_end(&mut tail).await {
                assert_eq!(cause.kind(), io::ErrorKind::ConnectionReset);
            }
        });
        let (input, rejection) = setup.await.unwrap();
        let receipt = scope.next_receipt().await.unwrap();
        assert!(auxiliary.matches_joined(receipt.joined()));
        let received = original.accept_auxiliary().await.unwrap().unwrap();
        assert!(received.result().is_err() && received.failure_accepted());
        assert!(original.take_preparation_job(&tasks, &received).is_err());
        assert!(original.retained_driver().is_none());
        assert!(original.retained_preparation_job().is_none());
        assert_eq!(grants.reserve().err().unwrap().reason, Reason::Capacity);
        scope.acknowledge(&receipt).unwrap();
        auxiliary.wait_registration_released().await;
        let staged = original
            .take_preparation_job(&tasks, &received)
            .unwrap()
            .unwrap();
        staged.grant.accept_rejection(rejection, &received).unwrap();
        let work = staged.work;
        let (_, driver) = original
            .spawn_preparation_job(&tasks, &staged.grant, input, |view| async move {
                let _view = view;
                work.await
            })
            .unwrap();
        preface.await.unwrap();
        first_caller.cancel();
        assert!(!driver.is_joined());
        assert_eq!(totals.preparation_snapshot().jobs, 1);
        let retained = original.retained_preparation_job().unwrap();
        assert!(retained.retains(&original));
        assert!(original.retained_driver().is_none());
        host.cancel();
        let receipt = scope.next_receipt().await.unwrap();
        assert!(driver.matches_joined(receipt.joined()));
        assert!(
            receipt.result().is_ok(),
            "actual original job receipt: {:?}",
            receipt.result()
        );
        std::future::poll_fn(|cx| {
            totals.poll_cancelled_retirements(cx);
            if original
                .closed()
                .is_some_and(|closed| closed.matches(&original))
                && original
                    .users_closed()
                    .is_some_and(|closed| closed.matches(&original))
            {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await;
        peer.await.unwrap();
        assert_eq!(totals.preparation_snapshot().jobs, 1);
        scope.acknowledge(&receipt).unwrap();
        driver.wait_registration_released().await;
        std::future::poll_fn(|cx| {
            totals.poll_cancelled_retirements(cx);
            if totals.snapshot().charged == 0 {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await;
        assert_eq!(totals.preparation_snapshot().jobs, 0);
        assert!(owner.shutdown(end).await.unwrap().is_complete());
        assert!(registration.closed().unwrap().matches(&registration));
        scope.begin_shutdown();
        assert!(scope.is_drained());
    })
    .await
    .unwrap();
}
