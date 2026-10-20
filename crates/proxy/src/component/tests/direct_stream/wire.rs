use super::*;
#[tokio::test]
async fn actual_direct_role_recovers_business_dns_under_same_host_scope_and_closes_all_original_candidates(
) {
    timeout(Duration::from_secs(12), async {
        for kind in crate::component::tests::business_carrier::sources::Kind::all() {
            let (proxy, dns, stop_dns) = dns::proxy().await;
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let peer = listener.local_addr().unwrap();
            let policy = zero_traits::DialPolicy {
                source_ip: Some("127.0.0.1".parse().unwrap()),
                ..Default::default()
            };
            let options = DirectOptions::validate(policy.clone())
                .unwrap()
                .controlled(NonZeroUsize::new(4).unwrap(), one(), Duration::from_secs(1))
                .unwrap();
            let captured = Arc::new(Captured::default());
            let factory = Arc::new(Factory(captured.clone()));
            let description = factory.describe(&options);
            assert_eq!(description.task_limit, Some(one()));
            assert_eq!(
                description.resources,
                vec![ResourceDeclaration::ControlledDirectStream {
                    dial: policy.clone(),
                    max_sockets: NonZeroUsize::new(4).unwrap()
                }]
            );
            let slot = Arc::new(ComponentSlot::from_recipe(
                ComponentId(2821),
                ResourceGeneration(1),
                ComponentRecipe::from_validated(factory, Arc::new(options)),
                Default::default(),
                "controlled-direct",
            ));
            let host = TestContext::new([slot.as_ref()]);
            let role = host.drive(slot.stream(deadline())).await.unwrap();
            slot.host_admission()
                .unwrap()
                .publish(std::iter::once(slot.admission_grant()));
            let actual = captured.0.lock().unwrap().clone().unwrap();
            assert_eq!(actual.observe().unwrap().retained(), 0);
            let mut callers = Vec::new();
            for _ in 0..2 {
            let mut session = Session::new(
                0,
                Address::Domain("direct.fixture.invalid".into()),
                peer.port(),
                Network::Tcp,
                ProtocolType::new("verified"),
            );
            session.original_target = Some(Address::Ipv4([127, 0, 0, 2]));
            session.direct_target = session.original_target.clone();
            session.target_host_source = Some(TargetHostSource::TlsSni);
            session.sni = Some("direct.fixture.invalid".into());
            let mut caller =
                crate::component::tests::business_carrier::sources::Caller::new_for_session(
                    &proxy, kind, session,
                )
                .await;
            let session = caller.session().clone();
            let invocation = caller.invocation().clone();
            assert!(invocation.deadline.is_none());
            let binding = UpstreamBinding {
                prefix: vec![],
                dial: policy.clone(),
                network_generation: NetworkGeneration(
                    proxy
                        .tcp_runtime_services()
                        .execution()
                        .upstream()
                        .network_generation(),
                ),
            };
            let destination = Destination::new(session.target.clone(), session.port);
            let facts = Arc::new(Facts::default());
            // A real role and public Session projection alone do not authorize
            // DNS/native execution. Missing sealed demand remains pre-I/O reject.
            let denied = Arc::new(DirectOpener::new(
                proxy.tcp_runtime_services().execution().upstream(),
                session.clone(),
            ));
            let error = host
                .drive(role.connect(
                    ConnectRequest {
                        destination: destination.clone(),
                    },
                    OpenContext {
                        identity: invocation.identity.clone(),
                        deadline: None,
                        cancel: invocation.cancellation(),
                        observer: None,
                        upstream: UpstreamTransport::new(binding.clone(), denied),
                    },
                ))
                .await
                .err()
                .unwrap();
            assert_eq!(
                error.reason,
                zero_platform_tokio::contract::Reason::Unsupported
            );
            assert_eq!(actual.observe().unwrap().retained(), 0);
            let demand = BusinessCarrierRequest::new(proxy.engine(), &slot, &invocation).unwrap();
            let opener = Arc::new(
                DirectOpener::new(
                    proxy.tcp_runtime_services().execution().upstream().with_observer(Some(facts.clone())),
                    session.clone(),
                )
                .with_business(demand),
            );
            let mut connected = host
                .drive(role.connect(
                    ConnectRequest { destination },
                    OpenContext {
                        identity: invocation.identity.clone(),
                        deadline: None,
                        cancel: invocation.cancellation(),
                        observer: Some(facts.clone()),
                        upstream: UpstreamTransport::new(binding, opener.clone()),
                    },
                ))
                .await
                .unwrap();
            assert_eq!(connected.peer_confirmation, PeerConfirmation::Confirmed);
            let evidence = opener
                .take_evidence()
                .unwrap()
                .unwrap_or_else(|_| panic!("actual Direct network evidence"));
            assert_eq!(evidence.remote, peer);
            assert!(evidence.local.is_some());
            assert_eq!(evidence.network.connection_attempts.len(), 2);
            let first = &evidence.network.connection_attempts[0];
            assert_eq!(first.remote_address.host, "127.0.0.2");
            assert_eq!(first.outcome, "failed");
            assert!(
                first.os_error.is_some(),
                "the actual refused OS error survives typed native wrapping"
            );
            assert_eq!(
                evidence.network.connection_attempts[1].remote_address.host,
                "127.0.0.1"
            );
            assert_eq!(evidence.network.connection_attempts[1].outcome, "connected");
            let (mut remote, local) = listener.accept().await.unwrap();
            assert_eq!(Some(local), evidence.local);
            let server = tokio::spawn(async move {
                let mut request = Vec::new();
                remote.read_to_end(&mut request).await.unwrap();
                assert_eq!(request, b"direct-request");
                remote
                    .write_all(b"response-after-half-close")
                    .await
                    .unwrap();
                remote.shutdown().await.unwrap();
            });
            host.drive(async {
                connected.io.write_all(b"direct-request").await.unwrap();
                connected.io.shutdown().await.unwrap();
                let mut reply = Vec::new();
                connected.io.read_to_end(&mut reply).await.unwrap();
                assert_eq!(reply, b"response-after-half-close");
            })
            .await;
            assert_eq!(
                &*facts.facts.lock().unwrap(),
                &[
                    (Some(SessionId(session.id)), ConnectFact::CarrierReady),
                    (Some(SessionId(session.id)), ConnectFact::PeerConfirmed)
                ]
            );
            assert_eq!(facts.outer.load(Ordering::Relaxed), 0, "Direct keeps original Flow relay accounting and adds no outer carrier/handshake counters");
            let auth = session.auth.as_ref().unwrap();
            assert_eq!((auth.up_bps, auth.down_bps, auth.device_limit, auth.quota_remaining_bytes, auth.policy_revision), (Some(4096), Some(8192), Some(1), Some(4096), Some(7)));
            let pending = actual.observe().unwrap();
            assert_eq!(
                pending.retained(),
                2,
                "failed candidate and selected actual original are separately charged"
            );
            assert!(
                pending.slots().iter().all(|slot| slot.driver().is_none()),
                "Direct does not invent a TCP transport driver"
            );
            let refresh = pending
                .slots()
                .iter()
                .filter_map(|slot| slot.auxiliary())
                .collect::<Vec<_>>();
            assert_eq!(refresh.len(), 1);
            let joined = refresh[0].try_joined().unwrap();
            assert_eq!(
                joined.class(),
                zero_platform_tokio::contract::lifecycle::TaskClass::Auxiliary
            );
            assert!(joined.result().is_ok());
            drop(connected);
            caller.join().await;
            assert!(actual.closed().is_none());
            proxy
                .engine()
                .track_session(session.id)
                .finish(zero_engine::SessionOutcome::DirectRelayed)
                .unwrap();
            server.await.unwrap();
            callers.push(caller);
            }
            stop_dns.send(()).unwrap();
            let queries = dns.await.unwrap();
            assert!((1..=2).contains(&queries), "real DNS cache policy may reuse its actual material between requests");
            let closed = host.drive(slot.shutdown(deadline())).await.unwrap();
            assert!(
                closed.is_complete(),
                "actual Direct same caller/view/task/native graph: {closed:?}"
            );
            assert!(actual.closed().unwrap().matches(&actual));
            assert!(actual.users_closed().unwrap().matches(&actual));
            assert_eq!(closed.closed_io.len(), 4, "both demands' failed and selected originals close exactly once under the same actual generation");
            assert_eq!(callers.len(), 2);
            for caller in &callers { caller.verify_carrier_completion(); }
        }
    })
    .await
    .unwrap();
}
