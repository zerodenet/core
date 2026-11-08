use super::*;

// This injects actual OS error returns at the persistent source-cell boundary,
// not a Windows closesocket failure. Native receipts below remain real UDP.
#[tokio::test]
async fn same_root_phase_then_two_original_sources_survive_positive_native_and_deferred_ack_without_cycle(
) {
    run(false).await;
}
#[tokio::test]
async fn native_first_same_root_fault_retains_late_original_phase_ledger_after_reception() {
    run(true).await;
}
async fn run(native_first: bool) {
    tokio::time::timeout(Duration::from_secs(3), async {
        let one = NonZeroUsize::new(1).unwrap();
        let root = CarrierOriginalTotals::new(one);
        let mut scope = TaskScope::suspended_hosted(tokio::runtime::Handle::current(), Some(one));
        scope.enable_registration().unwrap();
        let tasks = scope.registrar();
        let host = Arc::new(CancellationToken::new());
        let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let remote = peer.local_addr().unwrap();
        let owner = MetadataPoolOwner::new(instance(), one, limits(), &host).unwrap();
        owner
            .registration()
            .bind_declared_with_totals(
                &ResourceDeclaration::ControlledMetadataCarrier {
                    endpoint: zero_core::request::Destination::new(
                        zero_core::Address::Ipv4([127, 0, 0, 1]),
                        remote.port(),
                    ),
                    ports: vec![],
                    dial: Default::default(),
                    limits: limits(),
                    max_origins: one,
                },
                &tasks,
                &host,
                &root,
            )
            .unwrap();
        owner.start().unwrap();
        let preparation = owner.grants().prepare_original(&tasks).unwrap();
        let (begin, started) = tokio::sync::oneshot::channel::<(Arc<Retained>, TaskJoinWitness)>();
        let (permit, actor) = preparation
            .spawn_auxiliary(&tasks, async move {
                let (original, actor) = started.await.unwrap();
                let egress = Arc::new(crate::EgressInterfaceControl::default());
                let binding = UpstreamBinding {
                    prefix: vec![],
                    dial: Default::default(),
                    network_generation: zero_core::component::NetworkGeneration(
                        egress.generation(),
                    ),
                };
                let opener = crate::controlled_carrier::NativeCarrierOpener::new(
                    instance(),
                    binding.clone(),
                    remote,
                    egress,
                    None,
                )
                .unwrap();
                let endpoint = zero_core::request::Destination::new(
                    zero_core::Address::Ipv4([127, 0, 0, 1]),
                    remote.port(),
                );
                let candidates = opener
                    .resolve_controlled_carrier(
                        &binding,
                        endpoint,
                        CarrierContext {
                            session: None,
                            deadline: None,
                            cancel: CancellationToken::new(),
                        },
                    )
                    .await?;
                let (_, grant) = candidates.open_metadata(
                    0,
                    None,
                    &original.permit,
                    &actor,
                    tokio::time::Instant::now() + Duration::from_secs(3),
                )?;
                *original.wire.lock().unwrap() = Some(grant);
                let (sender, mut writable) = {
                    let wire = original.wire.lock().unwrap();
                    let wire = wire.as_ref().unwrap();
                    (wire.tx.clone(), wire.tx.create_write_poller().unwrap())
                };
                let message = MetadataTransmit {
                    destination: remote,
                    contents: b"original before cleanup",
                    source_ip: None,
                    ecn: None,
                    segment_size: None,
                };
                std::future::poll_fn(|cx| loop {
                    std::task::ready!(writable.poll_writable(cx)).unwrap();
                    match sender.try_send(&message) {
                        Err(error) if error.kind() == io::ErrorKind::WouldBlock => continue,
                        result => return Poll::Ready(result.unwrap()),
                    }
                })
                .await;
                Ok(())
            })
            .unwrap();
        let original = Arc::new(Retained {
            permit,
            tasks: tasks.clone(),
            wire: Mutex::default(),
            released: AtomicBool::new(false),
            changed: tokio::sync::Notify::new(),
        });
        let origin = original.permit.origin();
        let native_sources = origin.original_sources().unwrap();
        let directory = tempfile::tempdir().unwrap();
        let phase_error = ChannelError::new(Stage::Close, Reason::Io, Impact::Component)
            .with_source(os_source(&directory.path().join("phase")));
        let retirement = original
            .permit
            .install_protocol_control(
                &tasks,
                &actor,
                BoxChannelControl::from(Box::new(Control {
                    original: original.clone(),
                    fault: Some(phase_error),
                })),
            )
            .unwrap();
        assert!(begin.send((original.clone(), actor.clone())).is_ok());
        let mut bytes = [0; 64];
        let (read, _) = peer.recv_from(&mut bytes).await.unwrap();
        assert_eq!(&bytes[..read], b"original before cleanup");
        let joined = scope.next_receipt().await.unwrap();
        assert!(actor.matches_joined(joined.joined()) && joined.result().is_ok());
        assert!(!joined.joined().registration_released());
        let native_receiver = if native_first {
            let main = native_sources
                .originals()
                .find(|row| row.namespace == MetadataOriginalNamespace::Main)
                .unwrap()
                .reservation;
            assert_eq!(main.get(), 0);
            let source = os_source(&directory.path().join("earlier original native source"));
            assert!(native_sources
                .row(MetadataOriginalNamespace::Main, main)
                .unknown
                .set(source)
                .is_ok());
            native_sources.mark_failed();
            let actual = root.resource_failures().pop().unwrap();
            assert!(actual.original_sources().is_none());
            assert!(actual.acknowledge_retained(&actual));
            Some(actual)
        } else {
            None
        };
        assert!(!original
            .permit
            .retire_protocol(
                &tasks,
                tokio::time::Instant::now() + Duration::from_millis(10)
            )
            .await
            .unwrap());
        let receiver = root.resource_failures().pop().unwrap();
        assert_eq!(receiver.instance, instance());
        assert_eq!(receiver.ownership_received(), native_first);
        assert!(receiver.acknowledge_retained(&receiver));
        if let Some(earlier) = &native_receiver {
            assert!(Arc::ptr_eq(earlier, &receiver));
            assert!(
                receiver.original_sources().is_some(),
                "same already-received RF gained its exact late ledger"
            );
        }
        let child = receiver
            .original_sources()
            .unwrap()
            .protocol()
            .unwrap()
            .clone();
        assert!(child.source().is_some());
        let main = native_sources
            .originals()
            .find(|row| row.namespace == MetadataOriginalNamespace::Main)
            .unwrap()
            .reservation;
        let probe = native_sources
            .originals()
            .find(|row| row.namespace == MetadataOriginalNamespace::Probe)
            .expect("actual Linux GSO/GRO probe reservation")
            .reservation;
        // Equal numeric IDs across the two real native owners stay distinct.
        assert_eq!(main.get(), 0, "actual native main IDs start at zero");
        assert_eq!(
            probe.get(),
            0,
            "probe namespace independently starts at zero"
        );
        assert_eq!(main.get(), probe.get());
        let main_source = os_source(&directory.path().join("main"));
        let probe_source = os_source(&directory.path().join("probe"));
        assert!(native_sources
            .row(MetadataOriginalNamespace::Main, main)
            .close
            .set(main_source.clone())
            .is_ok());
        assert!(native_sources
            .row(MetadataOriginalNamespace::Probe, probe)
            .close
            .set(probe_source.clone())
            .is_ok());
        native_sources.mark_failed();
        assert!(Arc::ptr_eq(&receiver, &root.resource_failures()[0]));
        assert!(Arc::ptr_eq(
            native_sources
                .originals()
                .find(|row| row.namespace == MetadataOriginalNamespace::Main)
                .unwrap()
                .first_close
                .unwrap(),
            &main_source
        ));
        assert!(Arc::ptr_eq(
            native_sources
                .originals()
                .find(|row| row.namespace == MetadataOriginalNamespace::Probe)
                .unwrap()
                .first_close
                .unwrap(),
            &probe_source
        ));
        let weak = Arc::downgrade(&native_sources.0);
        original.released.store(true, Ordering::Release);
        original.changed.notify_waiters();
        std::future::poll_fn(|cx| {
            root.poll_cancelled_retirements(cx);
            if origin.closed().is_some() {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await;
        let receipt = origin.closed().unwrap();
        assert!(receipt.matches(&origin) && receipt.had_native_acquisition());
        assert_eq!(root.snapshot().charged, 1);
        assert!(!retirement.is_settled());
        let deferred = scope.defer_callbacks();
        scope.acknowledge(&joined).unwrap();
        assert_eq!(root.snapshot().charged, 1);
        drop(deferred);
        actor.wait_registration_released().await;
        std::future::poll_fn(|cx| {
            root.poll_cancelled_retirements(cx);
            if root.snapshot().charged == 0 {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await;
        assert!(retirement.is_settled());
        assert!(owner.shutdown().unwrap().is_complete());
        drop(original);
        drop(owner);
        drop(origin);
        drop(receipt);
        drop(native_sources);
        drop(retirement);
        assert!(
            weak.upgrade().is_some(),
            "actual receiver retains the late sources"
        );
        drop(receiver);
        drop(native_receiver);
        drop(child);
        assert!(
            weak.upgrade().is_none(),
            "pure row has no Slot/Phase/IO cycle"
        );
        scope.begin_shutdown();
        assert!(scope.is_drained());
    })
    .await
    .unwrap();
}
