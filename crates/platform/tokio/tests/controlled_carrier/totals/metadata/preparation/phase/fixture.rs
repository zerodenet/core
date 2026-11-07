use super::*;
pub(super) async fn run(panic_first: bool) {
    timeout(Duration::from_secs(3), async {
        let one = NonZeroUsize::new(1).unwrap();
        let totals = CarrierOriginalTotals::new(one);
        let mut scope = TaskScope::suspended_hosted(tokio::runtime::Handle::current(), Some(one));
        scope.enable_registration().unwrap();
        let tasks = scope.registrar();
        let host = Arc::new(CancellationToken::new());
        let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let remote = peer.local_addr().unwrap();
        let endpoint = destination(remote);
        let limits = MetadataLimits {
            main_sockets: one,
            probe_sockets: one,
            // Existing native GSO/GRO initialization may test v4 and v6.
            // This positive original/ACK fixture does not test probe quota.
            probe_attempts_per_initialization: NonZeroUsize::new(4).unwrap(),
            concurrent_send_calls: one,
            write_pollers: one,
        };
        let owner = Arc::new(
            MetadataPoolOwner::new(instance(), one, limits, &CancellationToken::new()).unwrap(),
        );
        owner
            .registration()
            .bind_declared_with_totals(
                &ResourceDeclaration::ControlledMetadataCarrier {
                    endpoint: endpoint.clone(),
                    dial: Default::default(),
                    limits,
                    max_origins: one,
                    ports: vec![],
                },
                &tasks,
                &host,
                &totals,
            )
            .unwrap();
        owner.start().unwrap();
        let original = owner.grants().prepare_original(&tasks).unwrap();
        let origin = original.origin();
        let (begin, started) = tokio::sync::oneshot::channel::<(
            Arc<Retained>,
            crate::contract::scope::TaskJoinWitness,
        )>();
        let (permit, actor) = original
            .spawn_auxiliary(&tasks, async move {
                let Ok((retained, actor)) = started.await else {
                    return Ok(());
                };
                let egress = Arc::new(crate::EgressInterfaceControl::default());
                let binding = UpstreamBinding {
                    prefix: vec![],
                    dial: Default::default(),
                    network_generation: NetworkGeneration(egress.generation()),
                };
                let opener =
                    NativeCarrierOpener::new(instance(), binding.clone(), remote, egress, None)
                        .unwrap();
                let candidates = opener
                    .resolve_controlled_carrier(&binding, endpoint, context())
                    .await?;
                let (_, grant) =
                    candidates.open_metadata(0, None, &retained.permit, &actor, deadline())?;
                // Install the unique actual views before the first actual send.
                *retained.wire.lock().unwrap() = Some(grant);
                let (sender, mut writable) = {
                    let wire = retained.wire.lock().unwrap();
                    let wire = wire.as_ref().unwrap();
                    (wire.tx.clone(), wire.tx.create_write_poller().unwrap())
                };
                let message = MetadataTransmit {
                    destination: remote,
                    contents: b"same Root retained UDP",
                    source_ip: None,
                    ecn: None,
                    segment_size: None,
                };
                std::future::poll_fn(|cx| loop {
                    std::task::ready!(writable.poll_writable(cx)).unwrap();
                    match sender.try_send(&message) {
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => continue,
                        result => return Poll::Ready(result.unwrap()),
                    }
                })
                .await;
                drop(writable);
                drop(sender);
                Ok(())
            })
            .unwrap();
        let permit = Arc::new(permit);
        let gate = Arc::new(Gate::default());
        gate.panic_first.store(panic_first, Ordering::Release);
        let retained = Arc::new(Retained {
            gate: gate.clone(),
            _owner: owner.clone(),
            permit,
            tasks: tasks.clone(),
            wire: Mutex::default(),
            claim: Mutex::default(),
        });
        let phase = retained
            .permit
            .install_protocol_control(&tasks, &actor, Box::new(Control(retained.clone())).into())
            .unwrap();
        assert!(begin.send((retained.clone(), actor.clone())).is_ok());
        let mut bytes = [0; 64];
        let mut early_receipt = None;
        // Keep the actual original task result observable if native setup
        // fails before its first wire write, instead of reporting only outer
        // Elapsed while the real Scope already holds its typed cause.
        let first = tokio::select! { biased;
            wire = peer.recv_from(&mut bytes) => Ok(wire.unwrap()),
            receipt = scope.next_receipt() => Err(receipt.unwrap()),
        };
        let (size, _) = match first {
            Ok(wire) => wire,
            Err(receipt) => {
                assert!(actor.matches_joined(receipt.joined()));
                assert!(
                    receipt.result().is_ok(),
                    "actual setup actor failed before UDP wire: {:?}",
                    receipt.result()
                );
                early_receipt = Some(receipt);
                peer.recv_from(&mut bytes).await.unwrap()
            }
        };
        assert_eq!(&bytes[..size], b"same Root retained UDP");
        let receipt = match early_receipt {
            Some(receipt) => receipt,
            None => scope.next_receipt().await.unwrap(),
        };
        assert!(actor.matches_joined(receipt.joined()) && receipt.result().is_ok());
        assert!(!receipt.joined().registration_released());
        assert!(origin.closed().is_none());
        assert_eq!(totals.snapshot().charged, 1);
        // The observer's original absolute deadline ends only its wait. The
        // same actual operation must retain the real UDP and wake Root later.
        assert!(!retained
            .permit
            .retire_protocol(&tasks, Instant::now() + Duration::from_millis(10))
            .await
            .unwrap());
        assert_eq!(gate.entered.load(Ordering::Acquire), 1);
        assert!(retained.wire.lock().unwrap().is_some());
        assert!(origin.closed().is_none());
        assert!(!phase.is_settled());
        std::future::poll_fn(|cx| {
            totals.poll_cancelled_retirements(cx);
            Poll::Ready(())
        })
        .await;
        let original_fault = if panic_first {
            let failure = totals
                .failures()
                .into_iter()
                .find(|failure| {
                    std::error::Error::source(failure.as_ref())
                        .and_then(|source| {
                            source.downcast_ref::<crate::original_protocol::OriginalPollPanic>()
                        })
                        .is_some_and(|panic| panic.contains::<ActualPollFailure>())
                })
                .expect("actual Root retained the original typed poll panic payload");
            assert_eq!(failure.stage, Stage::Close);
            assert_eq!(failure.reason, Reason::Internal);
            Some(failure)
        } else {
            assert!(
                totals.failures().is_empty(),
                "observer expiry is not a Root cleanup fault"
            );
            None
        };
        drop(retained);
        drop(owner);
        // No protocol observer, another dial, ResourceLifecycle shutdown or
        // replacement owner remains. The same Root controller owns this work.
        phase.request_retirement();
        let mut release = false;
        std::future::poll_fn(|cx| {
            totals.poll_cancelled_retirements(cx);
            if origin.closed().is_some() {
                Poll::Ready(())
            } else {
                // Root first installs its actual standing waker on the same
                // pending operation; only this late physical condition wakes it.
                if !release {
                    release = true;
                    gate.release();
                }
                Poll::Pending
            }
        })
        .await;
        assert_eq!(
            gate.entered.load(Ordering::Acquire),
            if panic_first { 2 } else { 1 }
        );
        if let Some(original) = &original_fault {
            assert!(totals
                .failures()
                .iter()
                .any(|actual| Arc::ptr_eq(actual, original)));
        }
        let native = origin.closed().unwrap();
        assert!(native.matches(&origin) && native.had_native_acquisition());
        assert_eq!(totals.snapshot().charged, 1);
        assert!(!phase.is_settled());
        let deferred = scope.defer_callbacks();
        scope.acknowledge(&receipt).unwrap();
        assert_eq!(totals.snapshot().charged, 1);
        drop(deferred);
        actor.wait_registration_released().await;
        std::future::poll_fn(|cx| {
            totals.poll_cancelled_retirements(cx);
            if totals.snapshot().charged == 0 {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await;
        assert!(native.matches(&origin));
        assert!(phase.is_settled());
        scope.begin_shutdown();
        assert!(scope.is_drained());
    })
    .await
    .unwrap();
}
