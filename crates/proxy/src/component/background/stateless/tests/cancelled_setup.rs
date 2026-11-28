//! Cancelled unpublished Business setup (`deadline: None`) settles via `try_close`
//! and never permanently blocks subsequent `N=1` Business admissions.
use super::*;
use std::{future::Future, task::Poll};

struct SuspendedHandshakeRole {
    peer: Destination,
    suspended_first: std::sync::atomic::AtomicBool,
    entered: Arc<tokio::sync::Notify>,
    hold: Arc<tokio::sync::Notify>,
}

impl StreamOutbound for SuspendedHandshakeRole {
    type Stream = BoxStream;

    async fn connect(
        &self,
        _request: ConnectRequest,
        context: OpenContext,
    ) -> Result<Connected<BoxStream>, ChannelError> {
        let io = context
            .upstream
            .open_stream(
                self.peer.clone(),
                CarrierContext {
                    session: context.session_identity()?,
                    deadline: context.deadline,
                    cancel: context.cancel.clone(),
                },
            )
            .await?;
        if !self
            .suspended_first
            .swap(true, std::sync::atomic::Ordering::AcqRel)
        {
            let wait = self.hold.notified();
            tokio::pin!(wait);
            let mut announced = false;
            std::future::poll_fn(|cx| {
                let state = wait.as_mut().poll(cx);
                if !announced && state.is_pending() {
                    announced = true;
                    self.entered.notify_one();
                }
                state
            })
            .await;
        }
        Ok(Connected {
            io,
            peer_confirmation: PeerConfirmation::Confirmed,
        })
    }
}

#[tokio::test]
async fn cancelled_no_deadline_setup_reaps_cleanly_and_reports_capacity_breakdown_when_full() {
    cancelled_setup(WaitOutcome::Release).await;
}

#[tokio::test]
async fn no_deadline_reap_grace_expires_without_releasing_credit_or_poisoning_cleanup() {
    cancelled_setup(WaitOutcome::Expire).await;
}

#[tokio::test]
async fn no_deadline_reap_cancellation_preserves_original_until_actual_aux_ack() {
    cancelled_setup(WaitOutcome::Cancel).await;
}

enum WaitOutcome {
    Release,
    Expire,
    Cancel,
}

async fn cancelled_setup(outcome: WaitOutcome) {
    tokio::time::timeout(std::time::Duration::from_secs(8), async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let peer = listener.local_addr().unwrap();
        let endpoint = Destination::new(Address::Ipv4([127, 0, 0, 1]), peer.port());
        let server = tokio::spawn(async move {
            let (mut first_peer, _) = listener.accept().await.unwrap();
            assert_eq!(
                first_peer.read(&mut [0; 1]).await.unwrap(),
                0,
                "cancelled unpublished TCP socket closes cleanly"
            );
            let (mut second_peer, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 4];
            second_peer.read_exact(&mut buf).await.unwrap();
            assert_eq!(&buf, b"ping");
            second_peer.write_all(b"pong").await.unwrap();
            second_peer.shutdown().await.unwrap();
        });

        let one = NonZeroUsize::new(1).unwrap();
        let two = NonZeroUsize::new(2).unwrap();
        let mut scope = TaskScope::suspended_hosted(tokio::runtime::Handle::current(), Some(one));
        scope.enable_registration().unwrap();
        let scope_registrar = scope.registrar();
        let (stop_scope_ack, mut scope_ack_stopped) = tokio::sync::oneshot::channel();
        // Delay the actual Scope receipt, not a synthetic cleanup callback.
        let (release_aux_ack, wait_aux_ack) = tokio::sync::oneshot::channel();
        let (aux_exited, wait_aux_exit) = tokio::sync::oneshot::channel();
        let scope_ack = tokio::spawn(async move {
            let mut gate = Some((aux_exited, wait_aux_ack));
            loop {
                tokio::select! {
                    _ = &mut scope_ack_stopped => break,
                    receipt = scope.next_receipt() => {
                        let Some(receipt) = receipt else { break };
                        if let Some((entered, release)) = gate.take() {
                            let _ = entered.send(());
                            release.await.unwrap();
                        }
                        let _ = scope.acknowledge(&receipt);
                    }
                }
            }
            scope
        });

        let mut caller = TaskScope::suspended_hosted(tokio::runtime::Handle::current(), Some(two));
        caller.enable_registration().unwrap();
        let caller_registrar = caller.registrar();
        let (stop_caller_ack, mut caller_ack_stopped) = tokio::sync::oneshot::channel();
        let caller_ack = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = &mut caller_ack_stopped => break,
                    receipt = caller.next_receipt() => {
                        let Some(receipt) = receipt else { break };
                        let _ = caller.acknowledge(&receipt);
                    }
                }
            }
            caller
        });

        let (stop_first_caller, first_caller_stopped) = tokio::sync::oneshot::channel();
        let (_, first_witness) = caller_registrar
            .spawn_tracked(
                zero_platform_tokio::contract::lifecycle::TaskClass::Critical,
                async move {
                    let _ = first_caller_stopped.await;
                    Ok(())
                },
            )
            .unwrap();
        let (stop_second_caller, second_caller_stopped) = tokio::sync::oneshot::channel();
        let (_, second_witness) = caller_registrar
            .spawn_tracked(
                zero_platform_tokio::contract::lifecycle::TaskClass::Critical,
                async move {
                    let _ = second_caller_stopped.await;
                    Ok(())
                },
            )
            .unwrap();

        let instance = ComponentInstanceId {
            component: ComponentId(947),
            generation: ResourceGeneration(1),
        };
        let host = Arc::new(CancellationToken::new());
        let mut owner = CarrierOwner::new(instance, one, &host);
        let bound = owner
            .registration()
            .bind_declared(
                &ResourceDeclaration::ControlledNodeCarrier {
                    endpoint: endpoint.clone(),
                    dial: Default::default(),
                    max_sockets: one,
                },
                &scope_registrar,
                &host,
            )
            .unwrap();
        owner.start(deadline()).await.unwrap();

        let egress = Arc::new(zero_platform_tokio::EgressInterfaceControl::default());
        let binding = UpstreamBinding {
            prefix: vec![],
            dial: Default::default(),
            network_generation: zero_core::component::NetworkGeneration(egress.generation()),
        };
        let request_facts = facts(&endpoint);
        let entered = Arc::new(tokio::sync::Notify::new());
        let hold = Arc::new(tokio::sync::Notify::new());
        let transfers = Arc::new(Mutex::new(Vec::new()));
        let role = Outbound {
            control_only: false,
            inner: erase_stream(SuspendedHandshakeRole {
                peer: endpoint.clone(),
                suspended_first: false.into(),
                entered: entered.clone(),
                hold: hold.clone(),
            }),
            tasks: Some(scope_registrar.clone()),
            cleanup_network: None,
            process: None,
            endpoint: endpoint.clone(),
            dial: Default::default(),
            grants: owner.grants(),
            transfers: transfers.clone(),
            limit: one,
        };

        let first_cancel = CancellationToken::new();
        let first_demand = Arc::new(Demand(first_cancel.clone(), None));
        let first_transport = UpstreamTransport::new(
            binding.clone(),
            Arc::new(Root {
                bound: bound.clone(),
                host: host.clone(),
                peer,
                native_connect_timeout: None,
                binding: binding.clone(),
                egress: egress.clone(),
                caller: first_witness,
                demand: first_demand,
                facts: request_facts.clone(),
            }),
        );
        let mut first_connect = Box::pin(role.connect(
            ConnectRequest {
                destination: endpoint.clone(),
            },
            OpenContext {
                identity: RequestIdentity::Business(request_facts.clone()),
                deadline: None,
                cancel: first_cancel.clone(),
                upstream: first_transport,
                observer: None,
            },
        ));
        tokio::select! {
            result = &mut first_connect => {
                panic!("first connect must suspend inside handshake before cancellation: {:?}", result.err());
            }
            _ = entered.notified() => {}
        }
        std::future::poll_fn(|cx| {
            assert!(first_connect.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;

        first_cancel.cancel();
        let cancelled = first_connect.await.err().unwrap();
        assert_eq!(cancelled.reason, Reason::Cancelled);
        let _ = stop_first_caller.send(());

        let second_demand = Arc::new(Demand(CancellationToken::new(), None));
        let second_transport = UpstreamTransport::new(
            binding.clone(),
            Arc::new(Root {
                bound,
                host: host.clone(),
                peer,
                native_connect_timeout: None,
                binding: binding.clone(),
                egress,
                caller: second_witness,
                demand: second_demand.clone(),
                facts: request_facts.clone(),
            }),
        );
        // The first auxiliary has really exited, but its host ACK is withheld.
        // No new native credit may be admitted during this explicit suspension.
        wait_aux_exit.await.unwrap();
        let reap_cancel = CancellationToken::new();
        let reap_context = OpenContext {
            identity: RequestIdentity::Business(request_facts.clone()),
            deadline: None,
            cancel: reap_cancel.clone(),
            upstream: second_transport.clone(),
            observer: None,
        };
        let mut reap = Box::pin(role.reap(&reap_context));
        std::future::poll_fn(|cx| {
            assert!(reap.as_mut().poll(cx).is_pending(), "actual Aux ACK must suspend grace wait");
            Poll::Ready(())
        }).await;
        assert_eq!(owner.retained(), 1);
        assert!(role.reserve_transfer(None).is_err(), "N=1 stays occupied before original ACK");
        match outcome {
            WaitOutcome::Release => {
                release_aux_ack.send(()).unwrap();
                reap.await.unwrap();
            }
            WaitOutcome::Expire => {
                tokio::time::pause();
                tokio::time::advance(std::time::Duration::from_secs(4)).await;
                std::future::poll_fn(|cx| {
                    assert!(reap.as_mut().poll(cx).is_pending());
                    Poll::Ready(())
                }).await;
                tokio::time::advance(std::time::Duration::from_secs(1)).await;
                assert_eq!(reap.await.unwrap_err().reason, Reason::Capacity);
                tokio::time::resume();
                assert_eq!(owner.retained(), 1, "grace expiry is not native close proof");
                release_aux_ack.send(()).unwrap();
            }
            WaitOutcome::Cancel => {
                reap_cancel.cancel();
                assert_eq!(reap.await.unwrap_err().reason, Reason::Cancelled);
                assert_eq!(owner.retained(), 1, "cancellation must retain original receipts");
                release_aux_ack.send(()).unwrap();
            }
        }
        let mut second = role
            .connect(
                ConnectRequest {
                    destination: endpoint.clone(),
                },
                OpenContext {
                    identity: RequestIdentity::Business(request_facts.clone()),
                    deadline: None,
                    cancel: second_demand.0.clone(),
                    upstream: second_transport.clone(),
                    observer: None,
                },
            )
            .await
            .expect("second no-deadline Business dial must reap cancelled unpublished setup");

        let capacity_err = role
            .connect(
                ConnectRequest {
                    destination: endpoint,
                },
                OpenContext {
                    identity: RequestIdentity::Business(request_facts),
                    deadline: None,
                    cancel: CancellationToken::new(),
                    upstream: second_transport,
                    observer: None,
                },
            )
            .await
            .err()
            .expect("third concurrent dial on N=1 must hit capacity");
        assert_eq!(capacity_err.reason, Reason::Capacity);
        let mut message = capacity_err.to_string();
        let mut cursor = std::error::Error::source(&capacity_err);
        while let Some(next) = cursor {
            message.push_str(": ");
            message.push_str(&next.to_string());
            cursor = next.source();
        }
        assert!(
            message.contains("actual taskless original or caller ACK still occupies capacity")
                && message.contains("limit=1, in_use=1, active_published=1"),
            "unexpected capacity diagnostic: {message}"
        );

        second.io.write_all(b"ping").await.unwrap();
        second.io.shutdown().await.unwrap();
        let mut reply = [0u8; 4];
        second.io.read_exact(&mut reply).await.unwrap();
        assert_eq!(&reply, b"pong");
        drop(second);
        let _ = stop_second_caller.send(());

        server.await.unwrap();
        assert!(owner.shutdown(deadline()).await.unwrap().is_complete());
        assert_eq!(owner.retained(), 0);
        drop(role);
        drop(scope_registrar);
        drop(caller_registrar);
        let _ = stop_scope_ack.send(());
        let _ = stop_caller_ack.send(());
        let mut scope = scope_ack.await.unwrap();
        let mut caller = caller_ack.await.unwrap();
        scope.begin_shutdown();
        caller.begin_shutdown();
        while let Some(receipt) = scope.next_receipt().await {
            let _ = scope.acknowledge(&receipt);
        }
        while let Some(receipt) = caller.next_receipt().await {
            let _ = caller.acknowledge(&receipt);
        }
        assert!(scope.is_drained() && caller.is_drained());
    })
    .await
    .unwrap();
}
