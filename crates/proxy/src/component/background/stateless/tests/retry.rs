//! Real native connect errors, not synthetic protocol errors, permit a fresh
//! original against the next candidate of the same domain demand.
use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use zero_platform_tokio::{
    contract::dns::{DnsGeneration, DnsQueryContext, DnsRequest, DnsResolution, DnsService},
    controlled_carrier::ResolvingCarrierOpener,
};

struct NodeDns;
impl DnsService for NodeDns {
    fn generation(&self) -> DnsGeneration {
        DnsGeneration::new(1, 0)
    }
    fn resolve(
        &self,
        request: DnsRequest,
        _context: DnsQueryContext,
    ) -> BoxFuture<'_, Result<DnsResolution, ChannelError>> {
        assert_eq!(request.name, "retry.node.test");
        Box::pin(async move {
            Ok(DnsResolution {
                addresses: vec![
                    zero_traits::IpAddress::V4([127, 0, 0, 2]),
                    zero_traits::IpAddress::V4([127, 0, 0, 1]),
                ],
                generation: self.generation(),
            })
        })
    }
}

struct CandidateRoot {
    root: Root,
    candidates:
        tokio::sync::Mutex<Option<zero_platform_tokio::controlled_carrier::CarrierCandidates>>,
    attempts: AtomicUsize,
}
impl UpstreamOpener for CandidateRoot {
    fn has_tracked_business_caller(&self) -> bool {
        true
    }
    fn packet_caller(
        &self,
        binding: &UpstreamBinding,
        identity: &RequestIdentity,
        call: &CarrierContext,
    ) -> Result<TaskJoinWitness, ChannelError> {
        self.root.packet_caller(binding, identity, call)
    }
    fn acquire_controlled_stream(
        &self,
        binding: &UpstreamBinding,
        endpoint: Destination,
        call: CarrierContext,
        grant: CarrierGrant,
    ) -> BoxFuture<'_, Result<zero_platform_tokio::controlled_carrier::CarrierIo, ChannelError>>
    {
        let binding = binding.clone();
        Box::pin(async move {
            let ticket = self
                .root
                .bound
                .node_business_issuer(&self.root.host, &grant, &endpoint, &binding)?
                .authorize_with_caller(self.root.demand.clone(), self.root.caller.clone())?;
            let dns = Arc::new(NodeDns);
            let mut candidates = self.candidates.lock().await;
            if candidates.is_none() {
                *candidates = Some(
                    ResolvingCarrierOpener::new(
                        grant.instance(),
                        binding.clone(),
                        endpoint.clone(),
                        dns.clone(),
                        self.root.egress.clone(),
                        None,
                    )?
                    .resolve_controlled_carrier(
                        &binding,
                        endpoint.clone(),
                        CarrierContext {
                            session: None,
                            deadline: call.deadline,
                            cancel: call.cancel.clone(),
                        },
                    )
                    .await?,
                );
            }
            let index = self.attempts.fetch_add(1, Ordering::SeqCst);
            let peer = candidates
                .as_ref()
                .unwrap()
                .peer(index)
                .expect("finite candidate cursor");
            drop(candidates);
            let native = if matches!(endpoint.address, Address::Domain(_)) {
                NativeCarrierOpener::business_resolved(
                    ticket,
                    peer,
                    dns.clone(),
                    dns.generation(),
                    self.root.egress.clone(),
                    None,
                )?
            } else {
                NativeCarrierOpener::business(ticket, self.root.egress.clone(), None)?
            };
            Ok(native
                .prepare_controlled_stream(&binding, endpoint, call.clone(), grant)?
                .open_with_deadline(call.deadline)
                .await?
                .into_parts()
                .0)
        })
    }
    fn open_stream(
        &self,
        _: &UpstreamBinding,
        _: Destination,
        _: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxStream, ChannelError>> {
        Box::pin(async { panic!("controlled original required") })
    }
    fn open_datagram(
        &self,
        _: &UpstreamBinding,
        _: zero_platform_tokio::contract::datagram::DatagramRequest,
        _: CarrierContext,
    ) -> BoxFuture<
        '_,
        Result<zero_platform_tokio::contract::datagram::BoxDatagramSession, ChannelError>,
    > {
        Box::pin(async { panic!("no datagram original") })
    }
}

struct Handshake {
    endpoint: Destination,
    reject_after_connect: bool,
}
impl StreamOutbound for Handshake {
    type Stream = BoxStream;
    async fn connect(
        &self,
        request: ConnectRequest,
        context: OpenContext,
    ) -> Result<Connected<BoxStream>, ChannelError> {
        let connected = RawRole(self.endpoint.clone())
            .connect(request, context)
            .await?;
        if self.reject_after_connect {
            return Err(crate::component::instance::invalid(
                Reason::Io,
                "protocol handshake failed",
            ));
        }
        Ok(connected)
    }
}

#[tokio::test]
async fn no_deadline_domain_retries_real_connect_failure_with_fresh_original_at_n_one() {
    retry_case(false, false).await;
}

#[tokio::test]
async fn domain_handshake_failure_does_not_retry_even_when_next_candidate_exists() {
    retry_case(false, true).await;
}

#[tokio::test]
async fn literal_connect_failure_does_not_retry_original() {
    retry_case(true, false).await;
}

async fn retry_case(literal: bool, reject_handshake: bool) {
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let good = listener.local_addr().unwrap();
        // Retain the failing endpoint's actual bound socket. It cannot be stolen
        // by another fixture and is deliberately not listening: connect refuses.
        let refused = tokio::net::TcpSocket::new_v4().unwrap();
        refused
            .bind(std::net::SocketAddr::from(([127, 0, 0, 2], good.port())))
            .unwrap();
        let endpoint = Destination::new(
            if literal {
                Address::Ipv4([127, 0, 0, 2])
            } else {
                Address::Domain("retry.node.test".into())
            },
            good.port(),
        );
        let one = NonZeroUsize::new(1).unwrap();
        let mut scope = TaskScope::suspended_hosted(tokio::runtime::Handle::current(), Some(one));
        scope.enable_registration().unwrap();
        let registrar = scope.registrar();
        let (stop_ack, mut stopped) = tokio::sync::oneshot::channel();
        let ack = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = &mut stopped => break,
                    receipt = scope.next_receipt() => {
                        let Some(receipt) = receipt else { break };
                        scope.acknowledge(&receipt).unwrap();
                    }
                }
            }
            scope
        });
        let mut caller = TaskScope::suspended_hosted(tokio::runtime::Handle::current(), Some(one));
        caller.enable_registration().unwrap();
        let (done, wait) = tokio::sync::oneshot::channel();
        let (_, witness) = caller
            .registrar()
            .spawn_tracked(
                zero_platform_tokio::contract::lifecycle::TaskClass::Critical,
                async move {
                    let _ = wait.await;
                    Ok(())
                },
            )
            .unwrap();
        let host = Arc::new(CancellationToken::new());
        let instance = ComponentInstanceId {
            component: ComponentId(949),
            generation: ResourceGeneration(1),
        };
        let mut owner = CarrierOwner::new(instance, one, &host);
        let bound = owner
            .registration()
            .bind_declared(
                &ResourceDeclaration::ControlledNodeCarrier {
                    endpoint: endpoint.clone(),
                    dial: Default::default(),
                    max_sockets: one,
                },
                &registrar,
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
        let demand = Arc::new(Demand(CancellationToken::new(), None));
        let request_facts = facts(&endpoint);
        let root = Arc::new(CandidateRoot {
            root: Root {
                bound,
                host: host.clone(),
                peer: good,
                native_connect_timeout: None,
                binding: binding.clone(),
                egress,
                caller: witness,
                demand: demand.clone(),
                facts: request_facts.clone(),
            },
            candidates: tokio::sync::Mutex::new(None),
            attempts: AtomicUsize::new(0),
        });
        let role = Outbound {
            control_only: false,
            inner: erase_stream(Handshake {
                endpoint: endpoint.clone(),
                reject_after_connect: reject_handshake,
            }),
            tasks: Some(registrar.clone()),
            cleanup_network: None,
            process: None,
            endpoint: endpoint.clone(),
            dial: Default::default(),
            grants: owner.grants(),
            transfers: Arc::new(Mutex::new(Vec::new())),
            limit: one,
        };
        // For the protocol-failure negative case the first candidate must accept
        // TCP, so a fake tagged connect error cannot accidentally satisfy it.
        let (failed_listener, refused) = if reject_handshake {
            (Some(refused.listen(1).unwrap()), None)
        } else {
            (None, Some(refused))
        };
        let result = role
            .connect(
                ConnectRequest {
                    destination: endpoint.clone(),
                },
                OpenContext {
                    identity: RequestIdentity::Business(request_facts),
                    deadline: None,
                    cancel: demand.0.clone(),
                    upstream: UpstreamTransport::new(binding, root.clone()),
                    observer: None,
                },
            )
            .await;
        if literal || reject_handshake {
            assert!(result.is_err());
            assert_eq!(
                root.attempts.load(Ordering::SeqCst),
                1,
                "not a retryable original failure"
            );
        } else {
            let mut connected =
                result.expect("real connect failure must advance domain cursor at N=1");
            assert_eq!(root.attempts.load(Ordering::SeqCst), 2);
            let (mut peer, _) = listener.accept().await.unwrap();
            connected.io.write_all(b"ping").await.unwrap();
            let mut ping = [0; 4];
            peer.read_exact(&mut ping).await.unwrap();
            assert_eq!(&ping, b"ping");
            drop(connected);
            drop(peer);
        }
        done.send(()).unwrap();
        let receipt = caller.next_receipt().await.unwrap();
        caller.acknowledge(&receipt).unwrap();
        // Actual close receipts, transferred views and caller ACK are required,
        // not merely a free port or a dropped future.
        let old = role.transfers.lock().unwrap().clone();
        for transfer in old {
            assert!(transfer.close(deadline()).await.unwrap().is_complete());
            assert!(transfer.adoption.closed().is_some());
            assert!(transfer.adoption.users_closed().is_some());
        }
        assert!(owner.shutdown(deadline()).await.unwrap().is_complete());
        assert_eq!(owner.retained(), 0);
        drop(role);
        drop(registrar);
        stop_ack.send(()).unwrap();
        let mut scope = ack.await.unwrap();
        scope.begin_shutdown();
        caller.begin_shutdown();
        assert!(scope.is_drained() && caller.is_drained());
        drop(failed_listener);
        drop(refused);
    })
    .await
    .unwrap();
}
