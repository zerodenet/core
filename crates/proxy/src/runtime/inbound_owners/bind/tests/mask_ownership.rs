//! Actual provider, Scope joins and original user receipts, independently of
//! raw-ICMP server receive permission. No port probe substitutes for cleanup.
use super::*;
#[path = "mask_ownership/failures.rs"]
mod failures;
use zero_core::component::{ComponentId, ComponentInstanceId, ResourceGeneration};
use zero_platform_tokio::contract::{
    component::*, context::*, inbound::*, lifecycle::*, scope::TaskScope, Reason,
};
use zero_transport::finalmask::{self, udp::Mask};
fn n(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap()
}
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(3)
}
struct Hold {
    _peer: finalmask::owned::source::Peer,
}
impl InboundHandshake for Hold {
    async fn complete(self, _: HandshakeContext) -> Result<AcceptedRequest, BuildError> {
        panic!("ownership fixture never handshakes a fabricated request")
    }
}
#[tokio::test]
async fn masked_xdns_original_waits_for_transferred_peer_and_same_scope_join_before_native_close() {
    timeout(Duration::from_secs(8), async {
        let masks = vec![Mask::Xdns {
            domain: "t.example.com".into(),
        }];
        let mut scope = TaskScope::new(tokio::runtime::Handle::current(), n(1));
        let tasks = scope.registrar();
        let limits = zero_platform_tokio::controlled_datagram::metadata::MetadataLimits {
            main_sockets: n(1),
            probe_sockets: n(2),
            probe_attempts_per_initialization: n(8),
            concurrent_send_calls: n(2),
            write_pollers: n(8),
        };
        let declarations = vec![ResourceDeclaration::MetadataDatagram {
            source: "127.0.0.1:0".parse().unwrap(),
            limits,
            max_bind_attempts: n(1),
        }];
        let native = crate::component::instance::HostResources::new_for_instance(
            ComponentInstanceId {
                component: ComponentId(946),
                generation: ResourceGeneration(1),
            },
            &declarations,
            tasks.cancellation(),
            tasks.scope_identity(),
        )
        .unwrap();
        let address = Arc::new(std::sync::OnceLock::new());
        let provider = Arc::new(Tap {
            native: native.clone(),
            address: address.clone(),
        });
        let returned = Arc::new(std::sync::Mutex::new(None));
        let sender = returned.clone();
        let (mut lifecycle, mut source) = finalmask::owned::source::source(
            MetadataDatagramRequest {
                ordinal: 0,
                source: "127.0.0.1:0".parse().unwrap(),
                limits,
            },
            masks.clone(),
            provider,
            tasks.clone(),
            tasks.cancellation(),
            n(2),
            n(2),
            move |peer| {
                *sender.lock().unwrap() = Some((peer.socket.clone(), peer.remote));
                Hold { _peer: peer }
            },
        );
        // Observe the same issued original address; no fabricated provider grant.
        let access = native.activate(deadline()).unwrap();
        lifecycle.start(deadline()).await.unwrap();
        drop(access);
        let address = *address.get().unwrap();
        let client = finalmask::packet_socket::wrap(
            std::net::UdpSocket::bind("127.0.0.1:0").unwrap(),
            &masks,
            false,
        )
        .unwrap();
        client.send_to(b"real original", address).await.unwrap();
        let held = source
            .next(AcceptContext {
                deadline: Some(deadline()),
                cancel: Default::default(),
            })
            .await
            .unwrap()
            .unwrap();
        scope.begin_shutdown();
        let first = lifecycle
            .shutdown(Instant::now() + Duration::from_millis(40))
            .await;
        assert!(matches!(first,Err(e) if e.reason==Reason::Timeout));
        let (stale, logical_peer) = returned.lock().unwrap().take().unwrap();
        assert!(
            stale
                .send_to(b"forbidden after stop", logical_peer)
                .await
                .is_err(),
            "retained real logical sender is fenced, even before native close"
        );
        drop(held);
        assert!(
            lifecycle
                .shutdown(Instant::now() + Duration::from_millis(40))
                .await
                .is_err(),
            "a retained original sender still owns custody"
        );
        drop(stale);
        drop(source);
        drop(client);
        let (closed, joined) = tokio::join!(lifecycle.shutdown(deadline()), async {
            let mut count = 0;
            while let Some(exit) = scope.next_exit().await {
                assert!(exit.result.is_ok());
                count += 1;
            }
            count
        });
        assert!(closed.unwrap().is_complete());
        assert_eq!(joined, 1, "actual original XDNS worker joined");
        let report = native.shutdown_io(deadline()).await.unwrap();
        assert!(report.is_complete(), "{report:?}");
    })
    .await
    .unwrap();
}
#[tokio::test]
#[ignore = "requires actual native ICMP socket access; raw receive qualification is separate"]
async fn masked_xicmp_same_native_provider_is_start_fenced_and_closes_actual_original_after_driver_join(
) {
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
                component: ComponentId(947),
                generation: ResourceGeneration(1),
            },
            &[
                ResourceDeclaration::MetadataDatagram {
                    source: "127.0.0.1:0".parse().unwrap(),
                    limits,
                    max_bind_attempts: n(1),
                },
                ResourceDeclaration::NativeIcmp { max_sockets: n(1) },
            ],
            tasks.cancellation(),
            tasks.scope_identity(),
        )
        .unwrap();
        assert!(native
            .bind_inbound_icmp("127.0.0.1".parse().unwrap())
            .is_err());
        let (mut lifecycle, mut source) = finalmask::owned::source::source(
            MetadataDatagramRequest {
                ordinal: 0,
                source: "127.0.0.1:0".parse().unwrap(),
                limits,
            },
            vec![Mask::Xicmp {
                ip: "127.0.0.1".into(),
                id: 54321,
            }],
            native.clone(),
            tasks.clone(),
            tasks.cancellation(),
            n(1),
            n(1),
            |peer| Hold { _peer: peer },
        );
        let access = native.activate(deadline()).unwrap();
        lifecycle.start(deadline()).await.unwrap();
        drop(access);
        assert!(native
            .bind_inbound_icmp("127.0.0.1".parse().unwrap())
            .is_err());
        let context = AcceptContext {
            deadline: None,
            cancel: Default::default(),
        };
        let mut pending = Box::pin(source.next(context));
        std::future::poll_fn(|cx| {
            assert!(pending.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        drop(pending); // cancellation keeps the exact receive owner in the source.
        scope.begin_shutdown();
        let (report, joins) = tokio::join!(lifecycle.shutdown(deadline()), async {
            let mut count = 0;
            while let Some(exit) = scope.next_exit().await {
                assert!(exit.result.is_ok());
                count += 1;
            }
            count
        });
        assert!(report.unwrap().is_complete());
        assert_eq!(joins, 1);
        drop(source);
        let closed = native.shutdown_io(deadline()).await.unwrap();
        assert!(closed.is_complete(), "{closed:?}");
        assert!(
            closed
                .closed_io
                .iter()
                .any(|s| s.contains("ICMP originals explicitly closed")),
            "{closed:?}"
        );
        assert!(
            closed
                .closed_io
                .iter()
                .any(|s| s.contains("metadata declaration 0 main")),
            "{closed:?}"
        );
        assert!(
            native
                .shutdown_io(deadline())
                .await
                .unwrap()
                .closed_io
                .is_empty(),
            "original close receipts are consumed once"
        );
    })
    .await
    .unwrap();
}

struct Tap {
    native: Arc<crate::component::instance::HostResources>,
    address: Arc<std::sync::OnceLock<std::net::SocketAddr>>,
}
impl ResourceAccess for Tap {
    fn listen_metadata_datagram(
        &self,
        request: MetadataDatagramRequest,
    ) -> zero_platform_tokio::contract::BoxFuture<'_, Result<GrantedMetadataListener, BuildError>>
    {
        Box::pin(async move {
            let grant = self.native.listen_metadata_datagram(request).await?;
            self.address.set(grant.tx.local_addr().unwrap()).unwrap();
            Ok(grant)
        })
    }
    fn confirm_metadata_users(
        &self,
        ordinal: usize,
        receipt:&zero_platform_tokio::controlled_datagram::metadata::users::MetadataUsersClosedReceipt,
        tasks: &zero_platform_tokio::contract::scope::TaskRegistrar,
    ) -> Result<(), BuildError> {
        self.native.confirm_metadata_users(ordinal, receipt, tasks)
    }
    fn read_file(
        &self,
        path: std::path::PathBuf,
        max: usize,
    ) -> zero_platform_tokio::contract::BoxFuture<'_, Result<Vec<u8>, BuildError>> {
        self.native.read_file(path, max)
    }
    fn listen_tcp(
        &self,
        address: Destination,
    ) -> zero_platform_tokio::contract::BoxFuture<
        '_,
        Result<zero_platform_tokio::ControlledTcpListener, BuildError>,
    > {
        self.native.listen_tcp(address)
    }
    fn listen_datagram(
        &self,
        address: Destination,
    ) -> zero_platform_tokio::contract::BoxFuture<'_, Result<GrantedDatagramListener, BuildError>>
    {
        self.native.listen_datagram(address)
    }
    fn open_device(
        &self,
        key: Arc<str>,
    ) -> zero_platform_tokio::contract::BoxFuture<
        '_,
        Result<zero_platform_tokio::contract::packet::BoxPacketSession, BuildError>,
    > {
        self.native.open_device(key)
    }
}
