//! Production reverse roles: real Source originals, wire IO and separate returns.
use super::framed;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, UdpSocket},
    time::{timeout, Instant},
};
use zero_core::request::{Destination, ServiceOrigin, ServicePurpose, ServiceRequestFacts};
use zero_platform_tokio::{
    contract::{
        context::{OpenContext, RequestIdentity},
        datagram::{BoxDatagramSession, DatagramRequest},
        network::{CarrierContext, UpstreamBinding, UpstreamOpener, UpstreamTransport},
        stream::{BoxStream, ConnectRequest},
        BoxFuture, CancellationToken, ChannelError,
    },
    controlled_carrier::{AcceptedCarrierProvider, CarrierOrigin},
};
const ID: &str = "11111111-2222-3333-4444-555555555555";
fn end() -> Instant {
    Instant::now() + Duration::from_secs(5)
}
struct OriginalOnly {
    provider: Arc<AcceptedCarrierProvider>,
    observed: Arc<Mutex<Option<CarrierOrigin>>>,
}
impl UpstreamOpener for OriginalOnly {
    fn observe_carrier_origin(&self, original: &CarrierOrigin) -> bool {
        if !self.provider.contains(original) {
            return false;
        }
        *self.observed.lock().unwrap() = Some(original.clone());
        true
    }
    fn open_stream(
        &self,
        _: &UpstreamBinding,
        _: Destination,
        _: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxStream, ChannelError>> {
        Box::pin(async {
            panic!("reverse must use its original accepted Source, not another native connector")
        })
    }
    fn open_datagram(
        &self,
        _: &UpstreamBinding,
        _: DatagramRequest,
        _: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxDatagramSession, ChannelError>> {
        Box::pin(async { panic!("reverse must use its same physical FIFO") })
    }
}
#[path = "reverse/fixture.rs"]
mod fixture;
use fixture::{empty_and_multiple_targets, pair, pair_carriers};
#[tokio::test]
async fn virtual_reverse_standard_stream_datagram_preserve_real_wire_and_original_closure() {
    timeout(Duration::from_secs(60), async {
        for carrier in [
            serde_json::json!({}),
            serde_json::json!({"ws":{"path":"/reverse"}}),
            serde_json::json!({"h2":{"path":"/reverse"}}),
            serde_json::json!({"grpc":{"service_names":["reverse/Tun"]}}),
        ] {
            let (portal, running, socks, _bridge, bridge_running) = pair(carrier, None).await;
            let plan = portal.tcp_runtime_services().components.clone().unwrap();
            let slot = plan.configured("portal").unwrap();
            let provider = slot.accepted_carrier_provider().unwrap();
            let source = portal.system_tun_source_registry().owner("entry").unwrap();
            let originals = provider.originals();
            framed::exercise(socks).await;
            empty_and_multiple_targets(socks).await;
            assert!(provider.returned().is_none());
            running.shutdown().await.unwrap();
            bridge_running.shutdown().await.unwrap();
            let returned = provider
                .returned()
                .expect("original resource and caller/driver ACKs");
            assert_eq!(returned.instance(), slot.instance_id());
            for original in originals {
                assert!(original.closed().unwrap().matches(&original));
            }
            let closed = source.terminal_for_test().unwrap();
            assert!(closed.failure().is_none() && closed.task_failures().is_empty());
            assert!(closed.closed_io_receipts() > 0);
        }
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn reverse_unique_loan_return_does_not_claim_original_source_close() {
    timeout(Duration::from_secs(30), async {
        let (portal, running, _socks, _bridge, bridge_running) =
            pair(serde_json::json!({}), None).await;
        let plan = portal.tcp_runtime_services().components.clone().unwrap();
        let slot = plan.configured("portal").unwrap();
        let provider = slot.accepted_carrier_provider().unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let observed = Arc::new(Mutex::new(None));
        let context = OpenContext {
            identity: RequestIdentity::Service(
                Arc::new(ServiceRequestFacts {
                    origin: ServiceOrigin::Runtime,
                    purpose: ServicePurpose::BackgroundControl,
                })
                .into(),
            ),
            deadline: Some(end()),
            cancel: CancellationToken::new(),
            observer: None,
            upstream: UpstreamTransport::new(
                UpstreamBinding {
                    prefix: vec![],
                    dial: slot.dial.clone(),
                    network_generation: zero_core::component::NetworkGeneration(
                        portal.egress_interface.generation(),
                    ),
                },
                Arc::new(OriginalOnly {
                    provider: provider.clone(),
                    observed: observed.clone(),
                }),
            ),
        };
        let mut loan = slot
            .stream(end())
            .await
            .unwrap()
            .connect_carrier(
                ConnectRequest {
                    destination: Destination::new(
                        zero_core::Address::Ipv4([127, 0, 0, 1]),
                        address.port(),
                    ),
                },
                context,
            )
            .await
            .unwrap()
            .io;
        let (mut peer, _) = listener.accept().await.unwrap();
        loan.io.write_all(b"loan").await.unwrap();
        let mut bytes = [0; 4];
        peer.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"loan");
        let original = loan.owner.original().clone();
        assert!(observed
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .same_original(&original));
        let mut receive = Box::pin(loan.io.read_exact(&mut bytes));
        std::future::poll_fn(|cx| {
            use std::future::Future;
            assert!(
                receive.as_mut().poll(cx).is_pending(),
                "actual reverse IO suspension"
            );
            std::task::Poll::Ready(())
        })
        .await;
        drop(receive);
        let source = portal
            .system_tun_source_registry()
            .owner("entry")
            .unwrap()
            .identity();
        assert!(!provider.source_views_returned(source));
        assert!(provider.fence_source(source));
        assert!(!provider.contains(&original));
        assert!(provider.stop_source_originals(source, end()).await.is_err());
        assert!(
            original.closed().is_none(),
            "held original IO prevents worker retirement"
        );
        assert!(!loan
            .owner
            .close(Instant::now() + Duration::from_millis(10))
            .await
            .unwrap()
            .is_complete());
        drop(loan.io);
        assert!(loan.owner.close(end()).await.unwrap().is_complete());
        assert!(loan.owner.closed().unwrap().matches(&loan.owner));
        assert!(original.closed().is_none() && provider.returned().is_none());
        drop(peer);
        drop(listener);
        assert!(provider.source_views_returned(source));
        provider.stop_source_originals(source, end()).await.unwrap();
        timeout(Duration::from_secs(5), async {
            while !provider.source_originals_returned(source) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(original.closed().unwrap().matches(&original));
        assert!(
            provider.returned().is_none(),
            "worker/native receipts do not retire its factory generation"
        );
        running.shutdown().await.unwrap();
        bridge_running.shutdown().await.unwrap();
        assert!(provider.returned().is_some());
        assert!(original.closed().unwrap().matches(&original));
    })
    .await
    .unwrap();
}
#[cfg(feature = "vmess")]
#[tokio::test]
async fn vmess_grpc_borrows_the_actual_virtual_reverse_original_without_legacy_fallback() {
    timeout(Duration::from_secs(35),async{
        let (_tail,tail_running,endpoint)=framed::start(serde_json::json!({"inbounds":[{"tag":"entry","listen":{"address":"127.0.0.1","port":0},"protocol":{"type":"vmess","users":[{"id":ID}],"grpc":{"service_names":["tail/Tun"]}}}],"route":{"final":{"type":"direct"}}})).await;
        let tail=serde_json::json!({"type":"vmess","server":"127.0.0.1","port":endpoint.port(),"id":ID,"grpc":{"service_names":["tail/Tun"]}});
        let (portal,running,socks,_bridge,bridge_running)=pair(serde_json::json!({}),Some(tail)).await;
        let plan=portal.tcp_runtime_services().components.clone().unwrap();
        let last=plan.configured("last").expect("entire existing cross-protocol caller graph selected");
        assert!(last.stream(end()).await.is_ok()&&last.datagram(end()).await.is_ok());
        let provider=plan.configured("portal").unwrap().accepted_carrier_provider().unwrap();
        let originals=provider.originals();
        framed::exercise(socks).await;
        assert!(provider.originals().iter().any(|actual|originals.iter().any(|original|original.same_original(actual))));
        running.shutdown().await.unwrap();bridge_running.shutdown().await.unwrap();tail_running.shutdown().await.unwrap();
        assert!(provider.returned().is_some());
        for original in originals{assert!(original.closed().unwrap().matches(&original));}
    }).await.unwrap();
}

#[path = "reverse/transitions.rs"]
mod transitions;

#[path = "reverse/active_graph.rs"]
mod active_graph;
