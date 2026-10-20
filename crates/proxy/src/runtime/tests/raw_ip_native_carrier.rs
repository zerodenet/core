//! Local carrier evidence never settles the old whole-gateway obligation.
use super::*;
use crate::runtime::{inbound_owners::InboundOwnerRegistry, Proxy};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
use zero_core::{component::NetworkGeneration, request::Destination, Address};
use zero_platform_tokio::{
    contract::{
        network::{CarrierContext, UpstreamBinding, UpstreamOpener},
        CancellationToken,
    },
    controlled_carrier::{CarrierOwner, NativeCarrierOpener},
    EgressInterfaceControl,
};

struct Fixture {
    proxy: Proxy,
    registry: Arc<InboundOwnerRegistry>,
    native: NativeObligations,
    registration: NativeRegistration,
    carrier: CarrierOwner,
    done: Option<oneshot::Sender<()>>,
    parent: Option<oneshot::Sender<()>>,
}
impl Fixture {
    async fn new() -> Self {
        let proxy = Proxy::new(
            zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap(),
        )
        .unwrap();
        let registry = Arc::new(InboundOwnerRegistry::default());
        let (stop, _) = watch::channel(false);
        let installed = registry
            .install("carrier-observation", &proxy.protocols, 1, stop)
            .unwrap();
        let group = installed
            .child_tasks(NonZeroUsize::new(1).unwrap())
            .unwrap();
        let native = NativeObligations::new(1, 0, 0);
        installed.register(native.resource()).unwrap();
        let mut permit = native.reserve(ChildKind::Tcp).unwrap();
        let registration = permit.registration();
        let (parent, parent_done) = oneshot::channel();
        installed
            .owner()
            .spawn_driver(async move {
                parent_done.await.map_err(std::io::Error::other)?;
                Ok(())
            })
            .unwrap();
        let (done, finished) = oneshot::channel();
        let (ready, readiness) = oneshot::channel();
        assert!(group
            .spawn_bound(&installed.tasks(), move |binding| async move {
                binding.wait_bound().await.map_err(std::io::Error::other)?;
                permit.bind_caller(binding).map_err(std::io::Error::other)?;
                permit.enter().map_err(std::io::Error::other)?;
                ready.send(()).unwrap();
                finished.await.map_err(std::io::Error::other)?;
                Ok(())
            })
            .unwrap());
        readiness.await.unwrap();
        let mut carrier = CarrierOwner::new(
            proxy.protocols.allocate_resource_identity().unwrap(),
            NonZeroUsize::new(2).unwrap(),
            &CancellationToken::new(),
        );
        carrier
            .start(Instant::now() + Duration::from_secs(2))
            .await
            .unwrap();
        Self {
            proxy,
            registry,
            native,
            registration,
            carrier,
            done: Some(done),
            parent: Some(parent),
        }
    }
    async fn join(&mut self) {
        self.done.take().unwrap().send(()).unwrap();
        self.parent.take().unwrap().send(()).unwrap();
        assert!(self
            .registry
            .confirm_shutdown(
                "carrier-observation",
                Instant::now() + Duration::from_secs(2)
            )
            .await
            .is_err());
        let joined = self.registration.caller_joined().unwrap();
        let witnessed = self
            .registration
            .caller_witness()
            .unwrap()
            .try_joined()
            .unwrap();
        assert_eq!(joined.task(), witnessed.id());
        assert_eq!(joined.class(), witnessed.class());
        assert!(joined.failure().is_none());
        assert!(witnessed.result().is_ok());
    }
    async fn assert_gate_retained(&mut self) {
        assert_eq!(self.native.0.lock().unwrap().records.len(), 1);
        assert!(!self
            .native
            .shutdown(Instant::now())
            .await
            .unwrap()
            .is_complete());
        assert!(self
            .registry
            .owner("carrier-observation")
            .unwrap()
            .terminal_for_test()
            .is_none());
    }
}
#[tokio::test]
async fn unused_same_slot_is_idempotent_but_no_acquisition_and_real_caller_join_cannot_clear_gateway(
) {
    let mut fixture = Fixture::new().await;
    let grant = fixture.carrier.grants().reserve().unwrap();
    let adoption = grant.adoption();
    fixture
        .registration
        .observe_carrier(adoption.clone())
        .unwrap();
    fixture
        .registration
        .observe_carrier(adoption.clone())
        .unwrap();
    assert!(fixture.registration.carrier_completion().is_none());
    let witness = fixture.registration.caller_witness().unwrap();
    let wait = witness.wait();
    tokio::pin!(wait);
    std::future::poll_fn(|cx| {
        assert!(wait.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(grant);
    adoption
        .retire(Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    assert!(!adoption.closed().unwrap().had_native_acquisition());
    fixture.join().await;
    assert!(fixture.registration.carrier_completion().is_none());
    fixture.assert_gate_retained().await;
    assert!(fixture
        .carrier
        .shutdown(Instant::now() + Duration::from_secs(2))
        .await
        .unwrap()
        .is_complete());
    drop(fixture.proxy);
}
#[tokio::test]
async fn distinct_carrier_slot_is_sticky_unknown_and_never_replaces_the_original_observation() {
    let mut fixture = Fixture::new().await;
    let first = fixture.carrier.grants().reserve().unwrap();
    let second = fixture.carrier.grants().reserve().unwrap();
    let original = first.adoption();
    fixture
        .registration
        .observe_carrier(original.clone())
        .unwrap();
    assert_eq!(
        fixture
            .registration
            .observe_carrier(second.adoption())
            .unwrap_err()
            .reason,
        Reason::Unsupported
    );
    assert_eq!(
        fixture
            .registration
            .observe_carrier(original.clone())
            .unwrap_err()
            .reason,
        Reason::InvalidConfig
    );
    {
        let state = fixture.native.0.lock().unwrap();
        let record = state.records.get(&fixture.registration.id).unwrap();
        assert!(record.carrier_conflict);
        assert!(record.carrier.as_ref().unwrap().same_slot(&original));
        assert!(!record
            .carrier
            .as_ref()
            .unwrap()
            .same_slot(&second.adoption()));
    }
    drop((first, second));
    fixture.join().await;
    fixture
        .carrier
        .shutdown(Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    assert!(fixture.registration.carrier_completion().is_none());
    fixture.assert_gate_retained().await;
}
#[tokio::test]
async fn original_native_close_and_real_caller_join_without_actual_users_proof_do_not_clear_gateway(
) {
    let mut fixture = Fixture::new().await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let egress = Arc::new(EgressInterfaceControl::default());
    let binding = UpstreamBinding {
        prefix: Vec::new(),
        dial: Default::default(),
        network_generation: NetworkGeneration(egress.generation()),
    };
    let opener = NativeCarrierOpener::new(
        fixture.carrier.instance(),
        binding.clone(),
        address,
        egress,
        None,
    )
    .unwrap();
    let grant = fixture.carrier.grants().reserve().unwrap();
    let original = grant.adoption();
    fixture
        .registration
        .observe_carrier(original.clone())
        .unwrap();
    let prepared = opener
        .prepare_controlled_stream(
            &binding,
            Destination::new(Address::Ipv4([127, 0, 0, 1]), address.port()),
            CarrierContext {
                session: None,
                deadline: None,
                cancel: CancellationToken::new(),
            },
            grant,
        )
        .unwrap();
    let carrier = prepared
        .open(Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    let (mut peer, _) = listener.accept().await.unwrap();
    let (mut io, adoption) = carrier.into_parts();
    assert!(adoption.same_slot(&original));
    io.write_all(b"native").await.unwrap();
    let mut bytes = [0; 6];
    peer.read_exact(&mut bytes).await.unwrap();
    assert_eq!(&bytes, b"native");
    io.shutdown().await.unwrap();
    assert_eq!(peer.read(&mut bytes).await.unwrap(), 0);
    peer.write_all(b"return").await.unwrap();
    io.read_exact(&mut bytes).await.unwrap();
    assert_eq!(&bytes, b"return");
    drop(io);
    adoption
        .retire(Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    assert!(adoption.closed().unwrap().had_native_acquisition());
    assert!(
        adoption.users_closed().is_none(),
        "this library-only carrier has no actual private-issuer user lease"
    );
    fixture.join().await;
    assert!(fixture.registration.carrier_completion().is_none());
    fixture.assert_gate_retained().await;
    fixture
        .carrier
        .shutdown(Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
}
