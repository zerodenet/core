use super::super::catalog::ComponentCatalog;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, DuplexStream},
    time::{Duration, Instant},
};
use zero_config::{OutboundAddressFamily, RuntimeConfig};
use zero_core::{
    component::{InboundId, NetworkGeneration, RoleId, SessionId},
    request::{Destination, RequestFacts, ServiceOrigin, ServicePurpose, ServiceRequestFacts},
    Address, ProtocolType,
};
use zero_platform_tokio::contract::{
    context::{OpenContext, RequestIdentity},
    datagram::{BoxDatagramSession, DatagramRequest},
    network::{CarrierContext, UpstreamBinding, UpstreamOpener, UpstreamTransport},
    stream::{BoxStream, ConnectRequest, DynStreamOutbound},
    BoxFuture, CancellationToken, ChannelError, DeliveryState, Reason,
};

#[derive(Default)]
struct Root {
    calls: AtomicUsize,
    peers: Mutex<Vec<DuplexStream>>,
}
impl UpstreamOpener for Root {
    fn open_stream(
        &self,
        _: &UpstreamBinding,
        _: Destination,
        _: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxStream, ChannelError>> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let (io, peer) = tokio::io::duplex(32);
            self.peers.lock().unwrap().push(peer);
            Ok(Box::pin(io) as BoxStream)
        })
    }
    fn open_datagram(
        &self,
        _: &UpstreamBinding,
        _: DatagramRequest,
        _: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxDatagramSession, ChannelError>> {
        Box::pin(async { panic!("Stream fixture must not acquire datagram I/O") })
    }
}
pub(super) fn config() -> Arc<RuntimeConfig> {
    Arc::new(RuntimeConfig::parse(r#"{"outbounds":[{"tag":"selected","protocol":{"type":"direct"}}],"route":{"final":{"type":"direct"}}}"#).unwrap())
}
pub(super) fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(2)
}
fn identity(business: bool) -> RequestIdentity {
    if !business {
        return RequestIdentity::Service(Arc::new(ServiceRequestFacts {
            origin: ServiceOrigin::Runtime,
            purpose: ServicePurpose::Bootstrap,
        }));
    }
    RequestIdentity::Business(Arc::new(RequestFacts {
        session: SessionId(19),
        inbound: RoleId::Inbound(InboundId(1)),
        original_source: None,
        effective_source: None,
        local_endpoint: None,
        original_target: None,
        target: None,
        target_source: None,
        subject: None,
        peer: None,
        protocol: ProtocolType::new("test"),
        sni: None,
        process: None,
    }))
}
async fn open(
    role: &Arc<dyn DynStreamOutbound>,
    root: &Arc<Root>,
    business: bool,
) -> Result<BoxStream, ChannelError> {
    open_with_policy(role, root, business, Default::default()).await
}
async fn open_with_policy(
    role: &Arc<dyn DynStreamOutbound>,
    root: &Arc<Root>,
    business: bool,
    policy: zero_traits::DialPolicy,
) -> Result<BoxStream, ChannelError> {
    let connected = role
        .connect(
            ConnectRequest {
                destination: Destination::new(Address::Ipv4([203, 0, 113, 1]), 80),
            },
            OpenContext {
                identity: identity(business),
                deadline: Some(deadline()),
                cancel: CancellationToken::new(),
                observer: None,
                upstream: UpstreamTransport::new(
                    UpstreamBinding {
                        prefix: vec![],
                        dial: policy,
                        network_generation: NetworkGeneration(1),
                    },
                    root.clone(),
                ),
            },
        )
        .await?;
    Ok(connected.io)
}

#[tokio::test]
async fn cold_validation_never_grants_admission_and_failed_commit_keeps_current_closed() {
    let catalog = ComponentCatalog::default();
    let config = config();
    let plan = catalog.prepare(config.clone()).unwrap();
    let role = plan
        .direct(Some("selected"))
        .unwrap()
        .stream(deadline())
        .await
        .unwrap();
    let root = Arc::new(Root::default());
    assert_eq!(
        open(&role, &root, false).await.err().unwrap().reason,
        Reason::StalePlan
    );
    assert!(
        catalog.commit(&config, || Ok(())).is_err(),
        "cold prepare is not a start acknowledgement"
    );
    catalog.start(config.clone(), deadline()).await.unwrap();
    open(&role, &root, false).await.unwrap();
    let error = open(&role, &root, true).await.err().unwrap();
    assert_eq!(error.reason, Reason::StalePlan);
    assert_eq!(error.delivery, DeliveryState::NotSubmitted);
    assert_eq!(root.calls.load(Ordering::SeqCst), 1);
    assert!(catalog
        .commit::<()>(&config, || Err("actual commit failed".into()))
        .is_err());
    assert!(open(&role, &root, true).await.is_err());
    catalog.commit(&config, || Ok(())).unwrap();
    open(&role, &root, true).await.unwrap();
    catalog.stop_business();
    assert!(open(&role, &root, true).await.is_err());
    open(&role, &root, false).await.unwrap();
    catalog.shutdown(deadline()).await.unwrap();
    assert!(open(&role, &root, false).await.is_err());
}

#[tokio::test]
async fn exact_candidate_survives_cold_cache_replacement_and_cached_old_handles_are_fenced() {
    let catalog = ComponentCatalog::default();
    let original = config();
    catalog.start(original.clone(), deadline()).await.unwrap();
    catalog.commit(&original, || Ok(())).unwrap();
    let old = catalog
        .prepare(original.clone())
        .unwrap()
        .direct(Some("selected"))
        .unwrap()
        .stream(deadline())
        .await
        .unwrap();
    let root = Arc::new(Root::default());
    let mut transferred = open(&old, &root, true).await.unwrap();

    let mut candidate = (*original).clone();
    candidate.outbounds[0].dial.address_family = OutboundAddressFamily::OnlyIpv4;
    let candidate = Arc::new(candidate);
    catalog.start(candidate.clone(), deadline()).await.unwrap();
    let selected = catalog
        .prepare(candidate.clone())
        .unwrap()
        .direct(Some("selected"))
        .unwrap()
        .stream(deadline())
        .await
        .unwrap();
    let mut other = (*original).clone();
    other.outbounds[0].dial.address_family = OutboundAddressFamily::OnlyIpv6;
    let other = Arc::new(other);
    let other_plan = catalog.prepare(other.clone()).unwrap();
    let cold_role = other_plan
        .direct(Some("selected"))
        .unwrap()
        .stream(deadline())
        .await
        .unwrap();
    // Cold alternatives may propose the same next generation. Only the actual
    // host-selected owner receives the grant; matching numeric IDs is insufficient.
    assert!(open(&cold_role, &root, false).await.is_err());
    assert!(catalog.commit(&other, || Ok(())).is_err());
    catalog.commit(&candidate, || Ok(())).unwrap();
    assert!(Arc::ptr_eq(
        &selected,
        &catalog
            .prepare(candidate.clone())
            .unwrap()
            .direct(Some("selected"))
            .unwrap()
            .stream(deadline())
            .await
            .unwrap()
    ));
    assert!(open(&old, &root, true).await.is_err());
    assert!(open(&old, &root, false).await.is_err());
    assert!(open(&cold_role, &root, true).await.is_err());
    open_with_policy(
        &selected,
        &root,
        true,
        candidate.outbounds[0].dial.to_policy(),
    )
    .await
    .unwrap();
    let historical = catalog
        .prepare(original)
        .unwrap()
        .direct(Some("selected"))
        .unwrap()
        .stream(deadline())
        .await
        .unwrap();
    assert!(open(&historical, &root, false).await.is_err());
    open_with_policy(
        &selected,
        &root,
        true,
        candidate.outbounds[0].dial.to_policy(),
    )
    .await
    .unwrap();
    // Publishing does not close transferred I/O or pretend to own its session.
    let mut peer = root.peers.lock().unwrap().remove(0);
    peer.write_all(b"x").await.unwrap();
    assert_eq!(transferred.read_u8().await.unwrap(), b'x');
    catalog.shutdown(deadline()).await.unwrap();
}
