use super::*;
use crate::{component::direct_bridge::Invocation, Proxy};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Mutex,
};
use tokio::{
    io::DuplexStream,
    time::{Duration, Instant},
};
use zero_core::{component::SessionId, Network, ProtocolType, Session};
use zero_platform_tokio::contract::{
    context::RequestIdentity,
    network::UpstreamTransport,
    stream::{erase_stream, Connected, PeerConfirmation, StreamOutbound},
    CancellationToken,
};

struct Capture {
    calls: AtomicUsize,
    seen: Mutex<Option<(Destination, RequestIdentity, Option<Instant>)>>,
}
struct RecordingRole(Arc<Capture>);
impl StreamOutbound for RecordingRole {
    type Stream = DuplexStream;
    async fn connect(
        &self,
        request: ConnectRequest,
        context: OpenContext,
    ) -> Result<Connected<DuplexStream>, ChannelError> {
        self.0.calls.fetch_add(1, Ordering::SeqCst);
        *self.0.seen.lock().unwrap() =
            Some((request.destination, context.identity, context.deadline));
        let (io, _peer) = tokio::io::duplex(32);
        Ok(Connected {
            io,
            peer_confirmation: PeerConfirmation::Confirmed,
        })
    }
}
struct NoRoot;
impl UpstreamOpener for NoRoot {
    fn open_stream(
        &self,
        _: &UpstreamBinding,
        _: Destination,
        _: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxStream, ChannelError>> {
        Box::pin(async { panic!("fixture must not bypass its selected role") })
    }
    fn open_datagram(
        &self,
        _: &UpstreamBinding,
        _: DatagramRequest,
        _: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxDatagramSession, ChannelError>> {
        Box::pin(async { panic!("fixture must not acquire datagram I/O") })
    }
}
fn fixture() -> (PrefixOpener, Arc<Capture>) {
    let proxy = Proxy::new(
        zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap(),
    )
    .unwrap();
    let mut session = Session::new(
        0,
        zero_core::Address::Ipv4([203, 0, 113, 9]),
        443,
        Network::Tcp,
        ProtocolType::new("test"),
    );
    proxy
        .engine()
        .prepare_session(&mut session, "incoming")
        .unwrap();
    let services = proxy
        .tcp_runtime_services()
        .for_inbound("incoming".into())
        .with_business_identity(&session)
        .unwrap();
    let execution = services.execution();
    let Invocation { identity, .. } = execution.invocation.unwrap();
    let role = execution
        .components
        .unwrap()
        .direct(None)
        .unwrap()
        .role_id();
    let root = UpstreamBinding {
        prefix: vec![],
        dial: Default::default(),
        network_generation: zero_core::component::NetworkGeneration(
            services.upstream().network_generation(),
        ),
    };
    let binding = UpstreamBinding {
        prefix: vec![role],
        ..root.clone()
    };
    let capture = Arc::new(Capture {
        calls: AtomicUsize::new(0),
        seen: Mutex::new(None),
    });
    let endpoint = Destination::new(zero_core::Address::Ipv4([127, 0, 0, 1]), 8080);
    let opener = PrefixOpener {
        role: erase_stream(RecordingRole(capture.clone())),
        context: OpenContext {
            identity,
            deadline: None,
            cancel: CancellationToken::new(),
            observer: None,
            upstream: UpstreamTransport::new(root, Arc::new(NoRoot)),
        },
        expected_binding: binding,
        expected_endpoint: endpoint,
        request_destination: Destination::new(zero_core::Address::Domain("127.0.0.1".into()), 8080),
        network: services.upstream(),
        observer: None,
        health: None,
    };
    (opener, capture)
}
fn carrier(opener: &PrefixOpener) -> CarrierContext {
    CarrierContext {
        session: opener.context.identity.session(),
        deadline: None,
        cancel: CancellationToken::new(),
    }
}

#[tokio::test]
async fn bound_prefix_rejects_wrong_role_generation_endpoint_dial_network_and_session_before_io() {
    let (opener, capture) = fixture();
    let mut bindings = vec![];
    let mut empty = opener.expected_binding.clone();
    empty.prefix.clear();
    bindings.push(empty);
    let mut generation = opener.expected_binding.clone();
    generation.prefix[0].generation = generation.prefix[0].generation.checked_next().unwrap();
    bindings.push(generation);
    let mut network = opener.expected_binding.clone();
    network.network_generation.0 += 1;
    bindings.push(network);
    let mut dial = opener.expected_binding.clone();
    dial.dial.address_family = zero_traits::AddressFamily::OnlyIpv4;
    bindings.push(dial);
    for binding in bindings {
        let error = opener
            .open_stream(&binding, opener.expected_endpoint.clone(), carrier(&opener))
            .await
            .err()
            .expect("wrong binding must fail");
        assert_eq!(error.reason, Reason::StalePlan);
        assert_eq!(error.session, opener.context.identity.session());
        assert_eq!(
            error.component,
            Some(opener.expected_binding.prefix[0].component)
        );
        assert_eq!(error.delivery, DeliveryState::NotSubmitted);
    }
    let mut wrong_endpoint = opener.expected_endpoint.clone();
    wrong_endpoint.port += 1;
    assert!(opener
        .open_stream(&opener.expected_binding, wrong_endpoint, carrier(&opener))
        .await
        .is_err());
    let mut foreign = carrier(&opener);
    foreign.session = Some(SessionId(foreign.session.unwrap().0 + 1));
    assert!(opener
        .open_stream(
            &opener.expected_binding,
            opener.expected_endpoint.clone(),
            foreign
        )
        .await
        .is_err());
    assert_eq!(capture.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn bound_prefix_preserves_actual_business_facts_named_node_target_and_earliest_deadline() {
    let (mut opener, capture) = fixture();
    let expected = match &opener.context.identity {
        RequestIdentity::Business(facts) => facts.clone(),
        _ => panic!("real business identity"),
    };
    let deadline = Instant::now() + Duration::from_secs(5);
    opener.context.deadline = Some(deadline);
    let mut carrier = carrier(&opener);
    carrier.deadline = Some(deadline + Duration::from_secs(5));
    let _io = opener
        .open_stream(
            &opener.expected_binding,
            opener.expected_endpoint.clone(),
            carrier,
        )
        .await
        .unwrap();
    let seen = capture.seen.lock().unwrap();
    let (destination, identity, actual_deadline) = seen.as_ref().unwrap();
    assert_eq!(destination, &opener.request_destination);
    assert_eq!(*actual_deadline, Some(deadline));
    let RequestIdentity::Business(actual) = identity else {
        panic!("identity was rewritten")
    };
    assert!(Arc::ptr_eq(actual, &expected));
    assert_ne!(actual.session.0, 0);
}

#[tokio::test]
async fn unrelated_carrier_token_cannot_replace_cancelled_parent_fence() {
    let (opener, capture) = fixture();
    opener.context.cancel.cancel();
    let error = opener
        .open_stream(
            &opener.expected_binding,
            opener.expected_endpoint.clone(),
            carrier(&opener),
        )
        .await
        .err()
        .expect("parent must cancel");
    assert_eq!(error.reason, Reason::Cancelled);
    assert_eq!(error.delivery, DeliveryState::Unknown);
    assert_eq!(capture.calls.load(Ordering::SeqCst), 0);
}

#[cfg(feature = "raw-ip-runtime")]
#[tokio::test]
async fn network_change_after_prefix_connect_keeps_unknown_delivery_and_actual_owner() {
    struct NetworkChange(zero_platform_tokio::EgressInterfaceControl);
    impl StreamOutbound for NetworkChange {
        type Stream = DuplexStream;
        async fn connect(
            &self,
            _: ConnectRequest,
            _: OpenContext,
        ) -> Result<Connected<DuplexStream>, ChannelError> {
            // This only invalidates the fixture's in-memory network epoch.
            self.0.invalidate_network();
            let (io, _peer) = tokio::io::duplex(32);
            Ok(Connected {
                io,
                peer_confirmation: PeerConfirmation::Confirmed,
            })
        }
    }
    let (mut opener, _capture) = fixture();
    opener.role = erase_stream(NetworkChange(opener.network.egress_control()));
    let error = opener
        .open_stream(
            &opener.expected_binding,
            opener.expected_endpoint.clone(),
            carrier(&opener),
        )
        .await
        .err()
        .expect("a stale connected prefix must be dropped");
    assert_eq!(error.reason, Reason::StalePlan);
    assert_eq!(error.delivery, DeliveryState::Unknown);
    assert_eq!(
        error.component,
        Some(opener.expected_binding.prefix[0].component)
    );
    assert_eq!(error.session, opener.context.identity.session());
    assert!(!error.retry_permitted());
}
