use super::fixtures::*;
use std::{
    error::Error,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Notify,
    time::Instant,
};
use zero_core::{request::Destination, Address};
use zero_platform_tokio::contract::{
    component::{ComponentFactory, ComponentInstance},
    datagram::{BoxDatagramSession, DatagramRequest},
    network::{CarrierContext, UpstreamBinding, UpstreamOpener},
    scope::TaskScope,
    stream::{BoxStream, ConnectRequest, PeerConfirmation},
    BoxFuture, ChannelError, CloseReason, DeliveryState, Impact, Reason, Stage,
};
use zero_proxy::component::{DirectFactory, DirectOptions};
use zero_traits::{AddressFamily, DialPolicy};

#[derive(Default)]
struct Opener {
    calls: AtomicUsize,
    active: AtomicUsize,
    entered: Notify,
    pending: bool,
}
struct Active<'a>(&'a AtomicUsize);
impl Drop for Active<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}
impl UpstreamOpener for Opener {
    fn open_stream(
        &self,
        binding: &UpstreamBinding,
        destination: Destination,
        _: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxStream, ChannelError>> {
        assert_eq!(binding.prefix.len(), 1);
        assert_eq!(binding.prefix[0].generation.0, 8);
        assert_eq!(binding.network_generation.0, 6);
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::Relaxed);
            self.active.fetch_add(1, Ordering::Relaxed);
            let _active = Active(&self.active);
            self.entered.notify_one();
            if self.pending {
                return std::future::pending().await;
            }
            let Address::Ipv4(ip) = destination.address else {
                panic!("unexpected test destination")
            };
            TcpStream::connect((std::net::Ipv4Addr::from(ip), destination.port))
                .await
                .map(|stream| Box::pin(stream) as BoxStream)
                .map_err(|source| {
                    ChannelError::new(Stage::Dial, Reason::Internal, Impact::Session)
                        .with_source(source)
                })
        })
    }
    fn open_datagram(
        &self,
        _: &UpstreamBinding,
        _: DatagramRequest,
        _: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxDatagramSession, ChannelError>> {
        panic!("Stream role must not open datagrams")
    }
}
fn instance() -> (ComponentInstance, TaskScope) {
    let (context, scope) = build_context();
    let options = Arc::new(DirectOptions::validate(DialPolicy::default()).unwrap());
    assert!(DirectFactory.describe(&options).resources.is_empty());
    (DirectFactory.build(options, context).unwrap(), scope)
}

#[tokio::test]
async fn real_stream_preserves_reverse_traffic_after_write_half_close() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let target = Destination::new(
        Address::Ipv4([127, 0, 0, 1]),
        listener.local_addr().unwrap().port(),
    );
    let server = tokio::spawn(async move {
        let (mut io, _) = listener.accept().await.unwrap();
        let mut payload = Vec::new();
        io.read_to_end(&mut payload).await.unwrap();
        assert_eq!(payload, b"request before FIN");
        io.write_all(b"reply after FIN").await.unwrap();
        io.shutdown().await.unwrap();
    });
    let opener = Arc::new(Opener::default());
    let (mut instance, mut scope) = instance();
    assert_eq!(opener.calls.load(Ordering::Relaxed), 0);
    instance.lifecycle.start(deadline()).await.unwrap();
    assert_eq!(opener.calls.load(Ordering::Relaxed), 0);
    let stream = instance.roles.stream.as_ref().unwrap();
    let observed = Arc::new(RecordedFacts::default());
    let mut context = open_context(opener.clone(), Default::default());
    // The legacy caller has operation timers but no new total-connect timeout.
    context.deadline = None;
    context.observer = Some(observed.clone());
    let connected = stream
        .connect(
            ConnectRequest {
                destination: target,
            },
            context,
        )
        .await
        .unwrap();
    assert_eq!(connected.peer_confirmation, PeerConfirmation::Confirmed);
    use zero_platform_tokio::contract::context::ConnectFact::{CarrierReady, PeerConfirmed};
    assert_eq!(
        *observed.0.lock().unwrap(),
        [
            (Some(zero_core::component::SessionId(5)), CarrierReady),
            (Some(zero_core::component::SessionId(5)), PeerConfirmed)
        ]
    );
    let mut io = connected.io;
    io.write_all(b"request before FIN").await.unwrap();
    io.shutdown().await.unwrap();
    let mut response = Vec::new();
    io.read_to_end(&mut response).await.unwrap();
    assert_eq!(response, b"reply after FIN");
    drop(io);
    server.await.unwrap();
    assert_eq!(opener.calls.load(Ordering::Relaxed), 1);
    assert!(instance
        .lifecycle
        .shutdown(deadline())
        .await
        .unwrap()
        .is_complete());
    assert!(scope.shutdown(deadline()).await.is_complete());
}

#[tokio::test]
async fn admission_deadline_policy_and_revocation_precede_any_io() {
    let opener = Arc::new(Opener::default());
    let (mut instance, _scope) = instance();
    let stream = instance.roles.stream.as_ref().unwrap().clone();
    let request = || ConnectRequest {
        destination: destination(),
    };
    let context = || open_context(opener.clone(), Default::default());
    assert_eq!(
        stream
            .connect(request(), context())
            .await
            .err()
            .unwrap()
            .reason,
        Reason::Revoked
    );
    let error = instance
        .lifecycle
        .start(Instant::now())
        .await
        .err()
        .unwrap();
    assert_eq!(
        (error.reason, error.impact),
        (Reason::Timeout, Impact::Component)
    );
    instance.lifecycle.start(deadline()).await.unwrap();
    let mismatch = open_context(
        opener.clone(),
        DialPolicy {
            address_family: AddressFamily::OnlyIpv6,
            ..Default::default()
        },
    );
    assert_eq!(
        stream
            .connect(request(), mismatch)
            .await
            .err()
            .unwrap()
            .reason,
        Reason::StalePlan
    );
    let mut expired = context();
    expired.deadline = Some(Instant::now());
    assert_eq!(
        stream
            .connect(request(), expired)
            .await
            .err()
            .unwrap()
            .reason,
        Reason::Timeout
    );
    let cancelled = context();
    cancelled.cancel.cancel();
    assert_eq!(
        stream
            .connect(request(), cancelled)
            .await
            .err()
            .unwrap()
            .reason,
        Reason::Cancelled
    );
    instance.lifecycle.revoke(CloseReason::Shutdown);
    let error = stream.connect(request(), context()).await.err().unwrap();
    assert_eq!(error.reason, Reason::Revoked);
    assert_eq!(error.delivery, DeliveryState::NotSubmitted);
    assert_eq!(error.component.unwrap().0, 7);
    assert_eq!(error.session.unwrap().0, 5);
    assert_eq!(opener.calls.load(Ordering::Relaxed), 0);
    assert!(instance
        .lifecycle
        .shutdown(deadline())
        .await
        .unwrap()
        .is_complete());
}

#[tokio::test]
async fn owner_revocation_cancels_inflight_carrier_without_a_driver_task() {
    let opener = Arc::new(Opener {
        pending: true,
        ..Default::default()
    });
    let (mut instance, _scope) = instance();
    instance.lifecycle.start(deadline()).await.unwrap();
    let stream = instance.roles.stream.as_ref().unwrap().clone();
    let context = open_context(opener.clone(), Default::default());
    let pending = tokio::spawn(async move {
        stream
            .connect(
                ConnectRequest {
                    destination: destination(),
                },
                context,
            )
            .await
    });
    opener.entered.notified().await;
    assert_eq!(opener.active.load(Ordering::Relaxed), 1);
    instance.lifecycle.revoke(CloseReason::Shutdown);
    let error = pending.await.unwrap().err().unwrap();
    assert_eq!(error.reason, Reason::Revoked);
    assert_eq!(error.delivery, DeliveryState::Unknown);
    assert_eq!(opener.active.load(Ordering::Relaxed), 0);
    assert!(instance
        .lifecycle
        .shutdown(deadline())
        .await
        .unwrap()
        .is_complete());
}

#[tokio::test]
async fn dropped_owner_denies_surviving_role_and_carrier_error_keeps_source() {
    let opener = Arc::new(Opener::default());
    let (mut instance, _scope) = instance();
    instance.lifecycle.start(deadline()).await.unwrap();
    let role = instance.roles.stream.take().unwrap();
    // Bind, read the selected port, then close it to force a real dial failure.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let error = role
        .connect(
            ConnectRequest {
                destination: Destination::new(Address::Ipv4([127, 0, 0, 1]), port),
            },
            open_context(opener.clone(), Default::default()),
        )
        .await
        .err()
        .unwrap();
    assert!(error
        .source()
        .unwrap()
        .downcast_ref::<std::io::Error>()
        .is_some());
    assert_eq!(error.delivery, DeliveryState::Unknown);
    drop(instance.lifecycle);
    let error = role
        .connect(
            ConnectRequest {
                destination: destination(),
            },
            open_context(opener.clone(), Default::default()),
        )
        .await
        .err()
        .unwrap();
    assert_eq!(error.reason, Reason::Revoked);
    assert_eq!(opener.calls.load(Ordering::Relaxed), 1);
}
