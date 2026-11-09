//! A real child metadata refund synchronously reenters its original parent book.
use super::*;
use std::{
    future::Future,
    sync::{
        atomic::{AtomicUsize, Ordering},
        OnceLock,
    },
    task::{Context, Poll, Wake, Waker},
    time::Duration,
};
use zero_core::{
    component::{ComponentId, ComponentInstanceId, NetworkGeneration, ResourceGeneration},
    request::Destination,
    Address,
};
use zero_platform_tokio::{
    contract::{
        component::ResourceDeclaration,
        lifecycle::{CloseReport, DynChannelControl, ResourceLifecycle},
        network::{PendingCarrierObserver, UpstreamBinding, UpstreamOpener, UpstreamTransport},
        scope::TaskScope,
        BoxFuture, CloseReason,
    },
    controlled_carrier::{CarrierOriginalTotals, CarrierOwner, ProtocolOriginalChildren},
    controlled_datagram::{metadata::MetadataLimits, metadata_pool::MetadataPoolOwner},
};
struct UnstartedControl;
impl DynChannelControl for UnstartedControl {
    fn cancel(&self, _: CloseReason) {}
    fn wait_closed(&mut self, _: Instant) -> BoxFuture<'_, Result<CloseReport, ChannelError>> {
        Box::pin(async { Ok(Default::default()) })
    }
}
struct Canonical(OnceLock<PendingCarrierObserver>);
impl UpstreamOpener for Canonical {
    // This exact canonical observer has no IO authority. The original Root
    // produces KnownNoNative only from its real unstarted preparation ledger.
    fn open_stream(
        &self,
        _: &UpstreamBinding,
        _: Destination,
        _: zero_platform_tokio::contract::network::CarrierContext,
    ) -> BoxFuture<'_, Result<zero_platform_tokio::contract::stream::BoxStream, ChannelError>> {
        Box::pin(async { Err(no_io_authority()) })
    }
    fn open_datagram(
        &self,
        _: &UpstreamBinding,
        _: zero_platform_tokio::contract::datagram::DatagramRequest,
        _: zero_platform_tokio::contract::network::CarrierContext,
    ) -> BoxFuture<
        '_,
        Result<zero_platform_tokio::contract::datagram::BoxDatagramSession, ChannelError>,
    > {
        Box::pin(async { Err(no_io_authority()) })
    }
    fn protocol_original_observer(&self) -> Option<PendingCarrierObserver> {
        self.0.get().cloned()
    }
}
fn no_io_authority() -> ChannelError {
    use zero_platform_tokio::contract::{DeliveryState, Impact, Reason, Stage};
    let mut error = ChannelError::new(Stage::Prepare, Reason::Unsupported, Impact::Component);
    error.delivery = DeliveryState::NotSubmitted;
    error
}
struct Reenter {
    book: Weak<Budget>,
    calls: AtomicUsize,
}
impl Wake for Reenter {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        let book = self
            .book
            .upgrade()
            .expect("same parent still owns the wait");
        assert!(
            book.records.try_lock().is_ok(),
            "actual child-header refund must not run below parent census lock"
        );
        assert!(book.all_returned());
        self.calls.fetch_add(1, Ordering::AcqRel);
    }
}
#[tokio::test]
async fn actual_child_metadata_refund_reenters_parent_budget_only_after_original_record_removal() {
    tokio::time::timeout(Duration::from_secs(3), async {
        let one = NonZeroUsize::new(1).unwrap();
        let instance = ComponentInstanceId {
            component: ComponentId(431),
            generation: ResourceGeneration(1),
        };
        let totals = CarrierOriginalTotals::new(one);
        let packet = Arc::new(zero_tun::packet_memory::PacketReturnTotals::default());
        let memory = packet.driver_metadata_view();
        let mut scope = TaskScope::suspended_hosted(tokio::runtime::Handle::current(), Some(one));
        scope.enable_registration().unwrap();
        let tasks = scope.registrar();
        let host = Arc::new(CancellationToken::new());
        let endpoint = Destination::new(Address::Ipv4([127, 0, 0, 1]), 43100);
        let binding = UpstreamBinding {
            prefix: vec![],
            dial: Default::default(),
            network_generation: NetworkGeneration(1),
        };
        let limits = MetadataLimits {
            main_sockets: one,
            probe_sockets: one,
            probe_attempts_per_initialization: NonZeroUsize::new(4).unwrap(),
            concurrent_send_calls: one,
            write_pollers: one,
        };
        let metadata = MetadataPoolOwner::new(instance, one, limits, &host).unwrap();
        metadata
            .registration()
            .bind_declared_with_accounting(
                &ResourceDeclaration::ControlledMetadataCarrier {
                    endpoint: endpoint.clone(),
                    dial: Default::default(),
                    ports: vec![],
                    limits,
                    max_origins: one,
                },
                &tasks,
                &host,
                &totals,
                &memory,
            )
            .unwrap();
        metadata.start().unwrap();
        // This is a genuine pre-IO Root reservation: no Aux/native was issued.
        let (preparation, retirement) = metadata
            .grants()
            .prepare_protocol_original(&tasks, Box::new(UnstartedControl).into())
            .unwrap_or_else(|rejected| panic!("actual reservation failed: {}", rejected.error));
        let canonical = Arc::new(Canonical(OnceLock::new()));
        let upstream = UpstreamTransport::new(binding.clone(), canonical.clone());
        let observer = upstream.pending_carrier_observer();
        let _ = canonical.0.set(observer.clone());
        let child = preparation
            .protocol_original(&upstream, observer, &endpoint, &retirement)
            .unwrap();
        let child_origin = child.metadata().unwrap().clone();
        let children = ProtocolOriginalChildren::new(memory.clone(), 1);
        children.retain(child).unwrap();
        let mut parent = CarrierOwner::new(instance, one, &CancellationToken::new());
        parent
            .start(Instant::now() + Duration::from_secs(2))
            .await
            .unwrap();
        let parent_grant = parent.grants().reserve().unwrap();
        let physical = Arc::new(Physical {
            owner: Weak::new(),
            binding,
            pending: Mutex::new(vec![]),
            protocols: Some(children),
            adoption: parent_grant.adoption(),
            stop: CancellationToken::new(),
            view: Mutex::new(None),
            witness: Mutex::new(None),
            received: Mutex::new(None),
            accepted: std::sync::atomic::AtomicBool::new(false),
            done: std::sync::atomic::AtomicBool::new(false),
        });
        let book = Budget::new(one);
        book.records.lock().unwrap().push(Arc::new(Record {
            instance,
            state: Mutex::new(State {
                physical: Some(physical),
                ..Default::default()
            }),
            stop: CancellationToken::new(),
            changed: Default::default(),
        }));
        let observed = memory.snapshot();
        let mut notification = Box::pin(memory.changed_since(observed));
        let reenter = Arc::new(Reenter {
            book: Arc::downgrade(&book),
            calls: AtomicUsize::new(0),
        });
        let waker = Waker::from(reenter.clone());
        assert!(notification
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending());
        book.harvest();
        assert!(
            reenter.calls.load(Ordering::Acquire) > 0,
            "actual original header refund must notify the same Root"
        );
        assert!(book.records.lock().unwrap().is_empty());
        assert!(memory.snapshot().occupied.bytes() < observed.occupied.bytes());
        assert_eq!(
            totals.snapshot().charged,
            1,
            "metadata return is not global original close"
        );
        assert!(child_origin.closed().is_none() && !retirement.is_settled());
        drop(notification);
        drop(preparation);
        retirement.request_retirement();
        std::future::poll_fn(|cx| {
            totals.poll_cancelled_retirements(cx);
            if totals.snapshot().charged == 0 {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await;
        let closed = child_origin.closed().unwrap();
        assert!(closed.matches(&child_origin) && !closed.had_native_acquisition());
        assert!(retirement.is_settled());
        drop(parent_grant);
        assert!(parent
            .shutdown(Instant::now() + Duration::from_secs(2))
            .await
            .unwrap()
            .is_complete());
        scope.begin_shutdown();
        assert!(scope.is_drained());
    })
    .await
    .unwrap();
}
