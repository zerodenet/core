use super::super::{instance::ComponentSlot, ComponentRegistry};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tokio::{
    sync::Notify,
    time::{Duration, Instant},
};
use zero_core::component::{ComponentId, ResourceGeneration};
use zero_platform_tokio::contract::{
    component::{
        BuildContext, ComponentFactory, ComponentInstance, ComponentRoles, ComponentSpec,
        ResourceOwner, RoleKind,
    },
    context::OpenContext,
    lifecycle::{erase_lifecycle, BuildError, CloseReport, ResourceLifecycle, Unfinished},
    stream::{erase_stream, ConnectRequest, Connected, StreamOutbound},
    CancellationToken, ChannelError, CloseReason, DeliveryState, Impact, Reason, Stage,
};

#[derive(Default)]
struct Counts {
    started: AtomicUsize,
    closed: AtomicUsize,
    dropped: AtomicUsize,
    entered: Notify,
}
struct Factory {
    counts: Arc<Counts>,
    wait: bool,
}
struct Owner {
    counts: Arc<Counts>,
    wait: bool,
    cancel: CancellationToken,
}
impl Drop for Owner {
    fn drop(&mut self) {
        self.counts.dropped.fetch_add(1, Ordering::Relaxed);
    }
}
impl ResourceLifecycle for Owner {
    async fn start(&mut self, _: Instant) -> Result<(), BuildError> {
        self.counts.started.fetch_add(1, Ordering::Relaxed);
        self.counts.entered.notify_one();
        if self.wait {
            self.cancel.cancelled().await;
        }
        let mut error = ChannelError::new(Stage::Prepare, Reason::InvalidConfig, Impact::Component);
        error.delivery = DeliveryState::NotSubmitted;
        Err(error)
    }
    fn revoke(&self, _: CloseReason) {
        self.cancel.cancel();
    }
    async fn shutdown(&mut self, _: Instant) -> Result<CloseReport, BuildError> {
        let attempt = self.counts.closed.fetch_add(1, Ordering::Relaxed);
        let mut report = CloseReport::default();
        if attempt == 0 {
            report
                .unfinished
                .push(Unfinished::Io("test close acknowledgement pending".into()));
        }
        Ok(report)
    }
}
struct NeverOpened;
impl StreamOutbound for NeverOpened {
    type Stream = tokio::io::DuplexStream;
    async fn connect(
        &self,
        _: ConnectRequest,
        _: OpenContext,
    ) -> Result<Connected<Self::Stream>, ChannelError> {
        panic!("failed component must not admit requests")
    }
}
impl ComponentFactory for Factory {
    type Options = ();
    fn describe(&self, _: &()) -> ComponentSpec {
        ComponentSpec {
            owner: ResourceOwner::Component,
            roles: vec![RoleKind::Stream],
            dependencies: vec![],
            static_identity: Arc::from([]),
            resources: vec![],
        }
    }
    fn build(&self, _: Arc<()>, context: BuildContext) -> Result<ComponentInstance, BuildError> {
        Ok(ComponentInstance {
            lifecycle: erase_lifecycle(Owner {
                counts: self.counts.clone(),
                wait: self.wait,
                cancel: context.cancel,
            }),
            roles: ComponentRoles {
                stream: Some(erase_stream(NeverOpened)),
                ..Default::default()
            },
        })
    }
}
fn slot(counts: Arc<Counts>, wait: bool) -> Arc<ComponentSlot> {
    use zero_traits::ProtocolMetadata;
    let mut registry = ComponentRegistry::<()>::default();
    registry
        .register(
            crate::adapters::direct::DirectAdapter::default().descriptor(),
            Factory { counts, wait },
            |_| Ok(()),
        )
        .unwrap();
    Arc::new(ComponentSlot::new(
        ComponentId(8),
        ResourceGeneration(2),
        registry.prepare("direct", &()).unwrap(),
    ))
}
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(2)
}

#[tokio::test]
async fn failed_start_keeps_owner_and_retries_unconfirmed_cleanup() {
    let counts = Arc::new(Counts::default());
    let slot = slot(counts.clone(), false);
    for _ in 0..2 {
        let error = slot.stream(deadline()).await.err().unwrap();
        assert_eq!(
            (error.reason, error.delivery),
            (Reason::InvalidConfig, DeliveryState::NotSubmitted)
        );
    }
    assert_eq!(counts.started.load(Ordering::Relaxed), 1);
    assert_eq!(counts.dropped.load(Ordering::Relaxed), 0);
    assert!(!slot.shutdown(deadline()).await.unwrap().is_complete());
    assert_eq!(counts.dropped.load(Ordering::Relaxed), 0);
    assert!(slot.shutdown(deadline()).await.unwrap().is_complete());
    assert_eq!(counts.closed.load(Ordering::Relaxed), 2);
    drop(slot);
    assert_eq!(counts.dropped.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn shutdown_waits_for_in_progress_start_to_retain_its_cleanup_owner() {
    let counts = Arc::new(Counts::default());
    let slot = slot(counts.clone(), true);
    let starting = slot.clone();
    let task = tokio::spawn(async move { starting.stream(deadline()).await });
    counts.entered.notified().await;
    let report = slot.shutdown(deadline()).await.unwrap();
    assert!(!report.is_complete());
    assert!(task.await.unwrap().is_err());
    assert_eq!(counts.started.load(Ordering::Relaxed), 1);
    assert_eq!(counts.closed.load(Ordering::Relaxed), 1);
    assert!(slot.shutdown(deadline()).await.unwrap().is_complete());
}
