//! An actual declared listener and its real hosted, initially Pending accept.
use super::*;
use std::{
    future::{poll_fn, Future},
    num::NonZeroUsize,
};
use tokio::sync::{oneshot, Notify};
use zero_platform_tokio::contract::{
    component::{
        BuildContext, ComponentFactory, ComponentInstance, ComponentRoles, ComponentSpec,
        ResourceDeclaration, ResourceOwner,
    },
    lifecycle::{erase_lifecycle, CloseReport, ResourceLifecycle, Unfinished},
    scope::TaskJoinWitness,
    ChannelError, CleanupFailureReceipt, CloseReason, Impact, Reason, Stage,
};

pub(super) struct State {
    pub pending: Notify,
    pub returned: Notify,
    pub address: Mutex<Option<std::net::SocketAddr>>,
    pub witness: Mutex<Option<TaskJoinWitness>>,
    pub release: Mutex<Option<oneshot::Sender<()>>>,
    pub failure: Arc<ChannelError>,
}
struct Factory {
    state: Arc<State>,
    address: zero_core::request::Destination,
    unknown: bool,
}
struct Lifecycle {
    state: Arc<State>,
    context: BuildContext,
    address: zero_core::request::Destination,
    done: Option<oneshot::Receiver<()>>,
    unknown: bool,
}
impl ComponentFactory for Factory {
    type Options = ();
    fn describe(&self, _: &()) -> ComponentSpec {
        ComponentSpec {
            owner: ResourceOwner::Component,
            roles: vec![],
            dependencies: vec![],
            static_identity: Arc::from([]),
            resources: vec![ResourceDeclaration::TcpListener {
                accepted: None,
                destination: self.address.clone(),
                max_bind_attempts: NonZeroUsize::new(1).unwrap(),
            }],
            task_limit: NonZeroUsize::new(1),
        }
    }
    fn build(&self, _: Arc<()>, context: BuildContext) -> Result<ComponentInstance, ChannelError> {
        Ok(ComponentInstance {
            roles: ComponentRoles::default(),
            lifecycle: erase_lifecycle(Lifecycle {
                state: self.state.clone(),
                context,
                address: self.address.clone(),
                done: None,
                unknown: self.unknown,
            }),
        })
    }
}
impl ResourceLifecycle for Lifecycle {
    async fn start(&mut self, _: Instant) -> Result<(), ChannelError> {
        let mut listener = self
            .context
            .resources
            .listen_tcp(self.address.clone())
            .await?;
        *self.state.address.lock().unwrap() = Some(listener.local_addr().unwrap());
        let state = self.state.clone();
        let (release, gate) = oneshot::channel();
        let (finished, done) = oneshot::channel();
        self.done = Some(done);
        *self.state.release.lock().unwrap() = Some(release);
        let (_, witness) = self
            .context
            .tasks
            .spawn_tracked(TaskClass::Critical, async move {
                let result = {
                    let read = listener.accept();
                    tokio::pin!(read);
                    poll_fn(|cx| {
                        let result = read.as_mut().poll(cx);
                        if result.is_pending() {
                            state.pending.notify_one();
                        }
                        result
                    })
                    .await
                };
                assert!(
                    result.is_err(),
                    "only the original provider stops this accept"
                );
                let _ = gate.await;
                // This is only the protocol view return. Native close and actual
                // Scope join/Host ACK remain the ordinary host's separate gates.
                drop(listener);
                let _ = finished.send(());
                Ok(())
            })
            .map_err(|error| {
                ChannelError::new(Stage::Prepare, Reason::Capacity, Impact::Component)
                    .with_source(error)
            })?;
        *self.state.witness.lock().unwrap() = Some(witness);
        Ok(())
    }
    fn revoke(&self, _: CloseReason) {}
    async fn shutdown(&mut self, deadline: Instant) -> Result<CloseReport, ChannelError> {
        let receipt = self.state.failure.cleanup_failure_receipt().unwrap();
        if !receipt.is_acknowledged() {
            let end = deadline.min(Instant::now() + Duration::from_millis(10));
            if let Some(done) = &mut self.done {
                match tokio::time::timeout_at(end, done).await {
                    Ok(Ok(())) => self.done = None,
                    Ok(Err(error)) => {
                        return Err(
                            ChannelError::new(Stage::Close, Reason::Io, Impact::Component)
                                .with_source(error),
                        )
                    }
                    Err(_) => {}
                }
            }
            self.state.returned.notify_one();
            return Err(ChannelError::retained(self.state.failure.clone()));
        }
        if let Some(done) = &mut self.done {
            tokio::time::timeout_at(deadline, done)
                .await
                .map_err(|_| ChannelError::retained(self.state.failure.clone()))?
                .map_err(|error| {
                    ChannelError::new(Stage::Close, Reason::Io, Impact::Component)
                        .with_source(error)
                })?;
            self.done = None;
        }
        Ok(if self.unknown {
            CloseReport {
                unfinished: vec![Unfinished::Resource(
                    "same protocol has no matching original close confirmation".into(),
                )],
                ..Default::default()
            }
        } else {
            CloseReport::default()
        })
    }
}
pub(super) fn slot(
    catalog: &ComponentCatalog,
    address: std::net::SocketAddr,
    unknown: bool,
) -> (Arc<ComponentSlot>, Arc<State>) {
    let source = Arc::new(std::io::Error::new(
        std::io::ErrorKind::TimedOut,
        "actual original cleanup deadline",
    ));
    let receipt = CleanupFailureReceipt::new();
    assert!(receipt.bind_payload(source.clone()));
    let state = Arc::new(State {
        pending: Notify::new(),
        returned: Notify::new(),
        address: Mutex::new(None),
        witness: Mutex::new(None),
        release: Mutex::new(None),
        failure: Arc::new(
            ChannelError::new(Stage::Close, Reason::Timeout, Impact::Component)
                .with_source(source)
                .with_cleanup_failure_receipt(receipt),
        ),
    });
    let recipe = crate::component::ComponentRecipe::from_validated(
        Arc::new(Factory {
            state: state.clone(),
            address: zero_core::request::Destination::new(
                match address.ip() {
                    std::net::IpAddr::V4(ip) => zero_core::Address::Ipv4(ip.octets()),
                    std::net::IpAddr::V6(ip) => zero_core::Address::Ipv6(ip.octets()),
                },
                address.port(),
            ),
            unknown,
        }),
        Arc::new(()),
    );
    let slot = Arc::new(ComponentSlot::from_recipe(
        catalog.allocate_id().unwrap(),
        ResourceGeneration(1),
        recipe,
        Default::default(),
        "actual-resource-cleanup",
    ));
    (slot, state)
}
