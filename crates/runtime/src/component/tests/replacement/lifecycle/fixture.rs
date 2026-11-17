use super::*;
use std::{net::SocketAddr, num::NonZeroUsize};
use zero_core::component::NetworkGeneration;
use zero_platform_tokio::{
    contract::{
        component::{
            BuildContext, ComponentFactory, ComponentInstance, ComponentRoles, ComponentSpec,
            GrantedMetadataListener, ResourceDeclaration, ResourceOwner,
        },
        lifecycle::{erase_lifecycle, BuildError, CloseReport, ResourceLifecycle},
        scope::TaskJoinWitness,
        CloseReason,
    },
    controlled_datagram::{
        metadata::{MetadataDatagramTx, MetadataLimits},
        metadata_pool::{MetadataPoolOrigin, MetadataPoolOwner, MetadataPoolPermit},
    },
};
fn n(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap()
}
fn limits() -> MetadataLimits {
    MetadataLimits {
        main_sockets: n(1),
        probe_sockets: n(1),
        probe_attempts_per_initialization: n(1),
        concurrent_send_calls: n(1),
        write_pollers: n(1),
    }
}
pub(super) struct State {
    stopping: [AtomicBool; 2],
    changed: tokio::sync::Notify,
    pub(super) originals: Mutex<[Option<MetadataPoolOrigin>; 2]>,
    pub(super) transferred: Mutex<Option<MetadataDatagramTx>>,
}
struct Factory {
    index: usize,
    peer: SocketAddr,
    state: Arc<State>,
    transfer: bool,
}
struct Owner {
    index: usize,
    state: Arc<State>,
    transfer: bool,
    context: BuildContext,
    owner: MetadataPoolOwner,
    permit: Option<MetadataPoolPermit>,
    grant: Option<GrantedMetadataListener>,
    caller: Option<TaskJoinWitness>,
    release: Option<tokio::sync::oneshot::Sender<()>>,
}
impl ComponentFactory for Factory {
    type Options = ();
    fn describe(&self, _: &()) -> ComponentSpec {
        ComponentSpec {
            owner: ResourceOwner::Component,
            roles: vec![],
            dependencies: vec![],
            static_identity: Arc::from([]),
            task_limit: Some(n(4)),
            resources: vec![ResourceDeclaration::ControlledMetadataDatagram {
                endpoint: self.peer,
                dial: Default::default(),
                limits: limits(),
                max_origins: n(1),
            }],
        }
    }
    fn build(&self, _: Arc<()>, context: BuildContext) -> Result<ComponentInstance, BuildError> {
        let owner = MetadataPoolOwner::new(context.instance(), n(1), limits(), &context.cancel)?;
        Ok(ComponentInstance {
            roles: ComponentRoles::default(),
            lifecycle: erase_lifecycle(Owner {
                index: self.index,
                state: self.state.clone(),
                transfer: self.transfer,
                context,
                owner,
                permit: None,
                grant: None,
                caller: None,
                release: None,
            }),
        })
    }
}
impl ResourceLifecycle for Owner {
    async fn start(&mut self, end: Instant) -> Result<(), BuildError> {
        self.context.resources.register_metadata_pool(
            0,
            self.owner.registration(),
            &self.context.tasks,
        )?;
        self.owner.start()?;
        let (release, wait) = tokio::sync::oneshot::channel();
        let (_, caller) = self
            .context
            .tasks
            .spawn_tracked(TaskClass::Auxiliary, async move {
                let _ = wait.await;
                Ok(())
            })
            .map_err(|source| {
                zero_platform_tokio::contract::ChannelError::new(
                    zero_platform_tokio::contract::Stage::Prepare,
                    zero_platform_tokio::contract::Reason::Capacity,
                    zero_platform_tokio::contract::Impact::Component,
                )
                .with_source(source)
            })?;
        self.release = Some(release);
        self.caller = Some(caller.clone());
        self.permit = Some(self.owner.grants().reserve(&caller)?);
        let permit = self.permit.as_ref().unwrap();
        let egress = zero_platform_tokio::EgressInterfaceControl::default();
        self.grant = Some(
            permit
                .open(
                    &caller,
                    &egress,
                    NetworkGeneration(egress.generation()),
                    end,
                )
                .unwrap_or_else(|error| {
                    panic!(
                        "actual metadata original failed: {error:?}; source: {}",
                        crate::component::tests::source::<std::io::Error>(&error)
                    )
                }),
        );
        self.state.originals.lock().unwrap()[self.index] = Some(permit.origin());
        if self.transfer {
            *self.state.transferred.lock().unwrap() = Some(self.grant.as_ref().unwrap().tx.clone());
        }
        Ok(())
    }
    fn revoke(&self, _: CloseReason) {
        self.owner.revoke();
    }
    async fn shutdown(&mut self, end: Instant) -> Result<CloseReport, BuildError> {
        self.state.stopping[self.index].store(true, Ordering::Release);
        self.state.changed.notify_waiters();
        // Models mutually retained physical drivers. This is only coordination;
        // each member still closes its own original and waits its actual ACK.
        loop {
            let changed = self.state.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self
                .state
                .stopping
                .iter()
                .all(|flag| flag.load(Ordering::Acquire))
            {
                break;
            }
            changed.await;
        }
        if let Some(release) = self.release.take() {
            let _ = release.send(());
        }
        if let Some(mut grant) = self.grant.take() {
            let mut claim = grant.users_claim.take();
            drop(grant);
            self.permit
                .as_ref()
                .unwrap()
                .return_unbound(&mut claim, &self.context.tasks)?;
        }
        // A retained view is an unfinished provider observation, not a new
        // protocol failure. Keep the actual permit for the next close pass.
        if let Some(permit) = &self.permit {
            if permit.close(None, &self.context.tasks).is_err() {
                return self.owner.shutdown();
            }
        }
        if let Some(caller) = &self.caller {
            tokio::time::timeout_at(end, caller.wait_registration_released())
                .await
                .map_err(|_| {
                    crate::component::instance::invalid(
                        Reason::Timeout,
                        "actual initializer ACK pending",
                    )
                })?;
        }
        self.owner.shutdown_after_registration_ack(end).await
    }
}
pub(super) fn coupled(
    catalog: &ComponentCatalog,
    peer: SocketAddr,
    transfer: bool,
) -> (Arc<ComponentPlan>, Arc<State>) {
    let state = Arc::new(State {
        stopping: std::array::from_fn(|_| AtomicBool::new(false)),
        changed: Default::default(),
        originals: Mutex::new([None, None]),
        transferred: Mutex::new(None),
    });
    let owner = catalog.allocate_id().unwrap();
    let slots = (0..2)
        .map(|index| {
            let id = if index == 0 {
                owner
            } else {
                catalog.allocate_id().unwrap()
            };
            let recipe = crate::component::ComponentRecipe::from_validated(
                Arc::new(Factory {
                    index,
                    peer,
                    state: state.clone(),
                    transfer: transfer && index == 0,
                }),
                Arc::new(()),
            )
            .with_lifecycle_owner(owner);
            Arc::new(ComponentSlot::from_recipe(
                id,
                ResourceGeneration(1),
                recipe,
                Default::default(),
                "coupled_original",
            ))
        })
        .collect();
    (plan(slots), state)
}
