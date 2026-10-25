//! Real TaskScope exits and a declared pre-owned listener pending provider proof.
use super::*;
use std::{future::Future, net::SocketAddr};
use tokio::io::{AsyncReadExt, DuplexStream};
use zero_platform_tokio::{
    contract::{
        component::{
            BuildContext, ComponentFactory, ComponentInstance, ComponentRoles, ComponentSpec,
            ResourceOwner,
        },
        lifecycle::{
            erase_lifecycle, BuildError, ChannelControl, CloseReport, ResourceLifecycle, TaskClass,
        },
        scope::TaskRegistrar,
        CancellationToken, CloseReason,
    },
    ControlledTcpListener, ControlledTcpListenerControl,
};

pub(super) struct State {
    pub(super) address: Option<SocketAddr>,
    native: Mutex<Option<(ControlledTcpListener, ControlledTcpListenerControl)>>,
    pub(super) peer: Mutex<Option<DuplexStream>>,
    pub(super) pending: tokio::sync::Notify,
    pub(super) task: Mutex<Option<zero_core::component::TaskId>>,
    pub(super) native_task: Mutex<Option<tokio::task::Id>>,
}
struct Factory {
    state: Arc<State>,
    panic: bool,
}
struct Owner {
    state: Arc<State>,
    control: Option<ControlledTcpListenerControl>,
    view: Option<ControlledTcpListener>,
    gate: Option<DuplexStream>,
    tasks: TaskRegistrar,
    panic: bool,
}
impl ComponentFactory for Factory {
    type Options = ();
    fn describe(&self, _: &()) -> ComponentSpec {
        let resources = self
            .state
            .address
            .map(|address| {
                vec![
                    zero_platform_tokio::contract::component::ResourceDeclaration::TcpListener {
                        accepted: None,
                        destination: zero_core::request::Destination::new(
                            zero_core::Address::Ipv4([127, 0, 0, 1]),
                            address.port(),
                        ),
                        max_bind_attempts: std::num::NonZeroUsize::new(1).unwrap(),
                    },
                ]
            })
            .unwrap_or_default();
        ComponentSpec {
            owner: ResourceOwner::Endpoint("preowned.listener.fixture".into()),
            roles: vec![],
            dependencies: vec![],
            static_identity: Arc::from([]),
            // A pre-owned native control still has a real declaration. Its
            // protocol report cannot substitute for host provider physical proof.
            resources,
            task_limit: self.panic.then(|| std::num::NonZeroUsize::new(1).unwrap()),
        }
    }
    fn build(&self, _: Arc<()>, context: BuildContext) -> Result<ComponentInstance, BuildError> {
        let (view, control) = match self.state.native.lock().unwrap().take() {
            Some((view, control)) => (Some(view), Some(control)),
            None => (None, None),
        };
        let (gate, peer) = tokio::io::duplex(8);
        *self.state.peer.lock().unwrap() = Some(peer);
        Ok(ComponentInstance {
            roles: ComponentRoles::default(),
            lifecycle: erase_lifecycle(Owner {
                state: self.state.clone(),
                view,
                control,
                gate: Some(gate),
                tasks: context.tasks,
                panic: self.panic,
            }),
        })
    }
}
impl ResourceLifecycle for Owner {
    async fn start(&mut self, _: tokio::time::Instant) -> Result<(), BuildError> {
        if self.panic {
            let mut gate = self.gate.take().unwrap();
            let state = self.state.clone();
            let id = self
                .tasks
                .spawn(TaskClass::Critical, async move {
                    *state.native_task.lock().unwrap() = Some(tokio::task::id());
                    gate.read_u8().await.unwrap();
                    panic!("actual retained component critical failure");
                })
                .unwrap();
            *self.state.task.lock().unwrap() = Some(id);
        }
        Ok(())
    }
    fn revoke(&self, reason: CloseReason) {
        if let Some(control) = &self.control {
            control.cancel(reason);
        }
    }
    async fn shutdown(
        &mut self,
        deadline: tokio::time::Instant,
    ) -> Result<CloseReport, BuildError> {
        if let Some(gate) = &mut self.gate {
            let ended = {
                let read = gate.read_u8();
                tokio::pin!(read);
                std::future::poll_fn(|cx| {
                    let result = read.as_mut().poll(cx);
                    if result.is_pending() {
                        self.state.pending.notify_one();
                    }
                    result
                })
                .await
            };
            assert_eq!(ended.unwrap_err().kind(), std::io::ErrorKind::UnexpectedEof);
            self.gate = None;
        }
        let report = match &mut self.control {
            Some(control) => control.wait_closed(deadline).await?,
            None => CloseReport::default(),
        };
        drop(self.view.take());
        Ok(report)
    }
}
pub(super) async fn task_slot(
    catalog: &ComponentCatalog,
    panic: bool,
) -> (Arc<ComponentSlot>, Arc<State>) {
    slot(catalog, panic, false).await
}
pub(super) async fn native_slot(
    catalog: &ComponentCatalog,
    panic: bool,
) -> (Arc<ComponentSlot>, Arc<State>) {
    slot(catalog, panic, true).await
}
async fn slot(
    catalog: &ComponentCatalog,
    panic: bool,
    native: bool,
) -> (Arc<ComponentSlot>, Arc<State>) {
    let (address, native) = if native {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        // The unique control is retained synchronously before another await.
        (
            Some(address),
            Some(ControlledTcpListener::new(
                listener,
                CancellationToken::new(),
            )),
        )
    } else {
        (None, None)
    };
    let state = Arc::new(State {
        address,
        native: Mutex::new(native),
        peer: Mutex::new(None),
        pending: Default::default(),
        task: Mutex::new(None),
        native_task: Mutex::new(None),
    });
    let recipe = crate::component::ComponentRecipe::from_validated(
        Arc::new(Factory {
            state: state.clone(),
            panic,
        }),
        Arc::new(()),
    );
    let slot = Arc::new(ComponentSlot::from_recipe(
        catalog.allocate_id().unwrap(),
        ResourceGeneration(1),
        recipe,
        Default::default(),
        "preowned_listener",
    ));
    (slot, state)
}
pub(super) fn assert_bound(state: &State) {
    assert!(
        std::net::TcpListener::bind(state.address.unwrap()).is_err(),
        "original native listener still holds its port"
    );
}
pub(super) fn assert_released(state: &State) {
    let rebound = std::net::TcpListener::bind(state.address.unwrap()).unwrap();
    assert_eq!(rebound.local_addr().unwrap(), state.address.unwrap());
}
