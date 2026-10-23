use super::*;
use tokio::runtime::Handle;
use zero_core::component::ResourceGeneration;
use zero_platform_tokio::contract::{lifecycle::TaskClass, scope::TaskScope};
mod authority;
mod lifecycle;
mod unknown;
fn one() -> NonZeroUsize {
    NonZeroUsize::new(1).unwrap()
}
fn instance() -> ComponentInstanceId {
    ComponentInstanceId {
        component: ComponentId(950),
        generation: ResourceGeneration(1),
    }
}
fn deadline() -> Instant {
    Instant::now() + std::time::Duration::from_secs(2)
}
struct Fixture {
    resources: Arc<HostResources>,
    scope: TaskScope,
    grants: Vec<RootDatagramGrant>,
    egress: EgressInterfaceControl,
}
impl Fixture {
    fn new(peers: &[SocketAddr], max_queries: NonZeroUsize) -> Self {
        let mut scope = TaskScope::new(Handle::current(), max_queries);
        scope.enable_registration().unwrap();
        let egress = EgressInterfaceControl::default();
        let declarations: Vec<_> = peers
            .iter()
            .map(|peer| ResourceDeclaration::ControlledOutboundDatagram {
                peer: *peer,
                network_generation: NetworkGeneration(egress.generation()),
                max_sockets: one(),
            })
            .collect();
        let resources = HostResources::new_for_instance(
            instance(),
            &declarations,
            CancellationToken::new(),
            scope.registrar().scope_identity(),
        )
        .unwrap();
        let ordinals: Vec<_> = (0..peers.len()).collect();
        let grants = resources
            .issue_datagram_root(&ordinals, egress.clone(), &scope.registrar(), max_queries)
            .unwrap();
        Self {
            resources,
            scope,
            grants,
            egress,
        }
    }
    fn spawn<F, Fut>(&self, run: F) -> TaskJoinWitness
    where
        F: FnOnce(TaskJoinWitness) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<(), ChannelError>> + Send + 'static,
    {
        let (bound, ready) = tokio::sync::oneshot::channel();
        let (_, witness) = self
            .scope
            .registrar()
            .spawn_tracked(TaskClass::Auxiliary, async move {
                let witness = ready.await.expect("same original registration gate");
                run(witness).await
            })
            .unwrap();
        bound.send(witness.clone()).unwrap();
        witness
    }
}
