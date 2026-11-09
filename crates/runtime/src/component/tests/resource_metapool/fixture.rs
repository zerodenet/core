use super::*;
use crate::component::{
    catalog::{supervision::testing::TestContext, ComponentCatalog},
    instance::ComponentSlot,
    ComponentRecipe,
};
use zero_platform_tokio::contract::{
    component::{
        BuildContext, ComponentFactory, ComponentInstance, ComponentRoles, ComponentSpec,
        ResourceOwner,
    },
    lifecycle::{erase_lifecycle, ResourceLifecycle},
    scope::{TaskClass, TaskJoinWitness, TaskRegistrar},
    CloseReason,
};
use zero_platform_tokio::controlled_datagram::metadata_pool::{
    MetadataPoolGrants, MetadataPoolOwner,
};
#[derive(Default)]
struct Issued {
    current: Mutex<Option<(MetadataPoolGrants, TaskRegistrar)>>,
}
struct Factory {
    declaration: ResourceDeclaration,
    issued: Arc<Issued>,
    cap: std::num::NonZeroUsize,
}
struct Lifecycle {
    owner: MetadataPoolOwner,
    context: BuildContext,
    issued: Arc<Issued>,
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
            resources: vec![self.declaration.clone()],
        }
    }
    fn build(&self, _: Arc<()>, context: BuildContext) -> Result<ComponentInstance, ChannelError> {
        let owner =
            MetadataPoolOwner::new(context.instance(), self.cap, limits(), &context.cancel)?;
        Ok(ComponentInstance {
            roles: ComponentRoles::default(),
            lifecycle: erase_lifecycle(Lifecycle {
                owner,
                context,
                issued: self.issued.clone(),
            }),
        })
    }
}
impl ResourceLifecycle for Lifecycle {
    async fn start(&mut self, _: Instant) -> Result<(), ChannelError> {
        self.context.resources.register_metadata_pool(
            0,
            self.owner.registration(),
            &self.context.tasks,
        )?;
        self.owner.start()?;
        *self.issued.current.lock().unwrap() =
            Some((self.owner.grants(), self.context.tasks.clone()));
        Ok(())
    }
    fn revoke(&self, _: CloseReason) {
        self.owner.revoke();
    }
    async fn shutdown(&mut self, _: Instant) -> Result<CloseReport, ChannelError> {
        self.owner.shutdown()
    }
}
pub(super) struct Fixture {
    pub slot: Arc<ComponentSlot>,
    pub grants: MetadataPoolGrants,
    pub tasks: TaskRegistrar,
}
impl Fixture {
    pub async fn new(peer: SocketAddr, cap: usize) -> Self {
        Self::declared(
            ResourceDeclaration::ControlledMetadataDatagram {
                endpoint: peer,
                dial: Default::default(),
                limits: limits(),
                max_origins: n(cap),
            },
            cap,
        )
        .await
    }
    pub async fn dynamic(
        endpoint: zero_core::request::Destination,
        ports: Vec<u16>,
        cap: usize,
    ) -> Self {
        Self::declared(
            ResourceDeclaration::ControlledMetadataCarrier {
                endpoint,
                dial: Default::default(),
                ports,
                limits: limits(),
                max_origins: n(cap),
            },
            cap,
        )
        .await
    }
    async fn declared(declaration: ResourceDeclaration, cap: usize) -> Self {
        // All fixtures model components in one host namespace, as production
        // does. Independent allocators may intentionally reuse numeric IDs.
        static CATALOG: std::sync::OnceLock<ComponentCatalog> = std::sync::OnceLock::new();
        let catalog = CATALOG.get_or_init(ComponentCatalog::default);
        let id = catalog.allocate_id().unwrap();
        let issued = Arc::new(Issued::default());
        let recipe = ComponentRecipe::from_validated(
            Arc::new(Factory {
                declaration,
                issued: issued.clone(),
                cap: n(cap),
            }),
            Arc::new(()),
        );
        let slot = Arc::new(ComponentSlot::from_recipe(
            id,
            zero_core::component::ResourceGeneration(1),
            recipe,
            Default::default(),
            "metadata-pool-fixture",
        ));
        TestContext::new([slot.as_ref()])
            .drive(slot.start(end()))
            .await
            .unwrap();
        let (grants, tasks) = issued.current.lock().unwrap().take().unwrap();
        Self {
            slot,
            grants,
            tasks,
        }
    }
    pub async fn drive<T>(&self, work: impl std::future::Future<Output = T>) -> T {
        TestContext::new([self.slot.as_ref()]).drive(work).await
    }
    pub fn query(&self) -> (TaskJoinWitness, tokio::sync::oneshot::Sender<()>) {
        let (release, wait) = tokio::sync::oneshot::channel();
        let registration = self
            .tasks
            .spawn_tracked(TaskClass::Auxiliary, async move {
                let _ = wait.await;
                Ok(())
            })
            .unwrap();
        (registration.1, release)
    }
    pub async fn complete_query(
        &self,
        query: &TaskJoinWitness,
        release: tokio::sync::oneshot::Sender<()>,
    ) {
        assert!(query.try_joined().is_none());
        release.send(()).unwrap();
        let joined = self.drive(query.wait_registration_released()).await;
        assert!(query.matches_joined(&joined));
        assert!(joined.result().is_ok());
        assert!(joined.registration_released());
    }
    pub async fn finish(&self) -> CloseReport {
        self.drive(self.slot.shutdown(end())).await.unwrap()
    }
}
