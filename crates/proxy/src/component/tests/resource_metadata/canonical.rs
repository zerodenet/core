//! Actual allocator/slot/start grant fixture. It does not bypass the resourceful
//! production gate or claim that the external QUIC users have been closed.
use super::*;
use crate::component::{
    catalog::{supervision::testing::TestContext, ComponentCatalog},
    instance::ComponentSlot,
    ComponentRecipe,
};
use std::future::Future;
use zero_platform_tokio::contract::{
    component::{
        BuildContext, ComponentFactory, ComponentInstance, ComponentRoles, ComponentSpec,
        ResourceOwner,
    },
    lifecycle::{erase_lifecycle, CloseReport, ResourceLifecycle, Unfinished},
    scope::TaskRegistrar,
    CloseReason,
};
#[derive(Default)]
struct Issued(Mutex<Option<(GrantedMetadataListener, TaskRegistrar)>>);
struct Factory(Arc<Issued>, ResourceDeclaration, NonZeroUsize);
struct Lifecycle {
    issued: Arc<Issued>,
    context: BuildContext,
    request: MetadataDatagramRequest,
}
impl ComponentFactory for Factory {
    type Options = ();
    fn describe(&self, _: &()) -> ComponentSpec {
        ComponentSpec {
            owner: ResourceOwner::Component,
            roles: vec![],
            dependencies: vec![],
            static_identity: Arc::from([]),
            resources: vec![self.1.clone()],
            task_limit: Some(self.2),
        }
    }
    fn build(&self, _: Arc<()>, context: BuildContext) -> Result<ComponentInstance, ChannelError> {
        let ResourceDeclaration::MetadataDatagram { source, limits, .. } = &self.1 else {
            unreachable!()
        };
        Ok(ComponentInstance {
            roles: ComponentRoles::default(),
            lifecycle: erase_lifecycle(Lifecycle {
                issued: self.0.clone(),
                context,
                request: MetadataDatagramRequest {
                    ordinal: 0,
                    source: *source,
                    limits: *limits,
                },
            }),
        })
    }
}
impl ResourceLifecycle for Lifecycle {
    async fn start(&mut self, _: Instant) -> Result<(), ChannelError> {
        let grant = self
            .context
            .resources
            .listen_metadata_datagram(self.request)
            .await?;
        assert!(grant.scope.matches(&self.context.tasks));
        *self.issued.0.lock().unwrap() = Some((grant, self.context.tasks.clone()));
        Ok(())
    }
    fn revoke(&self, _: CloseReason) {}
    async fn shutdown(&mut self, _: Instant) -> Result<CloseReport, ChannelError> {
        // The consumer test owns the actual endpoint/user lifetime. An empty
        // lifecycle body cannot stand in for that missing canonical receipt.
        Ok(CloseReport {
            unfinished: vec![Unfinished::Resource(
                "fixture transferred QUIC users have no canonical closed receipt".into(),
            )],
            ..Default::default()
        })
    }
}
pub(super) struct CanonicalMetadata {
    pub(super) slot: Arc<ComponentSlot>,
}
impl CanonicalMetadata {
    pub(super) async fn start(
        source: SocketAddr,
        limits: MetadataLimits,
        task_limit: NonZeroUsize,
    ) -> (Self, GrantedMetadataListener, TaskRegistrar) {
        let catalog = ComponentCatalog::default();
        let id = catalog.allocate_id().unwrap();
        let issued = Arc::new(Issued::default());
        let recipe = ComponentRecipe::from_validated(
            Arc::new(Factory(
                issued.clone(),
                ResourceDeclaration::MetadataDatagram {
                    source,
                    limits,
                    max_bind_attempts: n(1),
                },
                task_limit,
            )),
            Arc::new(()),
        );
        let slot = ComponentSlot::from_recipe(
            id,
            ResourceGeneration(1),
            recipe,
            Default::default(),
            "metadata-fixture",
        );
        TestContext::new([slot.as_ref()])
            .drive(slot.start(deadline()))
            .await
            .unwrap();
        let (grant, tasks) = issued.0.lock().unwrap().take().unwrap();
        (Self { slot }, grant, tasks)
    }
    pub(super) async fn drive<T>(&self, work: impl Future<Output = T>) -> T {
        TestContext::new([self.slot.as_ref()]).drive(work).await
    }
    pub(super) async fn shutdown(&self) -> CloseReport {
        self.drive(self.slot.shutdown(deadline())).await.unwrap()
    }
}

#[tokio::test]
async fn actual_slot_start_grants_the_canonical_original_scope_but_does_not_forge_user_completion()
{
    let (fixture, grant, tasks) = CanonicalMetadata::start(loopback(), limits(), n(1)).await;
    assert!(grant.scope.matches(&tasks));
    assert!(grant.scope.registration_open());
    let address = grant.tx.local_addr().unwrap();
    assert!(std::net::UdpSocket::bind(address).is_err());
    let report = fixture.shutdown().await;
    assert!(!report.is_complete());
    let retained = std::net::UdpSocket::bind(address).unwrap();
    assert!(!grant.scope.registration_open());
    drop(retained);
}
