//! Original MUX owner plus its protocol-owned immutable client material.
use std::sync::Arc;
use zero_core::component::ComponentInstanceId;
use zero_platform_tokio::contract::{
    component::{BuildContext, ComponentFactory, ComponentInstance, ComponentSpec, ResourceAccess},
    lifecycle::{
        erase_lifecycle, BuildError, CloseReport, DynResourceLifecycle, ResourceLifecycle,
    },
    CancellationToken, CloseReason,
};
use zero_transport::{
    pooled_driver::{PoolFactory, PoolOptions},
    tls::ech::EchConfigResolver,
};
#[derive(Clone)]
pub struct OwnedPoolOptions {
    pub(super) pool: Arc<PoolOptions>,
    pub(super) material: Option<Arc<super::super::Options>>,
}
impl OwnedPoolOptions {
    pub fn pool_options(&self) -> &Arc<PoolOptions> {
        &self.pool
    }
    pub fn with_scoped_preparation(mut self) -> Self {
        self.pool = Arc::new((*self.pool).clone().with_scoped_preparation());
        self
    }
}
pub struct OwnedPoolFactory;
impl ComponentFactory for OwnedPoolFactory {
    type Options = OwnedPoolOptions;
    fn describe(&self, options: &OwnedPoolOptions) -> ComponentSpec {
        let mut spec = PoolFactory.describe(&options.pool);
        if let Some(material) = &options.material {
            let mut identity = spec.static_identity.to_vec();
            identity.extend_from_slice(b"vmess.mux.original-client-material");
            identity.extend_from_slice(&material.identity);
            spec.static_identity = identity.into();
            if let Some(file) = material
                .material
                .as_ref()
                .and_then(|material| material.resource())
            {
                spec.resources.push(file);
            }
        }
        spec
    }
    fn build(
        &self,
        options: Arc<OwnedPoolOptions>,
        context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        self.build_owned(options, context, None)
    }
}
impl OwnedPoolFactory {
    pub fn build_with_ech_resolver(
        &self,
        options: Arc<OwnedPoolOptions>,
        context: BuildContext,
        resolver: Arc<dyn EchConfigResolver>,
    ) -> Result<ComponentInstance, BuildError> {
        self.build_owned(options, context, Some(resolver))
    }
    fn build_owned(
        &self,
        options: Arc<OwnedPoolOptions>,
        context: BuildContext,
        resolver: Option<Arc<dyn EchConfigResolver>>,
    ) -> Result<ComponentInstance, BuildError> {
        let instance = context.instance();
        let resources = context.resources.clone();
        let cancel = context.cancel.clone();
        let original = PoolFactory.build(options.pool.clone(), context)?;
        Ok(ComponentInstance {
            roles: original.roles,
            lifecycle: erase_lifecycle(Owner {
                original: original.lifecycle,
                options,
                instance,
                resources,
                cancel,
                resolver,
            }),
        })
    }
}
struct Owner {
    original: Box<dyn DynResourceLifecycle>,
    options: Arc<OwnedPoolOptions>,
    instance: ComponentInstanceId,
    resources: Arc<dyn ResourceAccess>,
    cancel: CancellationToken,
    resolver: Option<Arc<dyn EchConfigResolver>>,
}
impl ResourceLifecycle for Owner {
    async fn start(&mut self, end: tokio::time::Instant) -> Result<(), BuildError> {
        if let Some(material) = self
            .options
            .material
            .as_ref()
            .and_then(|options| options.material.as_ref())
        {
            material
                .start(self.resources.as_ref(), self.instance, &self.cancel, end)
                .await?;
        }
        self.original.start(end).await?;
        if let Some(material) = self
            .options
            .material
            .as_ref()
            .and_then(|options| options.material.as_ref())
        {
            material.record_bound(self.instance, self.resolver.as_ref())?;
        }
        Ok(())
    }
    fn revoke(&self, reason: CloseReason) {
        self.original.revoke(reason);
    }
    async fn shutdown(&mut self, end: tokio::time::Instant) -> Result<CloseReport, BuildError> {
        self.original.shutdown(end).await
    }
}
