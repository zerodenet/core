//! Same preconnection entry, native originals, client material and worker Scope.
use super::{Native, PreparedPreconnect};
use std::sync::Arc;
use zero_platform_tokio::contract::{
    component::{BuildContext, ComponentFactory, ComponentInstance, ComponentSpec, ResourceAccess},
    lifecycle::{
        erase_lifecycle, BuildError, CleanupFailurePhase, CloseReport, DynResourceLifecycle,
        ResourceCleanupFailures, ResourceLifecycle,
    },
    scope::TaskRegistrar,
    CancellationToken, ChannelError, CloseReason,
};
pub struct PoolFactory;
impl ComponentFactory for PoolFactory {
    type Options = PreparedPreconnect;
    fn describe(&self, options: &PreparedPreconnect) -> ComponentSpec {
        let mut spec = super::super::mux::OwnedPoolFactory.describe(&options.client_options());
        // One physical setup/driver slot per original plus the explicit finite
        // warm workers. Neither first demand nor retries can increase this.
        spec.task_limit = options
            .originals
            .get()
            .checked_add(options.warm.get())
            .and_then(std::num::NonZeroUsize::new);
        let mut identity = spec.static_identity.to_vec();
        identity.extend_from_slice(b"vless.actual-preconnect-workers.v1");
        identity.extend_from_slice(&options.warm.get().to_le_bytes());
        spec.static_identity = identity.into();
        spec
    }
    fn build(
        &self,
        options: Arc<PreparedPreconnect>,
        context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        self.build_owned(options, context, None)
    }
}
impl PoolFactory {
    pub fn build_with_ech_resolver(
        &self,
        options: Arc<PreparedPreconnect>,
        context: BuildContext,
        resolver: Arc<dyn zero_transport::tls::ech::EchConfigResolver>,
    ) -> Result<ComponentInstance, BuildError> {
        self.build_owned(options, context, Some(resolver))
    }
    fn build_owned(
        &self,
        options: Arc<PreparedPreconnect>,
        context: BuildContext,
        resolver: Option<Arc<dyn zero_transport::tls::ech::EchConfigResolver>>,
    ) -> Result<ComponentInstance, BuildError> {
        let instance = context.instance();
        if (options.requires_lender && options.pool.stream_dependency().is_none())
            || self.describe(&options).task_limit.is_none()
        {
            return Err(super::native::error(
                instance,
                std::io::Error::other(
                    "preconnect requires its actual lower owner and finite worker budget",
                ),
            ));
        }
        let resources = context.resources.clone();
        let tasks = context.tasks.clone();
        let cancel = context.cancel.child_token();
        let masks = if options.pool.stream_dependency().is_some() {
            Default::default()
        } else {
            super::super::masks::prepare(&options.protocol, resources.as_ref(), true, None)?
        };
        let leaf = options
            .leaf
            .clone()
            .with_preparation_reality(options.protocol.reality.clone())
            .with_native_tcp_masks(masks);
        let owned = options.client_options();
        let original = match resolver {
            Some(resolver) => super::super::mux::OwnedPoolFactory
                .build_with_ech_resolver(owned, context, resolver)?,
            None => super::super::mux::OwnedPoolFactory.build(owned, context)?,
        };
        Ok(ComponentInstance {
            roles: original.roles,
            lifecycle: erase_lifecycle(Owner {
                original: original.lifecycle,
                options,
                leaf,
                resources,
                tasks,
                cancel,
                instance,
                native: None,
                failures: Default::default(),
            }),
        })
    }
}
struct Owner {
    original: Box<dyn DynResourceLifecycle>,
    options: Arc<PreparedPreconnect>,
    leaf: crate::transport::VlessOutboundLeaf,
    resources: Arc<dyn ResourceAccess>,
    tasks: TaskRegistrar,
    cancel: CancellationToken,
    instance: zero_core::component::ComponentInstanceId,
    native: Option<Arc<Native>>,
    failures: ResourceCleanupFailures,
}
impl ResourceLifecycle for Owner {
    async fn start(&mut self, end: tokio::time::Instant) -> Result<(), BuildError> {
        self.original.start(end).await?;
        if self.native.is_some() {
            return Ok(());
        }
        let owner = self
            .options
            .access
            .control()
            .refresh()
            .and_then(|control| control.managed())
            .map_err(|source| super::native::error(self.instance, source))?;
        if owner.instance() != self.instance {
            return Err(super::native::error(
                self.instance,
                std::io::Error::other("wrong preconnect original owner"),
            ));
        }
        let native = Native::new(
            self.options.clone(),
            owner,
            self.leaf.clone(),
            self.tasks.clone(),
            self.resources.packet_driver_metadata()?,
            self.cancel.clone(),
        )?;
        self.options
            .access
            .install_native(&native)
            .map_err(|source| super::native::error(self.instance, source))?;
        self.native = Some(native);
        Ok(())
    }
    fn revoke(&self, reason: CloseReason) {
        self.cancel.cancel();
        if let Some(native) = &self.native {
            native.stop();
        }
        self.original.revoke(reason);
    }
    async fn shutdown(&mut self, end: tokio::time::Instant) -> Result<CloseReport, BuildError> {
        self.revoke(CloseReason::Shutdown);
        let mut report = CloseReport::default();
        if let Some(native) = &self.native {
            match native.drain(end).await {
                Ok(actual) => report.append(actual),
                Err(source) => {
                    self.failures
                        .receive(Arc::new(source), CleanupFailurePhase::Driver);
                }
            }
        }
        match self.original.shutdown(end).await {
            Ok(actual) => report.append(actual),
            Err(source) => {
                self.failures
                    .receive(Arc::new(source), CleanupFailurePhase::Shutdown);
            }
        }
        if let Some(source) = self.failures.report() {
            return Err(ChannelError::retained(source));
        }
        Ok(report)
    }
}
