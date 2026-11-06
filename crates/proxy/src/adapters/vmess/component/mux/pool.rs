//! Full client material belongs to this same actual MUX pool Scope.
use crate::component::background::{self, pool, BackgroundCarrier};
use std::sync::Arc;
use zero_platform_tokio::contract::{
    component::{BuildContext, ComponentFactory, ComponentInstance, ComponentSpec},
    lifecycle::BuildError,
};
pub(super) struct Options {
    pub(super) pool: Arc<::vmess::component::mux::OwnedPoolOptions>,
    pub(super) tag: String,
    pub(super) wire_endpoint: zero_core::request::Destination,
}
pub(super) struct Factory {
    pub(super) background: Arc<pool::Factory>,
}
impl ComponentFactory for Factory {
    type Options = Options;
    fn describe(&self, options: &Options) -> ComponentSpec {
        let mut spec = ::vmess::component::mux::OwnedPoolFactory.describe(&options.pool);
        let mut identity = spec.static_identity.to_vec();
        let wire = format!("{:?}", options.wire_endpoint);
        identity.extend_from_slice(&(wire.len() as u64).to_le_bytes());
        identity.extend_from_slice(wire.as_bytes());
        spec.static_identity = identity.into();
        spec
    }
    fn build(
        &self,
        options: Arc<Options>,
        mut context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        let instance = context.instance();
        context.background_carrier = Some(
            BackgroundCarrier {
                network: self.background.network.clone(),
                instance,
                initial: options.pool.pool_options().binding().clone(),
                endpoint: options.pool.pool_options().endpoint().clone(),
                wire_endpoint: options.wire_endpoint.clone(),
                tag: options.tag.clone(),
            }
            .transport(),
        );
        ::vmess::component::mux::OwnedPoolFactory.build_with_ech_resolver(
            options.pool.clone(),
            context,
            background::tls_material::resolver(self.background.network.clone(), instance),
        )
    }
}
