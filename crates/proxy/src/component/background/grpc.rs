//! Assemble the existing protocol gRPC pool with the real current host provider.
use super::{BackgroundCarrier, BackgroundNetwork};
use std::sync::Arc;
use zero_core::component::NetworkGeneration;
use zero_platform_tokio::contract::{
    component::{BuildContext, ComponentFactory, ComponentInstance, ComponentSpec},
    lifecycle::BuildError,
    network::UpstreamBinding,
};
use zero_transport::grpc::{NativePoolFactory, NativePoolOptions};
pub(crate) struct Options {
    pub(crate) pool: Arc<NativePoolOptions>,
    pub(crate) tag: String,
}
pub(crate) struct Factory {
    pub(crate) network: Arc<BackgroundNetwork>,
    pub(crate) generation: NetworkGeneration,
}
impl ComponentFactory for Factory {
    type Options = Options;
    fn describe(&self, options: &Options) -> ComponentSpec {
        NativePoolFactory.describe(&options.pool)
    }
    fn build(
        &self,
        options: Arc<Options>,
        mut context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        context.background_carrier = Some(
            BackgroundCarrier {
                network: self.network.clone(),
                instance: context.instance(),
                initial: UpstreamBinding {
                    prefix: vec![],
                    dial: options.pool.dial_policy().clone(),
                    network_generation: self.generation,
                },
                endpoint: options.pool.endpoint().clone(),
                wire_endpoint: options.pool.endpoint().clone(),
                tag: options.tag.clone(),
            }
            .transport(),
        );
        NativePoolFactory.build(options.pool.clone(), context)
    }
}
