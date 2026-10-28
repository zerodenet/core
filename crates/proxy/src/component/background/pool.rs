//! Literal shared-stream declarations assembled with the actual host provider.
use super::{BackgroundCarrier, BackgroundNetwork};
use std::sync::Arc;
use zero_core::component::NetworkGeneration;
use zero_platform_tokio::contract::{
    component::{BuildContext, ComponentFactory, ComponentInstance, ComponentSpec},
    lifecycle::BuildError,
};
use zero_transport::pooled_driver::{PoolFactory, PoolOptions};

pub(crate) struct Options {
    pub(crate) pool: Arc<PoolOptions>,
    pub(crate) tag: String,
}
pub(crate) struct Factory {
    pub(crate) network: Arc<BackgroundNetwork>,
    pub(crate) generation: NetworkGeneration,
}
impl Factory {
    pub(crate) fn new(network: Arc<BackgroundNetwork>) -> Self {
        let generation = NetworkGeneration(network.egress.generation());
        Self {
            network,
            generation,
        }
    }
}
impl ComponentFactory for Factory {
    type Options = Options;
    fn describe(&self, options: &Options) -> ComponentSpec {
        PoolFactory.describe(&options.pool)
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
                initial: options.pool.binding().clone(),
                endpoint: options.pool.endpoint().clone(),
                wire_endpoint: options.pool.endpoint().clone(),
                tag: options.tag.clone(),
            }
            .transport(),
        );
        PoolFactory.build(options.pool.clone(), context)
    }
}
