use std::sync::Arc;
use zero_platform_tokio::contract::{
    component::{BuildContext, ComponentFactory, ComponentInstance, ComponentSpec},
    lifecycle::BuildError,
};

pub(in crate::component) mod accounting;
mod bound_datagram;
mod inbound_dependencies;
mod packet_path;
pub use packet_path::PacketPathFacts;

trait BuildRecipe: Send + Sync {
    fn build(&self, context: BuildContext) -> Result<ComponentInstance, BuildError>;
}
struct Prepared<F: ComponentFactory> {
    factory: Arc<F>,
    options: Arc<F::Options>,
}
impl<F: ComponentFactory> BuildRecipe for Prepared<F> {
    fn build(&self, context: BuildContext) -> Result<ComponentInstance, BuildError> {
        self.factory.build(self.options.clone(), context)
    }
}

/// Protocol-neutral cold recipe; shared resource owners need no fake protocol
/// capability descriptor, callable role or recovery through Any.
#[derive(Clone)]
pub struct ComponentRecipe {
    spec: Arc<ComponentSpec>,
    recipe: Arc<dyn BuildRecipe>,
    bound_datagrams: Arc<[bound_datagram::NativeBinding]>,
    carrier_total: Option<usize>,
    process_limits: Option<accounting::ProcessLimits>,
    preparation_limits: Option<accounting::PreparationLimits>,
    buffer_limits: Option<zero_platform_tokio::controlled_carrier::CarrierBufferLimits>,
    component_task_total: Option<usize>,
    packet_path: Option<PacketPathFacts>,
}
impl ComponentRecipe {
    /// Options have already been validated by their owning factory/adapter.
    /// Construction and describe perform no runtime I/O or resource start.
    pub fn from_validated<F: ComponentFactory>(factory: Arc<F>, options: Arc<F::Options>) -> Self {
        let spec = Arc::new(factory.describe(&options));
        Self {
            spec,
            recipe: Arc::new(Prepared { factory, options }),
            bound_datagrams: Arc::from([]),
            carrier_total: None,
            process_limits: None,
            preparation_limits: None,
            buffer_limits: None,
            component_task_total: None,
            packet_path: None,
        }
    }
    pub fn spec(&self) -> &ComponentSpec {
        &self.spec
    }
    pub fn build(&self, context: BuildContext) -> Result<ComponentInstance, BuildError> {
        crate::configuration::confirm_preparation_io_closed().map_err(|source| {
            zero_platform_tokio::contract::ChannelError::new(
                zero_platform_tokio::contract::Stage::Prepare,
                zero_platform_tokio::contract::Reason::Revoked,
                zero_platform_tokio::contract::Impact::Component,
            )
            .with_source(source)
        })?;
        super::bindings::validate_ids(&self.spec, &context)?;
        let instance = self.recipe.build(context)?;
        super::bindings::validate_roles(&self.spec, &instance.roles)?;
        Ok(instance)
    }
}
