use std::sync::Arc;
use zero_platform_tokio::contract::{
    component::{BuildContext, ComponentFactory, ComponentInstance, ComponentSpec},
    lifecycle::BuildError,
};

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
}
impl ComponentRecipe {
    /// Options have already been validated by their owning factory/adapter.
    /// Construction and describe perform no runtime I/O or resource start.
    pub fn from_validated<F: ComponentFactory>(factory: Arc<F>, options: Arc<F::Options>) -> Self {
        let spec = Arc::new(factory.describe(&options));
        Self {
            spec,
            recipe: Arc::new(Prepared { factory, options }),
        }
    }
    pub fn spec(&self) -> &ComponentSpec {
        &self.spec
    }
    pub fn build(&self, context: BuildContext) -> Result<ComponentInstance, BuildError> {
        super::bindings::validate_ids(&self.spec, &context)?;
        let instance = self.recipe.build(context)?;
        super::bindings::validate_roles(&self.spec, &instance.roles)?;
        Ok(instance)
    }
}
