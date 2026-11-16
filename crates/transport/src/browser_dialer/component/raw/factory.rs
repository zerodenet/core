//! Cold declaration and installed raw Browser owner; no second protocol pool.
use super::*;
pub struct RawOptions {
    pub browser: Arc<Options>,
    pub carrier: RawCarrier,
    pub identity: Arc<[u8]>,
}
pub struct RawFactory;
impl ComponentFactory for RawFactory {
    type Options = RawOptions;
    fn describe(&self, options: &RawOptions) -> ComponentSpec {
        let mut spec = ComponentSpec {
            owner: ResourceOwner::Component,
            roles: vec![],
            dependencies: vec![],
            static_identity: options.identity.clone(),
            resources: vec![],
            task_limit: None,
        };
        options.browser.declare(&mut spec);
        spec
    }
    fn build(
        &self,
        options: Arc<RawOptions>,
        context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        let (owner, handle) = Owner::build(options.browser.clone(), &context)?;
        options.carrier.install(&handle.0)?;
        Ok(ComponentInstance {
            roles: Default::default(),
            lifecycle: erase_lifecycle(owner),
        })
    }
}
