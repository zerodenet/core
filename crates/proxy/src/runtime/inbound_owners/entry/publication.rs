//! The actual listener's publication fence survives partial candidate Ready.
use super::*;
impl InboundOwner {
    pub(crate) fn bind_configuration_publication(
        &self,
        config: &Arc<zero_config::RuntimeConfig>,
        publication: zero_engine::ConfigurationPublication,
    ) -> Result<(), EngineError> {
        let input = Arc::downgrade(config);
        let bound = self
            .publication
            .get_or_init(|| (input.clone(), publication));
        if !bound.0.ptr_eq(&input) {
            return Err(std::io::Error::other(
                "listener owner cannot move to another configuration publication",
            )
            .into());
        }
        Ok(())
    }
    pub(crate) fn has_uncommitted_configuration(&self) -> bool {
        self.publication
            .get()
            .is_some_and(|(_, publication)| !publication.is_committed())
    }
}
