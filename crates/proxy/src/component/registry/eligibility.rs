use super::*;
impl<Input: 'static> ComponentRegistry<Input> {
    /// A pure cold-input selection boundary for a partially migrated protocol.
    /// Selected inputs must still pass decode/validation; an invalid native
    /// option is never converted into a silent legacy fallback.
    pub fn register_where<F, Accept, Decode>(
        &mut self,
        descriptor: ProtocolCapabilityDescriptor,
        factory: F,
        accepts: Accept,
        decode_validate: Decode,
    ) -> Result<(), RegistrationError>
    where
        F: ComponentFactory,
        Accept: Fn(&Input) -> bool + Send + Sync + 'static,
        Decode: Fn(&Input) -> Result<F::Options, ConfigError> + Send + Sync + 'static,
    {
        self.register(descriptor, factory, decode_validate)?;
        self.entries
            .get_mut(descriptor.protocol)
            .expect("registered protocol")
            .accepts = Some(Arc::new(accepts));
        Ok(())
    }
    pub fn has_factory_for(&self, protocol: &str, input: &Input) -> bool {
        self.entries.get(protocol).is_some_and(|entry| {
            entry.registration.is_some()
                && entry.accepts.as_ref().is_none_or(|accepts| accepts(input))
        })
    }
}
