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
        self.register_with_resources_where(descriptor, factory, accepts, move |input, _| {
            decode_validate(input).map_err(RegistrationError::Options)
        })
    }
    /// The same cold selection boundary for protocols declaring shared owners.
    /// It cannot fall back after validation, allocation, build or start failure.
    pub fn register_with_resources_where<F, Accept, Decode>(
        &mut self,
        descriptor: ProtocolCapabilityDescriptor,
        factory: F,
        accepts: Accept,
        decode_validate: Decode,
    ) -> Result<(), RegistrationError>
    where
        F: ComponentFactory,
        Accept: Fn(&Input) -> bool + Send + Sync + 'static,
        Decode: Fn(&Input, &mut ResourcePreparation<'_>) -> Result<F::Options, RegistrationError>
            + Send
            + Sync
            + 'static,
    {
        self.register_with_resources(descriptor, factory, decode_validate)?;
        self.entries
            .get_mut(descriptor.protocol)
            .expect("registered protocol")
            .accepts = Some(Arc::new(move |input, _| accepts(input)));
        Ok(())
    }
    pub fn has_factory_for(&self, protocol: &str, input: &Input) -> bool {
        self.has_factory_for_candidate(protocol, input, None)
    }
    pub fn set_candidate_acceptance<F>(&mut self, protocol: &str, accepts: F)
    where
        F: Fn(&Input, Option<&zero_config::RuntimeConfig>) -> bool + Send + Sync + 'static,
    {
        let entry = self
            .entries
            .get_mut(protocol)
            .expect("registered candidate selector");
        let previous = entry.accepts.take();
        entry.accepts = Some(Arc::new(move |input, candidate| {
            previous
                .as_ref()
                .is_none_or(|check| check(input, candidate))
                && accepts(input, candidate)
        }));
    }
    pub fn has_factory_for_candidate(
        &self,
        protocol: &str,
        input: &Input,
        candidate: Option<&zero_config::RuntimeConfig>,
    ) -> bool {
        self.entries.get(protocol).is_some_and(|entry| {
            entry.registration.is_some()
                && entry
                    .accepts
                    .as_ref()
                    .is_none_or(|accepts| accepts(input, candidate))
        })
    }
}
