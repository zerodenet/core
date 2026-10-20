//! Typed cold-path registration. No input documents survive preparation.
use super::{instance::invalid, recipe::ComponentRecipe, ResourcePreparation};
use std::{collections::BTreeMap, sync::Arc};
use zero_config::ConfigError;
use zero_platform_tokio::contract::{
    component::{BuildContext, ComponentFactory, ComponentInstance, ComponentSpec},
    lifecycle::BuildError,
    Reason,
};
use zero_traits::ProtocolCapabilityDescriptor;

mod error;
pub use error::RegistrationError;
mod eligibility;

#[derive(Clone)]
pub struct ValidatedRecipe {
    legacy_connect_stage: &'static str,
    descriptor: ProtocolCapabilityDescriptor,
    recipe: ComponentRecipe,
}
impl ValidatedRecipe {
    pub(crate) fn component_recipe(&self) -> ComponentRecipe {
        self.recipe.clone()
    }
    pub(crate) fn legacy_connect_stage(&self) -> &'static str {
        self.legacy_connect_stage
    }
    pub fn descriptor(&self) -> ProtocolCapabilityDescriptor {
        self.descriptor
    }
    pub fn spec(&self) -> &ComponentSpec {
        self.recipe.spec()
    }
    /// The caller owns the stop-old/build-new ordering and installs the returned
    /// lifecycle owner before calling start. This method never starts resources.
    pub fn build(&self, context: BuildContext) -> Result<ComponentInstance, BuildError> {
        self.recipe.build(context)
    }
}

trait Registration<Input>: Send + Sync {
    fn prepare(
        &self,
        input: &Input,
        resources: &mut ResourcePreparation<'_>,
    ) -> Result<ValidatedRecipe, RegistrationError>;
}
struct TypedRegistration<F, Decode> {
    descriptor: ProtocolCapabilityDescriptor,
    factory: Arc<F>,
    decode_validate: Decode,
}
impl<Input, F, Decode> Registration<Input> for TypedRegistration<F, Decode>
where
    F: ComponentFactory,
    Decode: Fn(&Input, &mut ResourcePreparation<'_>) -> Result<F::Options, RegistrationError>
        + Send
        + Sync,
{
    fn prepare(
        &self,
        input: &Input,
        resources: &mut ResourcePreparation<'_>,
    ) -> Result<ValidatedRecipe, RegistrationError> {
        let options = Arc::new((self.decode_validate)(input, resources)?);
        let recipe = ComponentRecipe::from_validated(self.factory.clone(), options);
        Ok(ValidatedRecipe {
            legacy_connect_stage: "connect_component",
            descriptor: self.descriptor,
            recipe,
        })
    }
}

type InputAcceptance<Input> = Arc<dyn Fn(&Input) -> bool + Send + Sync>;

struct Entry<Input> {
    legacy_connect_stage: &'static str,
    feature: &'static str,
    descriptor: Option<ProtocolCapabilityDescriptor>,
    registration: Option<Arc<dyn Registration<Input>>>,
    accepts: Option<InputAcceptance<Input>>,
}

impl<Input> Clone for Entry<Input> {
    fn clone(&self) -> Self {
        Self {
            legacy_connect_stage: self.legacy_connect_stage,
            feature: self.feature,
            descriptor: self.descriptor,
            registration: self.registration.clone(),
            accepts: self.accepts.clone(),
        }
    }
}
impl<Input> Clone for ComponentRegistry<Input> {
    fn clone(&self) -> Self {
        Self {
            entries: self.entries.clone(),
        }
    }
}
pub struct ComponentRegistry<Input> {
    entries: BTreeMap<&'static str, Entry<Input>>,
}
impl<Input> Default for ComponentRegistry<Input> {
    fn default() -> Self {
        Self {
            entries: BTreeMap::new(),
        }
    }
}
impl<Input: 'static> ComponentRegistry<Input> {
    /// Record known but uncompiled names so they are distinguished from typos.
    pub fn declare(
        &mut self,
        protocol: &'static str,
        feature: &'static str,
    ) -> Result<(), RegistrationError> {
        if self.entries.contains_key(protocol) {
            return Err(RegistrationError::Duplicate(protocol));
        }
        self.entries.insert(
            protocol,
            Entry {
                legacy_connect_stage: "connect_component",
                feature,
                descriptor: None,
                registration: None,
                accepts: None,
            },
        );
        Ok(())
    }
    pub fn register<F, Decode>(
        &mut self,
        descriptor: ProtocolCapabilityDescriptor,
        factory: F,
        decode_validate: Decode,
    ) -> Result<(), RegistrationError>
    where
        F: ComponentFactory,
        Decode: Fn(&Input) -> Result<F::Options, ConfigError> + Send + Sync + 'static,
    {
        self.register_with_resources(
            descriptor,
            factory,
            move |input: &Input, _: &mut ResourcePreparation<'_>| {
                decode_validate(input).map_err(RegistrationError::Options)
            },
        )
    }

    pub fn register_with_resources<F, Decode>(
        &mut self,
        descriptor: ProtocolCapabilityDescriptor,
        factory: F,
        decode_validate: Decode,
    ) -> Result<(), RegistrationError>
    where
        F: ComponentFactory,
        Decode: Fn(&Input, &mut ResourcePreparation<'_>) -> Result<F::Options, RegistrationError>
            + Send
            + Sync
            + 'static,
    {
        let entry = self.entries.entry(descriptor.protocol).or_insert(Entry {
            legacy_connect_stage: "connect_component",
            feature: descriptor.feature,
            descriptor: None,
            registration: None,
            accepts: None,
        });
        if entry.registration.is_some() {
            return Err(RegistrationError::Duplicate(descriptor.protocol));
        }
        if entry.feature != descriptor.feature {
            return Err(RegistrationError::FeatureMismatch(descriptor.protocol));
        }
        entry.descriptor = Some(descriptor);
        entry.registration = Some(Arc::new(TypedRegistration {
            descriptor,
            factory: Arc::new(factory),
            decode_validate,
        }));
        Ok(())
    }

    pub(crate) fn set_legacy_connect_stage(&mut self, protocol: &str, stage: &'static str) {
        self.entries
            .get_mut(protocol)
            .expect("registered protocol")
            .legacy_connect_stage = stage;
    }
    pub fn has_factory(&self, protocol: &str) -> bool {
        self.entries
            .get(protocol)
            .is_some_and(|entry| entry.registration.is_some())
    }

    pub fn descriptors(&self) -> impl Iterator<Item = ProtocolCapabilityDescriptor> + '_ {
        self.entries.values().filter_map(|entry| entry.descriptor)
    }
    pub fn prepare(
        &self,
        protocol: &str,
        input: &Input,
    ) -> Result<ValidatedRecipe, RegistrationError> {
        let mut unavailable = || {
            Err(invalid(
                Reason::InvalidConfig,
                "shared resources require a host ComponentId allocator",
            ))
        };
        let mut resources = ResourcePreparation::new(&mut unavailable);
        self.prepare_with_resources(protocol, input, &mut resources)
    }

    pub fn prepare_with_resources(
        &self,
        protocol: &str,
        input: &Input,
        resources: &mut ResourcePreparation<'_>,
    ) -> Result<ValidatedRecipe, RegistrationError> {
        let result = self.prepare_entry(protocol, input, resources);
        if result.is_err() {
            resources.invalidate();
        }
        result
    }

    fn prepare_entry(
        &self,
        protocol: &str,
        input: &Input,
        resources: &mut ResourcePreparation<'_>,
    ) -> Result<ValidatedRecipe, RegistrationError> {
        resources.ensure_valid()?;
        let (name, entry) = self
            .entries
            .get_key_value(protocol)
            .ok_or_else(|| RegistrationError::Unknown(protocol.to_owned()))?;
        let registration =
            entry
                .registration
                .as_ref()
                .ok_or(RegistrationError::FeatureDisabled {
                    protocol: name,
                    feature: entry.feature,
                })?;
        if entry
            .accepts
            .as_ref()
            .is_some_and(|accepts| !accepts(input))
        {
            return Err(RegistrationError::Options(ConfigError::InvalidOutbound(
                format!("protocol `{name}` input is not selected for native component execution"),
            )));
        }
        registration.prepare(input, resources).map(|mut recipe| {
            recipe.legacy_connect_stage = entry.legacy_connect_stage;
            recipe
        })
    }
}
