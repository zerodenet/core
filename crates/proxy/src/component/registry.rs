//! Typed cold-path registration. No input documents survive preparation.
use std::{collections::BTreeMap, error::Error, fmt, sync::Arc};
use zero_config::ConfigError;
use zero_platform_tokio::contract::{
    component::{BuildContext, ComponentFactory, ComponentInstance, ComponentSpec},
    lifecycle::BuildError,
};
use zero_traits::ProtocolCapabilityDescriptor;

#[derive(Debug)]
pub enum RegistrationError {
    Duplicate(&'static str),
    Unknown(String),
    FeatureDisabled {
        protocol: &'static str,
        feature: &'static str,
    },
    FeatureMismatch(&'static str),
    Options(ConfigError),
}
impl fmt::Display for RegistrationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Duplicate(kind) => write!(f, "protocol `{kind}` registered more than once"),
            Self::Unknown(kind) => write!(f, "unknown protocol `{kind}`"),
            Self::FeatureDisabled { protocol, feature } => write!(
                f,
                "protocol `{protocol}` requires Cargo feature `{feature}`"
            ),
            Self::FeatureMismatch(kind) => {
                write!(f, "protocol `{kind}` has inconsistent feature metadata")
            }
            Self::Options(error) => error.fmt(f),
        }
    }
}
impl Error for RegistrationError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Options(error) => Some(error),
            _ => None,
        }
    }
}

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

#[derive(Clone)]
pub struct ValidatedRecipe {
    descriptor: ProtocolCapabilityDescriptor,
    spec: Arc<ComponentSpec>,
    recipe: Arc<dyn BuildRecipe>,
}
impl ValidatedRecipe {
    pub fn descriptor(&self) -> ProtocolCapabilityDescriptor {
        self.descriptor
    }
    pub fn spec(&self) -> &ComponentSpec {
        &self.spec
    }
    /// The caller owns the stop-old/build-new ordering and installs the returned
    /// lifecycle owner before calling start. This method never starts resources.
    pub fn build(&self, context: BuildContext) -> Result<ComponentInstance, BuildError> {
        super::bindings::validate_ids(&self.spec, &context)?;
        let instance = self.recipe.build(context)?;
        // build is state-only, so no I/O has started if shape validation fails.
        super::bindings::validate_roles(&self.spec, &instance.roles)?;
        Ok(instance)
    }
}

trait Registration<Input>: Send + Sync {
    fn prepare(&self, input: &Input) -> Result<ValidatedRecipe, ConfigError>;
}
struct TypedRegistration<F, Decode> {
    descriptor: ProtocolCapabilityDescriptor,
    factory: Arc<F>,
    decode_validate: Decode,
}
impl<Input, F, Decode> Registration<Input> for TypedRegistration<F, Decode>
where
    F: ComponentFactory,
    Decode: Fn(&Input) -> Result<F::Options, ConfigError> + Send + Sync,
{
    fn prepare(&self, input: &Input) -> Result<ValidatedRecipe, ConfigError> {
        let options = Arc::new((self.decode_validate)(input)?);
        let spec = Arc::new(self.factory.describe(&options));
        Ok(ValidatedRecipe {
            descriptor: self.descriptor,
            spec,
            recipe: Arc::new(Prepared {
                factory: self.factory.clone(),
                options,
            }),
        })
    }
}

struct Entry<Input> {
    feature: &'static str,
    descriptor: Option<ProtocolCapabilityDescriptor>,
    registration: Option<Arc<dyn Registration<Input>>>,
}

impl<Input> Clone for Entry<Input> {
    fn clone(&self) -> Self {
        Self {
            feature: self.feature,
            descriptor: self.descriptor,
            registration: self.registration.clone(),
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
                feature,
                descriptor: None,
                registration: None,
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
        let entry = self.entries.entry(descriptor.protocol).or_insert(Entry {
            feature: descriptor.feature,
            descriptor: None,
            registration: None,
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
        registration
            .prepare(input)
            .map_err(RegistrationError::Options)
    }
}
