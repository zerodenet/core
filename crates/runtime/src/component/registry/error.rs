use std::{error::Error, fmt};
use zero_config::ConfigError;
use zero_platform_tokio::contract::lifecycle::BuildError;

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
    Planning(BuildError),
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
            Self::Planning(error) => error.fmt(f),
        }
    }
}
impl Error for RegistrationError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Options(error) => Some(error),
            Self::Planning(error) => Some(error),
            _ => None,
        }
    }
}
impl From<ConfigError> for RegistrationError {
    fn from(error: ConfigError) -> Self {
        Self::Options(error)
    }
}
impl From<BuildError> for RegistrationError {
    fn from(error: BuildError) -> Self {
        Self::Planning(error)
    }
}
