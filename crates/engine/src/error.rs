use std::io;

use thiserror::Error;
use zero_config::ConfigError;
use zero_core::Error as CoreError;
use zero_error::RuntimeError;

#[derive(Debug, Error)]
pub enum EngineError {
    #[error(transparent)]
    Config(#[from] ConfigError),
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Core(#[from] CoreError),
    #[error("no inbound listeners are configured")]
    NoInbounds,
    #[error(
        "{kind} `{tag}` uses protocol `{protocol}` but this binary was built without Cargo feature `{feature}`"
    )]
    CompiledFeatureDisabled {
        kind: &'static str,
        tag: String,
        protocol: &'static str,
        feature: &'static str,
    },
    #[error("route or mode references target tag `{tag}` but no such outbound or group exists")]
    MissingRouteTarget { tag: String },
    #[error("engine plan is invalid: {message}")]
    InvalidPlan { message: String },
    #[error("inbound task exited unexpectedly")]
    InboundTaskExited,
    #[error("url_test group `{tag}` is invalid: {message}")]
    InvalidUrlTestGroup { tag: String, message: String },
    #[error("url_test task exited unexpectedly")]
    UrlTestTaskExited,
    #[error("selector group `{tag}` does not exist")]
    SelectorGroupNotFound { tag: String },
    #[error("group `{tag}` is not a selector group")]
    SelectorGroupTypeMismatch { tag: String },
    #[error("selector group `{group_tag}` does not contain target `{target_tag}`")]
    SelectorTargetNotFound {
        group_tag: String,
        target_tag: String,
    },
    #[error("outbound `{tag}` is temporarily unhealthy")]
    UnhealthyOutbound { tag: String },
    #[error("Fake-IP reverse mapping is missing for synthetic target `{address}`")]
    FakeIpReverseMissing { address: String },
    #[error("flow admission denied: {reason}")]
    AdmissionDenied { reason: String },
}

impl EngineError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::Config(_) => "config",
            Self::Io(_) => "io",
            Self::Core(_) => "core",
            Self::NoInbounds => "no_inbounds",
            Self::CompiledFeatureDisabled { .. } => "compiled_feature_disabled",
            Self::MissingRouteTarget { .. } => "missing_route_target",
            Self::InvalidPlan { .. } => "invalid_plan",
            Self::InboundTaskExited => "inbound_task_exited",
            Self::InvalidUrlTestGroup { .. } => "invalid_url_test_group",
            Self::UrlTestTaskExited => "url_test_task_exited",
            Self::SelectorGroupNotFound { .. } => "selector_group_not_found",
            Self::SelectorGroupTypeMismatch { .. } => "selector_group_type_mismatch",
            Self::SelectorTargetNotFound { .. } => "selector_target_not_found",
            Self::UnhealthyOutbound { .. } => "unhealthy_outbound",
            Self::FakeIpReverseMissing { .. } => "fake_ip_reverse_missing",
            Self::AdmissionDenied { .. } => "admission_denied",
        }
    }
}

impl From<RuntimeError> for EngineError {
    fn from(error: RuntimeError) -> Self {
        match error {
            RuntimeError::Io(error) => Self::Io(error),
            RuntimeError::Core(error) => Self::Core(error),
        }
    }
}

impl From<EngineError> for RuntimeError {
    fn from(error: EngineError) -> Self {
        match error {
            EngineError::Io(error) => Self::Io(error),
            EngineError::Core(error) => Self::Core(error),
            other => Self::Io(io::Error::other(other.to_string())),
        }
    }
}
