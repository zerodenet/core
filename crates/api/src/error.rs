use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::Permission;

/// A single structured diagnostic attached to an [`ApiError`].
///
/// Carries a machine-usable `field_path` (e.g. `"inbounds[0].protocol"`,
/// `"route.rules[2]"`) plus a human-readable `message`, so GUIs can render
/// validation errors next to the offending form field instead of showing
/// one opaque message. An error may carry multiple details (e.g. when a
/// config validation collects several field errors); single-error cases
/// carry one entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorDetail {
    /// Dotted/indices field path the diagnostic applies to. Omitted when
    /// the error is not field-specific.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub field_path: Option<String>,
    /// Human-readable explanation of this specific diagnostic.
    pub message: String,
}

impl ErrorDetail {
    pub fn new(field_path: Option<impl Into<String>>, message: impl Into<String>) -> Self {
        Self {
            field_path: field_path.map(Into::into),
            message: message.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error, Serialize, Deserialize)]
#[error("{code}: {message}")]
pub struct ApiError {
    pub code: ApiErrorCode,
    pub message: String,
    pub field_path: Option<String>,
    pub cause: Option<String>,
    /// Structured, field-level diagnostics. Empty for non-validation
    /// errors. On the wire only when non-empty (`skip_serializing_if`).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub details: Vec<ErrorDetail>,
}

impl ApiError {
    pub fn new(code: ApiErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            field_path: None,
            cause: None,
            details: Vec::new(),
        }
    }

    pub fn permission_denied(required: Permission) -> Self {
        Self::new(
            ApiErrorCode::PermissionDenied,
            format!("permission `{required:?}` is required"),
        )
    }

    /// Attach a single structured diagnostic (consuming and returning self).
    pub fn with_detail(mut self, detail: ErrorDetail) -> Self {
        self.details.push(detail);
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApiErrorCode {
    #[error("not_found")]
    NotFound,
    #[error("invalid_argument")]
    InvalidArgument,
    #[error("permission_denied")]
    PermissionDenied,
    #[error("insufficient_os_privilege")]
    InsufficientOsPrivilege,
    #[error("feature_disabled")]
    FeatureDisabled,
    #[error("conflict")]
    Conflict,
    #[error("unsupported")]
    Unsupported,
    #[error("internal")]
    Internal,
}

impl ApiErrorCode {
    /// Stable string code used in JSON error responses.
    /// Returns snake_case, matching the serde wire format.
    pub fn as_code_str(&self) -> &'static str {
        match self {
            Self::NotFound => "not_found",
            Self::InvalidArgument => "invalid_argument",
            Self::PermissionDenied => "permission_denied",
            Self::InsufficientOsPrivilege => "insufficient_os_privilege",
            Self::FeatureDisabled => "feature_disabled",
            Self::Conflict => "conflict",
            Self::Unsupported => "unsupported",
            Self::Internal => "internal",
        }
    }
}
