use serde::{Deserialize, Serialize};

use crate::{
    ApiErrorCode, Permission, API_ID, CAPABILITIES_CONTRACT_VERSION, CONFIG_SCHEMA_VERSION,
    CONTROL_API_VERSION, ERROR_CODE_CONTRACT_VERSION, EVENT_SCHEMA_ID,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApiCapabilities {
    #[serde(default)]
    pub api_id: String,
    #[serde(default)]
    pub schema_id: String,
    /// Versioned compatibility contracts published by current cores.
    ///
    /// `None` is intentionally distinguishable from V1 so a client reading an
    /// older capabilities payload can choose conservative compatibility
    /// behavior instead of assuming the current contract.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub contracts: Option<ApiContractVersions>,
    /// Complete stable error-code catalog for the current error contract.
    #[serde(default)]
    pub error_codes: Vec<String>,
    /// Core-wide, machine-readable limitation codes.
    #[serde(default)]
    pub global_limitations: Vec<String>,
    #[serde(default)]
    pub adapters: Vec<AdapterCapability>,
    #[serde(default)]
    pub sinks: Vec<SinkCapability>,
    #[serde(default)]
    pub features: Vec<String>,
    #[serde(default)]
    pub protocols: Vec<ProtocolCapability>,
    /// Compiled cargo feature flags visible at runtime.
    #[serde(default)]
    pub build_features: Vec<String>,
    #[serde(default)]
    pub permissions: Vec<Permission>,
}

impl ApiCapabilities {
    pub fn new() -> Self {
        Self {
            api_id: API_ID.to_owned(),
            schema_id: EVENT_SCHEMA_ID.to_owned(),
            contracts: Some(ApiContractVersions::current()),
            error_codes: ApiErrorCode::ALL
                .iter()
                .map(|code| code.as_code_str().to_owned())
                .collect(),
            global_limitations: Vec::new(),
            adapters: Vec::new(),
            sinks: Vec::new(),
            features: Vec::new(),
            protocols: Vec::new(),
            build_features: Vec::new(),
            permissions: Vec::new(),
        }
    }
}

/// Inclusive compatibility range for one independently versioned contract.
///
/// Version zero is reserved for "unknown/not published" in deserialized
/// partial data. A current core always publishes non-zero values.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContractVersionRange {
    #[serde(default)]
    pub current: u32,
    #[serde(default)]
    pub minimum_supported: u32,
}

impl ContractVersionRange {
    const fn new(current: u32, minimum_supported: u32) -> Self {
        Self {
            current,
            minimum_supported,
        }
    }
}

/// Independently versioned public contracts consumed by clients/controllers.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApiContractVersions {
    #[serde(default)]
    pub capabilities: ContractVersionRange,
    #[serde(default)]
    pub control_api: ContractVersionRange,
    #[serde(default)]
    pub config_schema: ContractVersionRange,
    #[serde(default)]
    pub error_codes: ContractVersionRange,
}

impl ApiContractVersions {
    pub fn current() -> Self {
        Self {
            capabilities: ContractVersionRange::new(CAPABILITIES_CONTRACT_VERSION, 1),
            control_api: ContractVersionRange::new(CONTROL_API_VERSION, 1),
            config_schema: ContractVersionRange::new(CONFIG_SCHEMA_VERSION, 1),
            error_codes: ContractVersionRange::new(ERROR_CODE_CONTRACT_VERSION, 1),
        }
    }
}

impl Default for ApiCapabilities {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdapterCapability {
    pub kind: String,
    pub enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SinkCapability {
    pub kind: String,
    pub enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProtocolCapability {
    pub protocol: String,
    pub feature: String,
    pub compiled: bool,
    pub status: String,
    pub compatibility_baseline: String,
    pub inbound: ProtocolNetworkCapability,
    pub outbound: ProtocolNetworkCapability,
    #[serde(default)]
    pub transports: Vec<String>,
    pub mux: CapabilityState,
    #[serde(default)]
    pub limitations: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProtocolNetworkCapability {
    pub tcp: CapabilityState,
    pub udp: CapabilityState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapabilityState {
    pub supported: bool,
    pub level: String,
    #[serde(default)]
    pub notes: Vec<String>,
}

impl CapabilityState {
    pub fn supported() -> Self {
        Self {
            supported: true,
            level: "supported".to_owned(),
            notes: Vec::new(),
        }
    }

    pub fn partial(notes: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            supported: true,
            level: "partial".to_owned(),
            notes: notes.into_iter().map(Into::into).collect(),
        }
    }

    pub fn experimental(notes: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            supported: true,
            level: "experimental".to_owned(),
            notes: notes.into_iter().map(Into::into).collect(),
        }
    }

    pub fn unsupported(notes: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            supported: false,
            level: "unsupported".to_owned(),
            notes: notes.into_iter().map(Into::into).collect(),
        }
    }

    pub fn not_applicable() -> Self {
        Self {
            supported: false,
            level: "not_applicable".to_owned(),
            notes: Vec::new(),
        }
    }
}
