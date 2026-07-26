//! Principal-scoped policy, device, quota, and cancellation management.

mod cancellation;
mod device;
mod policy;
mod quota;

pub(crate) use cancellation::PrincipalCancellationRegistry;
pub(crate) use device::PrincipalDeviceRegistry;
pub(crate) use policy::PrincipalPolicyRegistry;
pub(crate) use quota::{PrincipalQuotaRegistration, PrincipalQuotaRegistry};

pub use cancellation::PrincipalCancellationRegistration;
pub use device::PrincipalDeviceRegistration;
pub use quota::{
    inspect_principal_quota_state, PrincipalQuotaStateReport, PrincipalQuotaStateStatus,
};
