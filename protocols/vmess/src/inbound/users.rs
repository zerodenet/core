//! A parsed authorization snapshot. Construction never publishes live users.
use super::{IntoVmessInboundUserConfig, VmessInboundProfile, VmessUser};
use std::sync::{Arc, RwLock};
use zero_core::Error;
#[derive(Clone)]
pub struct PreparedVmessInboundUsers(Arc<[VmessUser]>);
impl std::fmt::Debug for PreparedVmessInboundUsers {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PreparedVmessInboundUsers")
            .field("user_count", &self.0.len())
            .finish_non_exhaustive()
    }
}
impl PreparedVmessInboundUsers {
    pub fn from_config_users<I, U>(users: I) -> Result<Self, Error>
    where
        I: IntoIterator<Item = U>,
        U: IntoVmessInboundUserConfig,
    {
        let users = users
            .into_iter()
            .map(IntoVmessInboundUserConfig::into_vmess_inbound_user_config)
            .map(
                |(
                    id,
                    cipher,
                    principal_key,
                    up_bps,
                    down_bps,
                    device_limit,
                    quota_remaining_bytes,
                    policy_revision,
                )| {
                    VmessUser::from_config(
                        &id,
                        &cipher,
                        principal_key,
                        up_bps,
                        down_bps,
                        device_limit,
                        quota_remaining_bytes,
                        policy_revision,
                    )
                },
            )
            .collect::<Result<Vec<_>, Error>>()?;
        Ok(Self(users.into()))
    }
    pub(crate) fn into_profile(self) -> VmessInboundProfile {
        VmessInboundProfile {
            users: Arc::new(RwLock::new(self.0)),
        }
    }
    pub(crate) fn publish(self, profile: &VmessInboundProfile) {
        *profile
            .users
            .write()
            .unwrap_or_else(|error| error.into_inner()) = self.0;
    }
}
