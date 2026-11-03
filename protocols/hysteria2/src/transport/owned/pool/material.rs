//! One recipe's immutable preparation bytes, released after real crypto startup.
use super::*;
type Material = Option<Arc<[u8]>>;
type Materials = Arc<[Material]>;
pub(super) type PreparedCa = Arc<Mutex<Option<Materials>>>;
impl PoolOptions {
    /// Assembly supplies bytes only after confirming its original File closes.
    /// Without a snapshot the existing factory still uses declared File access.
    pub fn with_prepared_ca(
        mut self,
        materials: Vec<Option<Arc<[u8]>>>,
    ) -> Result<Self, ChannelError> {
        if materials.len() != self.profiles.len()
            || self
                .profiles
                .iter()
                .zip(&materials)
                .any(|(profile, material)| profile.ca_cert_path().is_some() != material.is_some())
        {
            return Err(error(Stage::Prepare, Reason::InvalidConfig));
        }
        self.prepared_ca = Some(Arc::new(Mutex::new(Some(materials.into()))));
        Ok(self)
    }
}

impl Pool {
    /// Pure role binding to the assembly's already prepared shared resource.
    /// Actual dependency generation and profile identity are checked on use.
    pub fn bind_prepared_profile(
        self: &Arc<Self>,
        profile: usize,
        owner: ComponentId,
        identity: Arc<[u8]>,
    ) -> RoleOptions {
        RoleOptions {
            pool: Arc::downgrade(self),
            profile,
            owner,
            identity,
        }
    }
}
