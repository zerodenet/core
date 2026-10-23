use super::*;
pub struct WeakDrivers<O: DriverKind, B: Send + Sync + 'static>(pub(super) Weak<Shared<O, B>>);
impl<O: DriverKind, B: Send + Sync + 'static> WeakDrivers<O, B> {
    pub fn upgrade(&self) -> Option<DriverOwner<O, B>> {
        self.0.upgrade().map(|shared| DriverOwner { shared })
    }
}
pub struct Admission<O: DriverKind, B: Send + Sync + 'static>(pub(super) Weak<Shared<O, B>>);
impl<O: DriverKind, B: Send + Sync + 'static> Clone for Admission<O, B> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}
impl<O: DriverKind, B: Send + Sync + 'static> Admission<O, B> {
    pub fn check(&self) -> Result<(), ChannelError> {
        let owner = self
            .0
            .upgrade()
            .ok_or_else(|| ChannelError::new(Stage::Prepare, Reason::Revoked, Impact::Component))?;
        let state = owner.state.lock().unwrap();
        if let Some(source) = &state.rejected {
            let reason = match source.reason {
                DriverRegistrationReason::LocalCapacity
                | DriverRegistrationReason::Scope(SpawnError::Capacity) => Reason::Capacity,
                DriverRegistrationReason::Retired
                | DriverRegistrationReason::Scope(SpawnError::Closing) => Reason::Revoked,
                _ => Reason::Internal,
            };
            return Err(ChannelError::new(Stage::Prepare, reason, Impact::Component)
                .with_source(SharedRejection(source.clone())));
        }
        if state.retired {
            return Err(ChannelError::new(
                Stage::Prepare,
                Reason::Revoked,
                Impact::Component,
            ));
        }
        Ok(())
    }
}
#[derive(Debug)]
struct SharedRejection<O: DriverKind>(Arc<DriverRegistrationFailure<O>>);
impl<O: DriverKind> fmt::Display for SharedRejection<O> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}
impl<O: DriverKind> std::error::Error for SharedRejection<O> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}

impl<O: DriverKind, B: Send + Sync + 'static> super::super::UserAdmission for Admission<O, B> {
    fn check(&self) -> Result<(), ChannelError> {
        Admission::check(self)
    }
}
