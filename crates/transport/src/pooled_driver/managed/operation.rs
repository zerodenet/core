use super::*;
pub struct Operation(pub(super) Arc<ManagedPool>);
impl Drop for Operation {
    fn drop(&mut self) {
        self.0.operations.fetch_sub(1, Ordering::AcqRel);
        self.0.changed.notify_waiters();
    }
}

impl ManagedPool {
    pub fn check(&self, profile: &str) -> Result<(), crate::RuntimeError> {
        if !self.ready() || self.options.profile.as_ref() != profile {
            return Err(std::io::Error::other(failure(
                self.instance,
                Stage::Prepare,
                Reason::StalePlan,
            ))
            .into());
        }
        Ok(())
    }
}

impl ManagedPool {
    pub fn instance(&self) -> ComponentInstanceId {
        self.instance
    }
}
