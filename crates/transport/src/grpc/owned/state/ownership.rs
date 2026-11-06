//! Same actual pool mode; clearing a legacy debt requires real missing evidence.
use super::*;
impl GrpcPool {
    /// Cold mode admissibility observes real originals without clearing debt.
    pub fn validate_mode_candidate(
        &self,
        native: bool,
        legacy: bool,
    ) -> Result<(), crate::RuntimeError> {
        let old = {
            let owner = self.0.ownership.lock().unwrap();
            match &*owner {
                Ownership::LegacyUsed if native => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::PermissionDenied,
                        "gRPC legacy originals lack verified native/driver transfer",
                    )
                    .into())
                }
                Ownership::Managed(old) if legacy => Some(old.clone()),
                _ => None,
            }
        };
        if old.is_some_and(|old| !old.options.selected_background || !old.ready_to_replace()) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "gRPC native owner still retains original retirement obligations",
            )
            .into());
        }
        Ok(())
    }
    pub(in crate::grpc) fn enter_legacy(&self) -> Result<(), crate::RuntimeError> {
        let old = {
            let mut owner = self.0.ownership.lock().unwrap();
            match &*owner {
                Ownership::Pristine | Ownership::LegacyUsed => {
                    *owner = Ownership::LegacyUsed;
                    return Ok(());
                }
                Ownership::Managed(old) => old.clone(),
            }
        };
        if !old.options.selected_background || !old.ready_to_replace() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "gRPC pool belongs to its controlled native generation",
            )
            .into());
        }
        let previous = {
            let mut owner = self.0.ownership.lock().unwrap();
            if !matches!(&*owner,Ownership::Managed(actual) if Arc::ptr_eq(actual,&old)) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "gRPC pool owner changed before actual legacy entry",
                )
                .into());
            }
            self.0.retired.store(false, Ordering::Release);
            std::mem::replace(&mut *owner, Ownership::LegacyUsed)
        };
        drop(previous);
        Ok(())
    }
}
