//! Synchronous embedded publication over this source's existing transaction.
use super::*;
use std::{error::Error, sync::Arc};

#[derive(Debug)]
struct PublicationRecoveryFailure<E> {
    original: E,
    recovery: SourcePersistError,
}
impl<E: std::fmt::Display> std::fmt::Display for PublicationRecoveryFailure<E> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "embedded publication failed: {}; source recovery remains unconfirmed: {}",
            self.original, self.recovery
        )
    }
}
impl<E: Error + 'static> Error for PublicationRecoveryFailure<E> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.original)
    }
}
impl ConfigurationSource {
    /// Commit one already-prepared embedded domain input. The synchronous
    /// callback installs only domain state and must not re-enter this source
    /// or perform resource preparation. It runs under the original source and
    /// cold-preparation locks; there is no second host, worker or I/O owner.
    pub fn commit_embedded<E>(
        self: &Arc<Self>,
        config: &RuntimeConfig,
        persist: bool,
        identity: &str,
        install: impl FnOnce() -> Result<(), E>,
    ) -> Result<(), E>
    where
        E: Error + From<io::Error> + Send + Sync + 'static,
    {
        super::super::startup::with_source(self.clone(), || {
            self.ensure_resource_io_available().map_err(E::from)?;
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            self.ensure_state_available(&mut state)
                .map_err(|error| E::from(io::Error::from(error)))?;
            if persist {
                if let Err(original) = self.persist_ready_locked(config, identity, &mut state) {
                    let disposition = original.disposition;
                    let original = io::Error::from(original);
                    if disposition != SourceDisposition::Unknown {
                        if let Err(recovery) = self.rollback_pending_locked(&mut state) {
                            return Err(E::from(
                                model::SourceRecoveryFailure {
                                    stage: "embedded source persistence",
                                    original,
                                    recovery: recovery.into(),
                                }
                                .into_io(),
                            ));
                        }
                    }
                    // A successful compensation does not turn the original
                    // persistence error into a successful domain publication.
                    return Err(E::from(original));
                }
            }
            if let Err(original) = install() {
                if persist {
                    if let Err(recovery) = self.rollback_pending_locked(&mut state) {
                        return Err(E::from(io::Error::other(PublicationRecoveryFailure {
                            original,
                            recovery,
                        })));
                    }
                }
                return Err(original);
            }
            if persist {
                self.finalize_committed_locked(&mut state)
                    .map_err(|error| E::from(io::Error::from(error)))?;
            }
            Ok(())
        })
        .map_err(E::from)?
    }
}

#[cfg(test)]
#[path = "../../tests/configuration_embedded.rs"]
mod tests;
