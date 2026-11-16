//! Write-ahead route intent is distinct from confirmed installation.
use super::*;
impl RouteJournal {
    pub(in crate::route) fn install_route_with(
        &mut self,
        prefix: &str,
        install: impl FnOnce() -> io::Result<()>,
    ) -> io::Result<()> {
        if self.pending.iter().any(|value| value == prefix) {
            return Err(io::Error::other(format!(
                "unconfirmed route intent {prefix} in {}; resolve it before another acquisition",
                self.path.display()
            )));
        }
        // A previously installed route was observed missing before repair.
        // This new acquisition needs its own durable intent, not old proof.
        self.installed.retain(|value| value != prefix);
        self.pending.push(prefix.to_owned());
        self.persist()?;
        install()?;
        self.pending.retain(|value| value != prefix);
        self.record_route(prefix)
    }
    pub(in crate::route) fn cleanup_pending_with(
        &mut self,
        mut confirm_absent: impl FnMut(&str) -> io::Result<()>,
    ) -> io::Result<()> {
        let mut first = None;
        for prefix in self.pending.clone() {
            match confirm_absent(&prefix) {
                Ok(()) => {
                    self.pending.retain(|value| value != &prefix);
                    if let Err(source) = self.persist_or_clear() {
                        first.get_or_insert(source);
                    }
                }
                Err(source) => {
                    first.get_or_insert(source);
                }
            }
        }
        first.map_or(Ok(()), Err)
    }
}
