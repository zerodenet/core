//! Write-ahead route intent in the same journal's admitted row storage.
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
        // Admit confirmation storage before native mutation. On success the
        // same paid string moves from pending to installed, with no allocation.
        self._metadata
            .reserve_slot(&mut self.installed)
            .map_err(io::Error::other)?;
        let original = self._metadata.string(prefix).map_err(io::Error::other)?;
        if let Err((source, original)) = self._metadata.push(&mut self.pending, original) {
            self._metadata
                .release_string(original)
                .map_err(io::Error::other)?;
            return Err(io::Error::other(source));
        }
        // A previously confirmed route was observed missing before repair.
        // Its old proof cannot authorize deletion of this new acquisition.
        while let Some(index) = self.installed.iter().position(|value| value == prefix) {
            let retired = self.installed.remove(index);
            self._metadata
                .release_string(retired)
                .map_err(io::Error::other)?;
        }
        self.persist()?;
        install()?;
        let original = self
            .pending
            .pop()
            .expect("same synchronous pending route intent");
        self.installed.push(original);
        self.persist()
    }
    pub(in crate::route) fn cleanup_pending_record_with(
        &mut self,
        mut confirm_absent: impl FnMut(&RouteJournal, &str) -> io::Result<()>,
    ) -> io::Result<()> {
        let mut first = None;
        for index in (0..self.pending.len()).rev() {
            match confirm_absent(self, &self.pending[index]) {
                Ok(()) => {
                    let retired = self.pending.remove(index);
                    if let Err(source) = self._metadata.release_string(retired) {
                        first.get_or_insert_with(|| io::Error::other(source));
                    }
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
    #[cfg(test)]
    pub(in crate::route) fn cleanup_pending_with(
        &mut self,
        mut confirm_absent: impl FnMut(&str) -> io::Result<()>,
    ) -> io::Result<()> {
        self.cleanup_pending_record_with(|_, prefix| confirm_absent(prefix))
    }
}
