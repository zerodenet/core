//! Reverse original callbacks and incremental persistence without cloning rows.
use super::*;
impl RouteJournal {
    pub(in crate::route) fn cleanup(
        &mut self,
        mut remove_route: impl FnMut(&str) -> io::Result<()>,
        mut remove_exclusion: impl FnMut(IpAddr) -> io::Result<()>,
    ) -> io::Result<()> {
        self.cleanup_record(
            |_, prefix| remove_route(prefix),
            |_, peer| remove_exclusion(peer),
        )
    }
    /// Only read the original journal's scalar data for each existing callback;
    /// this is not another resource/provider or permission-bearing context.
    pub(in crate::route) fn cleanup_record(
        &mut self,
        mut remove_route: impl FnMut(&RouteJournal, &str) -> io::Result<()>,
        mut remove_exclusion: impl FnMut(&RouteJournal, IpAddr) -> io::Result<()>,
    ) -> io::Result<()> {
        let mut first_error = None;
        // The prefix remains in its one actual allocated table. The processed
        // suffix partitions failures then successes, preserving every original
        // reverse callback (including duplicates) with no snapshot allocation.
        let mut cursor = self.installed.len();
        while cursor != 0 {
            cursor -= 1;
            match remove_route(self, &self.installed[cursor]) {
                Ok(()) => {
                    let end = self.installed.len() - self._removed_routes;
                    self.installed[cursor..end].rotate_left(1);
                    self._removed_routes += 1;
                    if let Err(error) = self.persist_or_clear() {
                        first_error.get_or_insert(error);
                    }
                }
                Err(error) => {
                    first_error.get_or_insert(error);
                }
            }
        }
        let split = self.installed.len() - self._removed_routes;
        let mut kept = 0;
        for index in 0..split {
            let (failed, successful) = self.installed.split_at_mut(split);
            if !successful.contains(&failed[index]) {
                failed.swap(kept, index);
                kept += 1;
            }
        }
        for value in self.installed.drain(kept..) {
            if let Err(error) = self._metadata.release_string(value) {
                first_error.get_or_insert_with(|| io::Error::other(error));
            }
        }
        self._removed_routes = 0;
        let mut cursor = self.excluded.len();
        while cursor != 0 {
            cursor -= 1;
            match remove_exclusion(self, self.excluded[cursor]) {
                Ok(()) => {
                    let end = self.excluded.len() - self._removed_exclusions;
                    self.excluded[cursor..end].rotate_left(1);
                    self._removed_exclusions += 1;
                    if let Err(error) = self.persist_or_clear() {
                        first_error.get_or_insert(error);
                    }
                }
                Err(error) => {
                    first_error.get_or_insert(error);
                }
            }
        }
        let split = self.excluded.len() - self._removed_exclusions;
        let mut kept = 0;
        for index in 0..split {
            let (failed, successful) = self.excluded.split_at_mut(split);
            if !successful.contains(&failed[index]) {
                failed.swap(kept, index);
                kept += 1;
            }
        }
        self.excluded.truncate(kept);
        self._removed_exclusions = 0;
        if self.installed.is_empty() && self.excluded.is_empty() && !self.scoped_bypass {
            if let Err(error) = self.clear() {
                first_error.get_or_insert(error);
            }
        } else if let Err(error) = self.persist() {
            first_error.get_or_insert(error);
        }
        first_error.map_or(Ok(()), Err)
    }
}

#[cfg(test)]
mod tests;
