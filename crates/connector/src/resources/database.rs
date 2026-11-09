//! SQLite supplies the original close operation and returns the Connection on failure.
use super::*;
pub(crate) struct OriginalDatabase {
    acquiring: bool,
    pub(crate) connection: Option<rusqlite::Connection>,
    pub(crate) failure: Option<Arc<rusqlite::Error>>,
    pub(crate) scratch: Option<tempfile::TempDir>,
    scratch_failure: Option<Arc<io::Error>>,
    charge: Option<OriginalCharge>,
}
impl OriginalDatabase {
    pub(crate) fn retirement_complete(&self) -> bool {
        !self.acquiring && self.connection.is_none() && self.scratch.is_none()
    }
    pub(crate) fn close(&mut self) -> io::Result<()> {
        if self.acquiring {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "original database acquisition has no returned native outcome",
            ));
        }
        // SQLITE_BUSY returns this exact live Connection. A later original-user
        // return permits another close on that same Connection, never on a reused fd.
        if let Some(connection) = self.connection.take() {
            connection.flush_prepared_statement_cache();
            if let Err((connection, error)) = connection.close() {
                self.connection = Some(connection);
                let error = Arc::new(error);
                self.failure.get_or_insert_with(|| error.clone());
                return Err(io::Error::other(error));
            }
        }
        // Keep the actual scratch owner on failed removal. TempDir::close consumes
        // it even on failure, so its error alone cannot authorize a later success.
        if let Some(directory) = self.scratch.as_ref() {
            match std::fs::remove_dir_all(directory.path()) {
                Ok(()) => {}
                Err(error)
                    if error.kind() == io::ErrorKind::NotFound
                        && matches!(directory.path().symlink_metadata(), Err(error) if error.kind() == io::ErrorKind::NotFound) =>
                    {}
                Err(error) => {
                    let error = Arc::new(error);
                    self.scratch_failure.get_or_insert_with(|| error.clone());
                    return Err(io::Error::new(error.kind(), error));
                }
            }
            // Actual removal above is the evidence; this disables emergency cleanup.
            let _removed = self.scratch.take().unwrap().keep();
        }
        // Both the actual SQLite connection and original scratch directory are
        // now positively retired. Historical diagnostics remain independent.
        if let Some(charge) = self.charge.take() {
            charge.complete();
        }
        if let Some(error) = &self.failure {
            return Err(io::Error::other(error.clone()));
        }

        if let Some(error) = &self.scratch_failure {
            return Err(io::Error::new(error.kind(), error.clone()));
        }
        Ok(())
    }
}
impl DispatcherResources {
    pub(crate) fn database(
        &self,
        create: impl FnOnce() -> io::Result<tempfile::TempDir>,
    ) -> io::Result<Arc<Mutex<OriginalDatabase>>> {
        self.retire_databases(false)?;
        self.available()?;
        let charge = self.0.capacity.original()?;
        let original = Arc::new(Mutex::new(OriginalDatabase {
            acquiring: true,
            connection: None,
            failure: None,
            scratch: None,
            scratch_failure: None,
            charge: Some(charge),
        }));
        self.0.databases.lock().unwrap().push(original.clone());
        let directory = match create() {
            Ok(directory) => directory,
            Err(error) => {
                let charge = {
                    let mut state = original
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    state.acquiring = false;
                    state.charge.take()
                };
                self.0
                    .databases
                    .lock()
                    .unwrap()
                    .retain(|actual| !Arc::ptr_eq(actual, &original));
                if let Some(charge) = charge {
                    charge.complete();
                }
                return Err(error);
            }
        };
        let path = directory.path().join("index.sqlite");
        original
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .scratch = Some(directory);
        // The same installed owner keeps its directory even if opening SQLite
        // fails. Cleanup must explicitly remove it before returning the unit.
        let opened = rusqlite::Connection::open(path);
        let mut state = original
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.acquiring = false;
        match opened {
            Ok(connection) => state.connection = Some(connection),
            Err(error) => {
                if let Err(cleanup) = state.close() {
                    self.retain_failure(cleanup);
                }
                return Err(io::Error::other(error));
            }
        }
        drop(state);
        Ok(original)
    }
    pub(super) fn retire_databases(&self, require_all: bool) -> io::Result<()> {
        let mut databases = self.0.databases.lock().unwrap();
        let mut position = 0;
        let mut outstanding = false;
        while position < databases.len() {
            if Arc::strong_count(&databases[position]) != 1 {
                outstanding = true;
                position += 1;
                continue;
            }
            let (complete, result) = {
                let mut original = databases[position]
                    .lock()
                    .unwrap_or_else(|error| error.into_inner());
                let result = original.close();
                (original.retirement_complete(), result)
            };
            if complete {
                if let Err(error) = result {
                    self.retain_retired_failure(error);
                    // Retain this same now-closed database's distinct native
                    // and path faults, without keeping its old users/native alive.
                    self.0
                        .retired_databases
                        .lock()
                        .unwrap()
                        .push(databases[position].clone());
                }
                databases.remove(position);
            } else {
                if let Err(error) = result {
                    self.retain_failure(error);
                }
                position += 1;
            }
        }
        if require_all && outstanding {
            return Err(self.retain_failure(io::Error::other(
                "original SQLite connection still has a live index view",
            )));
        }
        self.available()
    }
}
