//! SQLite supplies the original close operation and returns the Connection on failure.
use super::*;
pub(crate) struct OriginalDatabase {
    pub(crate) connection: Option<rusqlite::Connection>,
    pub(crate) failure: Option<rusqlite::Error>,
    pub(crate) scratch: Option<tempfile::TempDir>,
}
impl OriginalDatabase {
    pub(crate) fn close(&mut self) -> io::Result<()> {
        if let Some(error) = &self.failure {
            return Err(io::Error::other(error.to_string()));
        }
        if let Some(connection) = self.connection.take() {
            connection.flush_prepared_statement_cache();
            if let Err((connection, error)) = connection.close() {
                self.connection = Some(connection);
                self.failure = Some(error);
                return Err(io::Error::other(
                    "original SQLite Connection.close failed; Connection retained",
                ));
            }
        }
        // No SQLite original handle remains before its private scratch path retires.
        if let Some(directory) = self.scratch.take() {
            directory.close()?;
        }
        Ok(())
    }
}
impl DispatcherResources {
    pub(crate) fn database(
        &self,
        directory: tempfile::TempDir,
    ) -> io::Result<Arc<Mutex<OriginalDatabase>>> {
        self.retire_databases(false)?;
        self.available()?;
        let connection = rusqlite::Connection::open(directory.path().join("index.sqlite"))
            .map_err(|error| io::Error::other(error.to_string()))?;
        let original = Arc::new(Mutex::new(OriginalDatabase {
            connection: Some(connection),
            failure: None,
            scratch: Some(directory),
        }));
        self.0.databases.lock().unwrap().push(original.clone());
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
            let result = databases[position]
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .close();
            match result {
                Ok(()) => {
                    databases.remove(position);
                }
                Err(error) => {
                    self.retain_failure(error);
                    position += 1;
                }
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
