//! Initial documents use the same retained source as subsequent preparation.
use super::*;
use std::sync::Arc;
use zero_config::ConfigError;

fn read_error(path: &Path, source: io::Error) -> ConfigError {
    ConfigError::ReadConfig {
        path: path.display().to_string(),
        source,
    }
}
impl ConfigurationSource {
    /// Standalone document loading uses the same retained original-file gate as
    /// initial kernel assembly. It grants no persistence path or runtime host.
    pub fn load_document(path: impl AsRef<Path>) -> Result<RuntimeConfig, ConfigError> {
        Arc::new(Self::default()).load_initial_configuration(path)
    }

    /// Load before domain or runtime construction. `self` is the actual owner
    /// handed onward to PreparedConfiguration; its optional persistence path
    /// is unchanged by reading a document at `path`.
    pub fn load_initial_configuration(
        self: &Arc<Self>,
        path: impl AsRef<Path>,
    ) -> Result<RuntimeConfig, ConfigError> {
        let path = path.as_ref();
        super::super::startup::with_source(self.clone(), || self.read_configuration_owned(path))
            .map_err(|source| read_error(path, source))?
    }

    /// Preserve the existing from_path recovery IO error boundary while using
    /// the same actual source owner as its subsequent document and rule reads.
    pub fn verify_initial_recovery(self: &Arc<Self>, path: impl AsRef<Path>) -> io::Result<()> {
        super::super::startup::with_source(self.clone(), || {
            self.verify_recovery_owned(path.as_ref())
        })?
    }

    fn read_configuration_owned(&self, path: &Path) -> Result<RuntimeConfig, ConfigError> {
        let raw = self
            .io
            .read_text(path)
            .map_err(|source| read_error(path, source))?;
        // Parsing only starts once the original document FD has a matching
        // native-close receipt. Preserve the public document path semantics.
        RuntimeConfig::parse_with_source_dir(&raw, path.parent().map(Path::to_path_buf))
    }

    /// A bounded inert material snapshot for a cold protocol recipe. The same
    /// finite configuration startup slot retains the actual original before
    /// reading. Only a matching native-close receipt permits returning bytes;
    /// an interrupted/unknown close blocks every subsequent preparation.
    /// This does not parse protocol material or construct a replacement carrier.
    pub fn read_preparation_file(path: &Path, max_bytes: usize) -> io::Result<Vec<u8>> {
        let source = Arc::new(Self::default());
        super::super::startup::with_source(source.clone(), || {
            source.io.read_limited(path, max_bytes)
        })?
    }

    /// Cold raw document readers (early logging and CLI submission) retain
    /// their own parse/fallback behavior. An unconfirmed original remains in
    /// the same finite startup slot and blocks later preparation/submission.
    pub fn read_initial_document(path: impl AsRef<Path>) -> io::Result<String> {
        let source = Arc::new(Self::default());
        super::super::startup::with_source(source.clone(), || source.io.read_text(path.as_ref()))?
    }
}

#[cfg(test)]
#[path = "../../tests/configuration_document.rs"]
mod tests;
