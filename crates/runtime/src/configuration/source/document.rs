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

    /// Early logging has no typed configuration yet. A normal read/UTF-8/JSON
    /// error may use its existing fallback, but an unconfirmed original remains
    /// in the same finite startup slot and blocks the later real initial load.
    pub fn read_initial_document(path: impl AsRef<Path>) -> io::Result<String> {
        let source = Arc::new(Self::default());
        super::super::startup::with_source(source.clone(), || source.io.read_text(path.as_ref()))?
    }
}

#[cfg(test)]
#[path = "../../tests/configuration_document.rs"]
mod tests;
