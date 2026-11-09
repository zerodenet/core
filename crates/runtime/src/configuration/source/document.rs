//! Initial documents use the same retained source as subsequent preparation.
use super::*;
use std::sync::Arc;
use zero_config::ConfigError;
mod input;
mod log;
mod probe;

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
        let raw = input::read(self, path, probe::Dialect::Config)
            .map_err(|source| {
                if probe::is_unknown(&source) {
                    // An indeterminate malformed budget fact is never permission
                    // for an unbounded read or a guessed smaller legal limit.
                    ConfigError::ParseConfig(serde_json::Error::io(source))
                } else {
                    read_error(path, source)
                }
            })?
            .0;
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

    /// Initial control material uses the kernel's actual retained source. This
    /// preserves existing byte semantics; parsing happens only after the same
    /// original file has a confirmed close outcome.
    pub fn read_initial_material(self: &Arc<Self>, path: &Path) -> io::Result<Vec<u8>> {
        super::super::startup::with_source(self.clone(), || {
            self.ensure_resource_io_available()?;
            self.io.read(path)
        })?
    }

    /// A caller with an explicit material budget uses that same original-file
    /// owner and close barrier, rather than opening a parallel file reader.
    pub fn read_control_material(
        self: &Arc<Self>,
        path: &Path,
        max_bytes: usize,
    ) -> io::Result<Vec<u8>> {
        super::super::startup::with_source(self.clone(), || {
            self.ensure_resource_io_available()?;
            self.io.read_limited(path, max_bytes)
        })?
    }

    /// The process's original early logging input. Whole-document Value
    /// construction is unnecessary; only an actual selected log/magic subtree
    /// is materialized, under this same document's configured cold envelope.
    pub fn read_initial_log_config(
        path: impl AsRef<Path>,
    ) -> io::Result<Option<zero_config::LogConfig>> {
        let source = Arc::new(Self::default());
        let (raw, facts) = super::super::startup::with_source(source.clone(), || {
            input::read(&source, path.as_ref(), probe::Dialect::Value)
        })??;
        log::decode(&raw, facts.request)
    }

    /// Genuine standalone raw readers retain their own parse behavior.
    /// The process early logger uses read_initial_log_config; CLI submission
    /// uses its explicit actual client frame/total Source snapshot instead. An unconfirmed original remains in
    /// the same finite startup slot and blocks later preparation/submission.
    pub fn read_initial_document(path: impl AsRef<Path>) -> io::Result<String> {
        let source = Arc::new(Self::default());
        super::super::startup::with_source(source.clone(), || source.io.read_text(path.as_ref()))?
    }
}

#[cfg(test)]
#[path = "../../tests/configuration_document.rs"]
mod tests;
