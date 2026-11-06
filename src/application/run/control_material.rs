//! Initial TLS bytes use the actual configuration source and production limit.
use std::{
    io,
    path::{Path, PathBuf},
    sync::Arc,
};
#[derive(Debug)]
struct MaterialRead {
    label: &'static str,
    path: PathBuf,
    source: io::Error,
}
impl std::fmt::Display for MaterialRead {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "read gRPC {} `{}`: {}",
            self.label,
            self.path.display(),
            self.source
        )
    }
}
impl std::error::Error for MaterialRead {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}
pub(super) fn read(
    source: &Arc<zero_runtime::configuration::ConfigurationSource>,
    path: &Path,
    label: &'static str,
    max_bytes: usize,
) -> io::Result<Vec<u8>> {
    source
        .read_control_material(path, max_bytes)
        .map_err(|source| {
            io::Error::new(
                source.kind(),
                MaterialRead {
                    label,
                    path: path.to_owned(),
                    source,
                },
            )
        })
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn bounded_initial_material_keeps_same_source_close_barrier_and_original_error() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("material.pem");
        std::fs::write(&path, b"original-material").unwrap();
        let source = Arc::new(zero_runtime::configuration::ConfigurationSource::new(
            directory.path().join("zero.json"),
        ));
        let error = read(&source, &path, "TLS certificate", 1).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        let contextual = error
            .get_ref()
            .unwrap()
            .downcast_ref::<MaterialRead>()
            .unwrap();
        assert_eq!(contextual.source.kind(), io::ErrorKind::InvalidData);
        source.confirm_original_io_closed().unwrap();
        assert_eq!(
            read(&source, &path, "TLS certificate", 17).unwrap(),
            b"original-material"
        );
        source.confirm_original_io_closed().unwrap();
        let missing = read(
            &source,
            &directory.path().join("absent"),
            "TLS private key",
            17,
        )
        .unwrap_err();
        assert_eq!(missing.kind(), io::ErrorKind::NotFound);
        assert!(
            missing
                .get_ref()
                .unwrap()
                .downcast_ref::<MaterialRead>()
                .unwrap()
                .source
                .raw_os_error()
                .is_some()
        );
        source.confirm_original_io_closed().unwrap();
    }
}
