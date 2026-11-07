//! The CLI report stays V1; its terminal error keeps every original IO fault.
use std::{io, sync::Arc};
#[derive(Debug)]
pub(super) struct InspectionFailure(pub(super) Vec<Arc<io::Error>>);
impl std::fmt::Display for InspectionFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("connector persistent state is incompatible with this build")
    }
}
impl std::error::Error for InspectionFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0.first().map(|source| source.as_ref() as _)
    }
}
