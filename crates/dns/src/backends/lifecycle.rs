//! Exact persistent resources of one actual backend. Query coordinators and
//! Fake-IP journals have separate lifetimes; these methods do not close them.
use super::*;
impl ResolverBackend {
    pub(crate) fn has_persistent_resources(&self) -> bool {
        match self {
            #[cfg(feature = "doh")]
            Self::Doh(_) => true,
            #[cfg(feature = "doq")]
            Self::Doq(_) => true,
            _ => false,
        }
    }
    pub(crate) fn resource_admission_open(&self) -> bool {
        match self {
            #[cfg(feature = "doh")]
            Self::Doh(resolver) => resolver.owner.resource_admission_open(),
            #[cfg(feature = "doq")]
            Self::Doq(resolver) => resolver.resource_admission_open(),
            _ => true,
        }
    }
    pub(crate) fn retire_resource_admission(&self) {
        match self {
            #[cfg(feature = "doh")]
            Self::Doh(resolver) => resolver.owner.retire(),
            #[cfg(feature = "doq")]
            Self::Doq(resolver) => resolver.retire_resources(),
            _ => {}
        }
    }
    pub(crate) async fn shutdown_resources(
        &self,
        deadline: tokio::time::Instant,
    ) -> io::Result<()> {
        match self {
            #[cfg(feature = "doh")]
            Self::Doh(resolver) => resolver.owner.shutdown(deadline).await,
            #[cfg(feature = "doq")]
            Self::Doq(resolver) => resolver.shutdown_resources(deadline).await,
            _ => {
                let _ = deadline;
                Ok(())
            }
        }
    }
}
