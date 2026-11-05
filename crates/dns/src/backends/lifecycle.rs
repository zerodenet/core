//! Exact persistent resources of one actual backend. Query coordinators and
//! Fake-IP journals have separate lifetimes; these methods do not close them.
use super::*;
impl ResolverBackend {
    pub(crate) fn has_persistent_resources(&self) -> bool {
        match self {
            Self::System(_) => true,
            #[cfg(feature = "doh")]
            Self::Doh(_) => true,
            #[cfg(feature = "doq")]
            Self::Doq(_) => true,
            #[cfg(any(feature = "udp", feature = "dot"))]
            _ => false,
        }
    }
    pub(crate) fn resource_admission_open(&self) -> bool {
        match self {
            Self::System(resolver) => resolver.is_open(),
            #[cfg(feature = "doh")]
            Self::Doh(resolver) => resolver.owner.resource_admission_open(),
            #[cfg(feature = "doq")]
            Self::Doq(resolver) => resolver.resource_admission_open(),
            #[cfg(any(feature = "udp", feature = "dot"))]
            _ => true,
        }
    }
    pub(crate) fn retire_resource_admission(&self) {
        match self {
            Self::System(resolver) => resolver.revoke(),
            #[cfg(feature = "doh")]
            Self::Doh(resolver) => resolver.owner.retire(),
            #[cfg(feature = "doq")]
            Self::Doq(resolver) => resolver.retire_resources(),
            #[cfg(any(feature = "udp", feature = "dot"))]
            _ => {}
        }
    }
    pub(crate) async fn shutdown_resources(
        &self,
        deadline: tokio::time::Instant,
    ) -> io::Result<()> {
        match self {
            Self::System(resolver) => resolver.shutdown(deadline).await,
            #[cfg(feature = "doh")]
            Self::Doh(resolver) => resolver.owner.shutdown(deadline).await,
            #[cfg(feature = "doq")]
            Self::Doq(resolver) => resolver.shutdown_resources(deadline).await,
            #[cfg(any(feature = "udp", feature = "dot"))]
            _ => {
                let _ = deadline;
                Ok(())
            }
        }
    }
}
