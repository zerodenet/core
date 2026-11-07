//! Retained TLS pools only borrow the actual current runtime ECH resolver.
use super::BackgroundNetwork;
use std::{io, sync::Arc};
use zero_transport::tls::ech::{EchConfigResolveFuture, EchConfigResolver};
pub(super) struct CurrentEch {
    pub(super) network: Arc<BackgroundNetwork>,
    pub(super) instance: zero_core::component::ComponentInstanceId,
}
impl CurrentEch {
    fn current(&self) -> io::Result<Arc<dyn EchConfigResolver>> {
        let source = self.network.current_services()?;
        let services = source.runtime_services()?;
        let plan = services
            .components
            .as_ref()
            .map_err(|source| io::Error::other(source.clone()))?;
        plan.authorize_carrier_prefix(self.instance, &[])
            .map_err(io::Error::other)?;
        Ok(services.upstream().ech_resolver())
    }
}
impl EchConfigResolver for CurrentEch {
    fn resolve(
        &self,
        server: String,
        query_name: String,
        force_query: zero_traits::EchForceQuery,
    ) -> EchConfigResolveFuture {
        let resolver = self.current();
        let network = self.network.clone();
        let instance = self.instance;
        let generation = network.egress.generation();
        Box::pin(async move {
            let material = resolver?.resolve(server, query_name, force_query).await?;
            if network.egress.generation() != generation {
                return Err(io::Error::new(
                    io::ErrorKind::Interrupted,
                    "ECH transport network changed",
                ));
            }
            // This is a current-generation/actual-owner check, not a native
            // grant. The Node query owns its original sockets and joins.
            drop(CurrentEch { network, instance }.current()?);
            Ok(material)
        })
    }
}

/// Borrow the existing current resolver/cache for this same actual owner.
pub(crate) fn resolver(
    network: Arc<BackgroundNetwork>,
    instance: zero_core::component::ComponentInstanceId,
) -> Arc<dyn EchConfigResolver> {
    Arc::new(CurrentEch { network, instance })
}
