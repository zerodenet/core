//! Same actual DoH owners retain explicit endpoint backends across questions.
use super::*;
impl TransportOwners {
    pub(crate) fn doh_endpoint(
        &self,
        key: crate::ech::pool::DohKey,
        build: impl FnOnce() -> io::Result<Arc<crate::backends::ResolverBackend>>,
    ) -> io::Result<Arc<crate::backends::ResolverBackend>> {
        let mut state = self.doh.lock().expect("DNS DoH owners poisoned");
        if state.closing {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "DNS DoH resources are closing",
            ));
        }
        state
            .endpoints
            .retain(|(_, backend)| match backend.as_ref() {
                crate::backends::ResolverBackend::Doh(resolver) => {
                    !resolver.owner.drained() || backend.resource_admission_open()
                }
                _ => false,
            });
        if let Some((_, backend)) = state
            .endpoints
            .iter()
            .find(|(current, backend)| *current == key && backend.resource_admission_open())
        {
            return Ok(backend.clone());
        }
        // Construction is cold and synchronous under the original registry's
        // one lock. Concurrent callers cannot publish duplicate endpoint pools.
        let backend = build()?;
        let crate::backends::ResolverBackend::Doh(resolver) = backend.as_ref() else {
            return Err(io::Error::other("DoH endpoint built another transport"));
        };
        state.owners.push(resolver.owner.clone());
        state.endpoints.push((key, backend.clone()));
        Ok(backend)
    }
}
