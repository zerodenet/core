//! Source fences and exact original logical controls, without native close claims.
use super::*;
impl AcceptedCarrierProvider {
    /// Fence this actual Source before stopping native IO. Existing views keep
    /// their real leases until consuming owners have joined and returned them.
    pub fn fence_source(&self, source: ComponentInstanceId) -> bool {
        let mut retiring = self.retiring_sources.lock().unwrap();
        let originals = self.originals.lock().unwrap();
        let owns = originals.iter().any(|row| row.source == source);
        retiring.insert(source);
        owns
    }
    pub fn release_source(&self, source: ComponentInstanceId) {
        let originals = self.originals.lock().unwrap().clone();
        if originals
            .iter()
            .filter(|row| row.source == source)
            .all(Self::settled)
        {
            self.retiring_sources.lock().unwrap().remove(&source);
        }
    }
    pub async fn stop_source_originals(
        &self,
        source: ComponentInstanceId,
        end: tokio::time::Instant,
    ) -> Result<(), ChannelError> {
        if !self.source_views_returned(source) {
            return Err(failure());
        }
        let originals = self.originals.lock().unwrap().clone();
        for row in originals {
            if row.source == source {
                row.control.request_stop(end).await?;
            }
        }
        Ok(())
    }
    pub fn source_originals_returned(&self, source: ComponentInstanceId) -> bool {
        let originals = self.originals.lock().unwrap().clone();
        originals
            .iter()
            .filter(|row| row.source == source)
            .all(Self::settled)
    }
    pub fn source_views_returned(&self, source: ComponentInstanceId) -> bool {
        let originals = self.originals.lock().unwrap().clone();
        originals
            .iter()
            .filter(|row| row.source == source)
            .all(|row| row.uses.load(std::sync::atomic::Ordering::Acquire) & USE_COUNT_MASK == 0)
    }
}
