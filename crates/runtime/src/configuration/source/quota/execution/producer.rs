//! Only this Source's quota flush callback; no task/IO/ACK interface escapes.
use super::*;
use zero_traits::packet_metadata::PacketMetadataLayout;

pub(super) struct Producer {
    value: Option<Arc<dyn Fn() -> io::Result<()> + Send + Sync>>,
    storage: Option<Arc<QuotaMemoryCredit>>,
}
impl Producer {
    pub(super) fn new<F>(memory: &QuotaMemoryBudget, work: F) -> io::Result<Self>
    where
        F: Fn() -> io::Result<()> + Send + Sync + 'static,
    {
        let layout = PacketMetadataLayout::arc::<F>()
            .and_then(|layout| {
                layout.checked_add(PacketMetadataLayout::arc::<QuotaMemoryCredit>()?)
            })
            .map_err(io::Error::other)?;
        let storage = memory.reserve(layout)?;
        // Erase the same allocation, not a new callback/task/Box registry.
        let value: Arc<dyn Fn() -> io::Result<()> + Send + Sync> = Arc::new(work);
        Ok(Self {
            value: Some(value),
            storage: Some(Arc::new(storage)),
        })
    }
    pub(super) fn write(&self) -> io::Result<()> {
        self.value.as_ref().unwrap()()
    }
}
impl Clone for Producer {
    fn clone(&self) -> Self {
        Self {
            value: self.value.clone(),
            storage: self.storage.clone(),
        }
    }
}
impl Drop for Producer {
    fn drop(&mut self) {
        // The final attachment's own Arc allocation must also be deallocated
        // before its credit can return; concurrent clones have one winner.
        drop(self.value.take());
        if let Some(storage) = self.storage.take() {
            drop(Arc::into_inner(storage));
        }
    }
}
