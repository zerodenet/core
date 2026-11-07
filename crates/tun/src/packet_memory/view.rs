//! Allocation-only projection of the same prepaid original memory grant. The
//! Host retains policy setters; consumers cannot construct a replacement Root.
use super::*;
use zero_traits::PacketBuffer;

#[derive(Clone)]
pub struct PacketMemoryView {
    pub(super) original: NativeReadGrant,
}
impl NativeReadGrant {
    pub fn memory_view(&self) -> Result<PacketMemoryView, PacketMemoryError> {
        self.check_memory_open()?;
        Ok(PacketMemoryView {
            original: self.clone(),
        })
    }
    fn check_memory_open(&self) -> Result<(), PacketMemoryError> {
        let state = self
            .0
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.revoked {
            Err(PacketMemoryError::Stopped)
        } else {
            Ok(())
        }
    }
}
impl PacketReturnTotals {
    /// Host checks the same Kernel root without exposing policy through a view.
    pub fn owns_view(&self, view: &PacketMemoryView) -> bool {
        std::ptr::eq(self, Arc::as_ptr(&view.original.0.root))
    }
}
impl PacketMemoryView {
    /// A concrete Source operation may prepay its real ancillary receive
    /// workspace before native acquisition. This returns memory custody only;
    /// it cannot bind a socket, spawn work or replace the Host limits.
    pub fn native_workspace(&self, bytes: usize) -> Result<NativeReadGrant, PacketMemoryError> {
        let state = self
            .original
            .0
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.revoked {
            return Err(PacketMemoryError::Stopped);
        }
        self.original.0.root.native_grant(bytes)
    }
    /// Read-only identity of the exact granting original, never a close proof.
    pub fn same_original(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.original.0, &other.original.0)
    }
    pub fn check_open(&self) -> Result<(), PacketMemoryError> {
        self.original.check_memory_open()
    }
    pub fn transmit_packet(
        &self,
        original: usize,
        derived_frames: usize,
        derived_bytes: usize,
    ) -> Result<PacketBuffer, PacketMemoryError> {
        // Seal against this original's revoke while admitting actual memory.
        // This grants no native IO, task authority or close/join observation.
        let state = self
            .original
            .0
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.revoked {
            return Err(PacketMemoryError::Stopped);
        }
        self.original
            .0
            .root
            .transmit_packet(original, derived_frames, derived_bytes)
    }
    pub async fn changed(&self) {
        let changed = self.original.0.root.notified();
        tokio::pin!(changed);
        changed.as_mut().enable();
        if self.check_open().is_err() {
            return;
        }
        changed.await;
    }
    pub fn transmit(&self) -> PacketMemorySnapshot {
        self.original.0.root.transmit()
    }
}

#[cfg(test)]
mod tests;
