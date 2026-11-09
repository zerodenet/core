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
        let _notifications = self.defer_notifications();
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
    /// One actual fragment assembly in the SAME Native frame/byte lane.
    /// This is memory-only admission, never another IO/read/Scope permission.
    pub fn try_assembly(&self, bytes: usize) -> Result<NativeReadBuffer, PacketMemoryError> {
        let _notifications = self.defer_notifications();
        let state = self
            .original
            .0
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.revoked {
            return Err(PacketMemoryError::Stopped);
        }
        self.original.0.root.native_assembly(bytes)
    }
    /// Read-only identity of the exact granting original, never a close proof.
    pub fn same_original(&self, other: &Self) -> bool {
        PacketMetadataArc::ptr_eq(&self.original.0, &other.original.0)
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
        let _notifications = self.defer_notifications();
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
    /// Admit every concrete codec intermediate/output before any codec state
    /// changes. The one original Root transaction either reserves all lengths
    /// or none; only actual allocation lifetimes return their same credits.
    pub fn transmit_buffers(
        &self,
        lengths: &[usize],
    ) -> Result<Vec<PacketBuffer>, PacketMemoryError> {
        let _notifications = self.defer_notifications();
        let credits = {
            let state = self
                .original
                .0
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.revoked {
                return Err(PacketMemoryError::Stopped);
            }
            self.original.0.root.reserve_transmit(lengths)?
        };
        Ok(credits
            .into_iter()
            .map(super::PacketTransmitCredit::allocate)
            .collect())
    }
    pub fn transmit_batch(
        &self,
        lengths: &[usize],
    ) -> Result<zero_traits::PacketBatch, PacketMemoryError> {
        if lengths.is_empty() {
            self.check_open()?;
            return Ok(Default::default());
        }
        let bytes = lengths
            .iter()
            .try_fold(0usize, |sum, n| sum.checked_add(*n))
            .ok_or(PacketMemoryError::Overflow)?;
        let _notifications = self.defer_notifications();
        let envelope = {
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
                .reserve_transmit_envelope(lengths.len(), bytes)?
        };
        envelope.allocate_owned(lengths)
    }
    /// Prepay a concrete owning Source/driver layout before constructing its
    /// heap objects. No IO, Scope, native receipt or policy setter is issued.
    /// Existing prepaid credits remain owned after this view is revoked.
    pub fn reserve_driver_metadata(
        &self,
        layout: PacketMetadataLayout,
    ) -> Result<PacketMetadataCredit, PacketMemoryError> {
        let state = self
            .original
            .0
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.revoked {
            return Err(PacketMemoryError::Stopped);
        }
        self.original.0.root.reserve_driver_metadata(layout)
    }
    /// Metadata-only clone of this actual still-live grant's existing Root.
    /// It creates no native/task authority, allocator or replacement budget.
    pub fn driver_metadata_view(&self) -> Result<PacketDriverMetadataView, PacketMemoryError> {
        self.original.check_memory_open()?;
        Ok(self.original.0.root.driver_metadata_view())
    }
    pub(in crate::packet_memory) fn minimum_metadata_layout(
        &self,
    ) -> Result<PacketMetadataLayout, PacketMemoryError> {
        self.original.minimum_header_layout()
    }
    pub fn driver_metadata(&self) -> PacketMetadataSnapshot {
        self.original.0.root.driver_metadata()
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

impl PacketMemoryView {
    /// Same actual Kernel accounting object only; no policy, IO or close right.
    pub fn same_kernel_root(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.original.0.root, &other.original.0.root)
    }
}
