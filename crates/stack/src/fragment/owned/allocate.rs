use super::*;
impl<K: Clone + Eq + Hash> OwnedFragmentReassembler<K> {
    pub(super) async fn allocate(
        &self,
        packet: &PacketBuffer,
    ) -> Result<PacketBuffer, FragmentRejectReason> {
        #[cfg(feature = "packet-memory")]
        if let Some(memory) = &self.memory {
            let buffer = memory
                .try_assembly(header::MAX_STORAGE)
                .map_err(|_| FragmentRejectReason::ResourceLimit)?;
            return buffer
                .into_packet(header::MAX_STORAGE)
                .map_err(|_| FragmentRejectReason::ResourceLimit);
        }
        // Explicit independent library-owned PacketStorage keeps its original
        // derivation contract; Kernel constructors always carry actual memory.
        let mut storage = packet
            .derive_zeroed(&[header::MAX_STORAGE])
            .await
            .map_err(|_| FragmentRejectReason::ResourceLimit)?;
        if storage.len() != 1 {
            return Err(FragmentRejectReason::ResourceLimit);
        }
        Ok(storage.pop().expect("one actual assembly allocation"))
    }
}
