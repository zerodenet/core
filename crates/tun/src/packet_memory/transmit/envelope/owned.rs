use super::*;
impl PacketTransmitEnvelope {
    pub fn allocate_owned(
        &self,
        lengths: &[usize],
    ) -> Result<zero_traits::PacketBatch, PacketMemoryError> {
        if lengths.is_empty() {
            return Ok(Default::default());
        }
        let bytes = lengths
            .iter()
            .try_fold(0usize, |sum, value| sum.checked_add(*value))
            .ok_or(PacketMemoryError::Overflow)?;
        let root = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remaining
            .root
            .clone();
        let _notifications = root.defer_notifications();
        let mut reserved = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if lengths.len() > reserved.remaining.frames || bytes > reserved.remaining.bytes {
            return Err(
                if lengths.len() <= reserved.total.frames && bytes <= reserved.total.bytes {
                    PacketMemoryError::Busy
                } else {
                    PacketMemoryError::Capacity
                },
            );
        }
        let mut headers = reserved
            .headers
            .as_ref()
            .expect("same storage headers")
            .try_lease(lengths.len())?;
        let array = if lengths.len() > 1 {
            let array = reserved
                .arrays
                .as_ref()
                .expect("same prepared array promise")
                .try_lease(lengths.len());
            match array {
                Ok(array) => Some(array),
                Err(source) => {
                    drop(reserved);
                    drop(headers);
                    return Err(source);
                }
            }
        } else {
            None
        };
        reserved.remaining.frames -= lengths.len();
        reserved.remaining.bytes -= bytes;
        drop(reserved);
        let mut allocate = |bytes: usize| {
            let mut credit = NativePacketCredit::new(
                credit::Credit {
                    root: root.clone(),
                    lane: Lane::Transmit,
                    frames: 1,
                    bytes,
                },
                None,
            );
            credit.install_header_lease(headers.split_one().expect("same admitted storage header"));
            credit.envelope = Some(self.clone());
            credit.return_to_envelope = true;
            read::NativeReadBuffer::from_credit(credit)
                .into_packet(bytes)
                .unwrap_or_else(|_| unreachable!("same owned transform capacity"))
        };
        if lengths.len() == 1 {
            return Ok(zero_traits::PacketBatch::one(allocate(lengths[0])));
        }
        let mut values = Box::<[PacketBuffer]>::new_uninit_slice(lengths.len());
        for (target, bytes) in values.iter_mut().zip(lengths) {
            target.write(allocate(*bytes));
        }
        let pointer = Box::into_raw(values) as *mut PacketBuffer;
        // Exact same prepaid array layout, all elements initialized above.
        let values = unsafe { Vec::from_raw_parts(pointer, lengths.len(), lengths.len()) };
        Ok(
            zero_traits::PacketBatch::from_prepaid_array(values, array.expect("same actual array"))
                .unwrap_or_else(|_| unreachable!("same concrete returned array layout")),
        )
    }
}
