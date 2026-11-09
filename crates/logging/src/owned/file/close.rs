use super::*;
impl Entry {
    /// Caller has already consumed the actual original worker join and ACK.
    pub(in crate::owned) fn close(&self) -> bool {
        let outcome = {
            let mut native = self.native.lock().unwrap_or_else(|e| e.into_inner());
            if native.unknown.is_some() || native.acquiring {
                return false;
            }
            if native.closed.is_some() || native.original.is_none() {
                return true;
            }
            let original = native.original.as_mut().unwrap();
            match original.close() {
                Ok(receipt) if original.matches_closed(&receipt) => {
                    native.closed = Some(receipt);
                    self.native_closed
                        .store(true, std::sync::atomic::Ordering::Release);
                    Ok(())
                }
                other => Err(other.err().unwrap_or_else(|| {
                    io::Error::other("logging original close receipt mismatch")
                })),
            }
        };
        match outcome {
            Ok(()) => true,
            Err(error) => {
                let source = self.source(2, fault::IoPhase::Close, error, None);
                self.native
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .unknown = Some(source);
                self.unknown
                    .store(true, std::sync::atomic::Ordering::Release);
                self.book.wake.notify();
                false
            }
        }
    }
}
