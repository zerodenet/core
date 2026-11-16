//! Explicit standalone protocol-library API only. Standard Source handshakes
//! call handshake_hosted and cannot reach this detached convenience worker.
use super::*;
impl EncryptionServer {
    pub(in crate::encryption::server) fn start_library_maintenance(&self) -> io::Result<()> {
        if !self.inner.config.resume {
            return Ok(());
        }
        match self.inner.maintenance.compare_exchange(
            COLD,
            LIBRARY,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => {}
            Err(LIBRARY) => return Ok(()),
            Err(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::Interrupted,
                    "decryption state belongs to its original Source",
                ))
            }
        }
        let owner = Arc::downgrade(&self.inner);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(60)).await;
                let Some(owner) = owner.upgrade() else { break };
                owner
                    .cache
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .prune();
            }
        });
        Ok(())
    }
}
