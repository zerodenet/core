//! Concrete kernel-owned memory installation; no descriptor or task authority.
use super::*;
use crate::packet_memory::NativeReadGrant;
impl AdoptedTunOwner {
    pub fn prepare_macos_workspaces(
        &self,
        read: NativeReadGrant,
        write: NativeReadGrant,
    ) -> io::Result<()> {
        let _read = read.defer_notifications();
        let _write = write.defer_notifications();
        let state = self.lock();
        if !matches!(*state, State::Cold) || self.macos_memory.get().is_some() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "same native owner already attempted",
            ));
        }
        let memory = crate::macos::memory::Original::prepare(read, write)?;
        self.macos_memory.set(memory).map_err(|_| {
            io::Error::new(
                io::ErrorKind::AlreadyExists,
                "same utun workspace owner already installed",
            )
        })
    }
    /// Known-unacquired or closed state only; never treats Unknown as no IO.
    pub fn return_unacquired_macos_workspaces(&self) {
        let _notifications = self.macos_memory.get().map(|memory| memory.notifications());
        let mut state = self.lock();
        let buffers = if matches!(*state, State::Cold | State::NoNative(_) | State::Closed(_)) {
            if matches!(*state, State::Cold) {
                // Seal this never-entered original before returning its read/
                // write allocations; a concurrent late acquire cannot start.
                *state = State::NoNative(Arc::new(io::Error::new(
                    io::ErrorKind::Interrupted,
                    "same unacquired utun memory was revoked",
                )));
            }
            self.macos_memory
                .get()
                .and_then(|memory| memory.take_unused())
        } else {
            None
        };
        drop(state);
        drop(buffers);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::packet_memory::{PacketMemoryLimit, PacketReturnTotals};
    #[test]
    fn same_unacquired_utun_buffers_remain_charged_until_explicit_revocation() {
        let roots = Arc::new(PacketReturnTotals::new_with_transmit(
            PacketMemoryLimit {
                frames: 1,
                bytes: 1,
            },
            PacketMemoryLimit {
                frames: 2,
                bytes: 65_536 + 65_540,
            },
            PacketMemoryLimit {
                frames: 1,
                bytes: 65_540,
            },
        ));
        let memory = roots
            .datagram_workspaces([65_536, 65_540], Some(65_540))
            .unwrap();
        let owner = AdoptedTunOwner::new();
        owner
            .prepare_macos_workspaces(
                memory.reads[1].clone(),
                memory.control.as_ref().unwrap().clone(),
            )
            .unwrap();
        assert!(matches!(
            owner.outcome(),
            TunAcquisitionOutcome::KnownNoNative
        ));
        assert!(!memory.reads[1].workspace_returned());
        assert!(!memory.control.as_ref().unwrap().workspace_returned());
        assert_eq!(roots.native().occupied.bytes, 65_536 + 65_540);
        assert_eq!(roots.transmit().occupied.bytes, 65_540);
        owner.return_unacquired_macos_workspaces();
        assert!(memory.reads[1].workspace_returned());
        assert!(memory.control.as_ref().unwrap().workspace_returned());
        assert!(matches!(
            owner.lock().outcome(),
            TunAcquisitionOutcome::KnownNoNative
        ));
        for grant in memory.reads.iter().chain(memory.control.iter()) {
            grant.revoke();
        }
        assert_eq!(roots.native().occupied.bytes, 0);
        assert_eq!(roots.transmit().occupied.bytes, 0);
    }
}
