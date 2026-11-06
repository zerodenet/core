use super::*;
/// One prepaid workspace in the original native driver's custody. This keeps
/// no buffer queue: the current exclusive allocation returns its credit only
/// when its memory is released. Other originals cannot steal the next read.
#[derive(Clone)]
pub struct NativeReadGrant(Arc<ReadSlot>);
pub(super) struct ReadSlot {
    bytes: usize,
    state: Mutex<SlotState>,
    changed: Notify,
    blocking: Condvar,
}
struct SlotState {
    ready: Option<credit::Credit>,
    revoked: bool,
}
impl ReadSlot {
    pub(super) fn return_credit(&self, credit: credit::Credit) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.revoked {
            drop(state);
            drop(credit);
            return;
        }
        assert!(
            state.ready.is_none(),
            "same unique read workspace returned once"
        );
        state.ready = Some(credit);
        drop(state);
        self.changed.notify_waiters();
        self.blocking.notify_all();
    }
}
impl NativeReadGrant {
    pub(super) fn from_credit(credit: credit::Credit) -> Self {
        Self(Arc::new(ReadSlot {
            bytes: credit.bytes,
            state: Mutex::new(SlotState {
                ready: Some(credit),
                revoked: false,
            }),
            changed: Notify::new(),
            blocking: Condvar::new(),
        }))
    }
    pub fn bytes(&self) -> usize {
        self.0.bytes
    }
    pub fn try_buffer(&self) -> Result<NativeReadBuffer, PacketMemoryError> {
        self.try_workspace().map(NativeReadBuffer::from_credit)
    }
    pub fn try_workspace(&self) -> Result<NativePacketCredit, PacketMemoryError> {
        let mut state = self
            .0
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.revoked {
            return Err(PacketMemoryError::Stopped);
        }
        let credit = state.ready.take().ok_or(PacketMemoryError::Busy)?;
        Ok(NativePacketCredit::new(
            credit,
            Some(Arc::downgrade(&self.0)),
        ))
    }
    pub async fn buffer(&self) -> Result<NativeReadBuffer, PacketMemoryError> {
        self.workspace().await.map(NativeReadBuffer::from_credit)
    }
    pub async fn workspace(&self) -> Result<NativePacketCredit, PacketMemoryError> {
        loop {
            let changed = self.0.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            match self.try_workspace() {
                Err(PacketMemoryError::Busy) => changed.await,
                other => return other,
            }
        }
    }
    /// Revoke allocation only. An outstanding payload keeps its same debit
    /// until its real memory is released; this is no native or join receipt.
    pub fn revoke(&self) {
        let mut state = self
            .0
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.revoked = true;
        let unused = state.ready.take();
        drop(state);
        drop(unused);
        self.wake();
    }
    /// The actual blocking SDK reader waits on its own admitted workspace.
    /// Stop wakes this original Condvar; no polling timeout/reaper is needed.
    pub fn buffer_blocking(
        &self,
        stopped: &std::sync::atomic::AtomicBool,
    ) -> Result<NativeReadBuffer, PacketMemoryError> {
        let mut state = self
            .0
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        loop {
            if state.revoked || stopped.load(std::sync::atomic::Ordering::Acquire) {
                return Err(PacketMemoryError::Stopped);
            }
            if let Some(credit) = state.ready.take() {
                drop(state);
                return Ok(NativeReadBuffer::from_credit(NativePacketCredit::new(
                    credit,
                    Some(Arc::downgrade(&self.0)),
                )));
            }
            state = self
                .0
                .blocking
                .wait(state)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }
    pub fn wake(&self) {
        self.0.changed.notify_waiters();
        self.0.blocking.notify_all();
    }
}
/// The credit covers actual allocation capacity, including the unused tail.
/// Reading/truncating does not refund the tail before its memory is released.
pub struct NativeReadBuffer {
    bytes: Vec<u8>,
    credit: NativePacketCredit,
}
impl NativeReadBuffer {
    pub(super) fn from_credit(credit: NativePacketCredit) -> Self {
        Self {
            bytes: vec![0; credit.capacity()],
            credit,
        }
    }
    pub fn as_mut_bytes(&mut self) -> &mut [u8] {
        &mut self.bytes
    }
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }
    pub fn into_packet(mut self, length: usize) -> Result<zero_traits::PacketBuffer, Self> {
        if length > self.bytes.len() {
            return Err(self);
        }
        self.bytes.truncate(length);
        Ok(zero_traits::PacketBuffer::from_owner(ReadPacket {
            bytes: self.bytes,
            _credit: self.credit,
        }))
    }
}
struct ReadPacket {
    bytes: Vec<u8>,
    _credit: NativePacketCredit,
}
impl AsRef<[u8]> for ReadPacket {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}
impl AsMut<[u8]> for ReadPacket {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut self.bytes
    }
}
impl zero_traits::PacketStorage for ReadPacket {
    fn allocation_bytes(&self) -> usize {
        self._credit.capacity()
    }
}
