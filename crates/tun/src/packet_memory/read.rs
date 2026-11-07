use super::*;
/// One prepaid workspace in the original native driver's custody. This keeps
/// no buffer queue: the current exclusive allocation returns its credit only
/// when its memory is released. Other originals cannot steal the next read.
#[derive(Clone)]
pub struct NativeReadGrant(pub(super) Arc<ReadSlot>);
pub(super) struct ReadSlot {
    pub(super) root: Arc<PacketReturnTotals>,
    bytes: usize,
    pub(super) state: Mutex<SlotState>,
    pub(super) changed: Notify,
    pub(super) blocking: Condvar,
    pub(super) notifications: notifications::Notifications,
}
pub(super) struct SlotState {
    pub(super) ready: Option<credit::Credit>,
    pub(super) revoked: bool,
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
        self.notify();
    }
    pub(super) fn notify(&self) {
        if self.notifications.request() {
            self.notify_now();
        }
    }
    pub(super) fn notify_now(&self) {
        self.changed.notify_waiters();
        self.blocking.notify_all();
    }
}
impl NativeReadGrant {
    pub(super) fn from_credit(credit: credit::Credit) -> Self {
        assert_eq!(credit.frames, 1, "one original IO workspace");
        Self(Arc::new(ReadSlot {
            root: credit.root.clone(),
            bytes: credit.bytes,
            state: Mutex::new(SlotState {
                ready: Some(credit),
                revoked: false,
            }),
            changed: Notify::new(),
            blocking: Condvar::new(),
            notifications: notifications::Notifications::default(),
        }))
    }
    pub fn bytes(&self) -> usize {
        self.0.bytes
    }
    /// New fragment assembly admission charges the same kernel root. The
    /// prepaid original read slot remains separate, so existing assemblies
    /// cannot prevent the next read needed to complete them.
    pub fn try_assembly(&self, bytes: usize) -> Result<NativeReadBuffer, PacketMemoryError> {
        if self
            .0
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .revoked
        {
            return Err(PacketMemoryError::Stopped);
        }
        self.0.root.native_assembly(bytes)
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
        self.0.root.wake();
    }
    /// The actual blocking SDK reader waits on its own admitted workspace.
    /// Stop wakes this original Condvar; no polling timeout/reaper is needed.
    pub fn buffer_blocking(
        &self,
        stopped: &std::sync::atomic::AtomicBool,
    ) -> Result<NativeReadBuffer, PacketMemoryError> {
        self.buffer_blocking_inner(stopped, None)
    }
    /// A real native reader may interrupt only allocation waiting for its own
    /// reversible pause. This observation is not that reader's pause ACK.
    pub fn buffer_blocking_interruptible(
        &self,
        stopped: &std::sync::atomic::AtomicBool,
        paused: &std::sync::atomic::AtomicBool,
    ) -> Result<NativeReadBuffer, PacketMemoryError> {
        self.buffer_blocking_inner(stopped, Some(paused))
    }
    fn buffer_blocking_inner(
        &self,
        stopped: &std::sync::atomic::AtomicBool,
        paused: Option<&std::sync::atomic::AtomicBool>,
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
            if paused.is_some_and(|paused| paused.load(std::sync::atomic::Ordering::Acquire)) {
                return Err(PacketMemoryError::Paused);
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
    /// The same IO workspace has returned; this is no native or task receipt.
    pub fn workspace_returned(&self) -> bool {
        self.0
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .ready
            .is_some()
    }
    pub fn wake(&self) {
        // External stop/pause predicates can change without holding this mutex.
        // Serialize with Condvar::wait to avoid missing their final wakeup.
        drop(
            self.0
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        );
        self.0.notify();
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
    fn prepare_transmit(
        &mut self,
        frames: usize,
        bytes: usize,
    ) -> Result<(), zero_traits::PacketDeriveError> {
        self._credit
            .begin_transmit(frames, bytes)
            .map_err(super::transmit::derive_error)
    }
    fn take_vec_allocation(&mut self) -> Option<Vec<u8>> {
        Some(std::mem::take(&mut self.bytes))
    }

    fn try_derive_zeroed(
        &self,
        lengths: &[usize],
    ) -> Result<Vec<zero_traits::PacketBuffer>, zero_traits::PacketDeriveError> {
        self._credit.try_derive_zeroed(lengths)
    }
    fn derive_zeroed<'a>(&'a self, lengths: &'a [usize]) -> zero_traits::PacketDerivation<'a> {
        self._credit.derive_zeroed(lengths)
    }
    fn allocation_bytes(&self) -> usize {
        self._credit.capacity()
    }
}
